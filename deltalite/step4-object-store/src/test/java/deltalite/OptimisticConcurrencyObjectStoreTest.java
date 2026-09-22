package deltalite;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import deltalite.store.ObjectStoreStorage;
import objectstorelite.client.ObjectStoreClient;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.server.StorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests Optimistic Concurrency Control (OCC) and conflict resolution
 * against an objectstorelite cluster using conditional PUTs (If-None-Match: *).
 */
public class OptimisticConcurrencyObjectStoreTest {

    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    @TempDir
    Path clusterStorageRoot;

    private Cluster cluster;
    private ObjectStoreStorage storage;

    @BeforeEach
    void setUp() throws Exception {
        List<ProcessId> nodeIds = new ArrayList<>();
        List<Path> nodePaths = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            ProcessId id = ProcessId.of("node-" + i);
            nodeIds.add(id);
            nodePaths.add(clusterStorageRoot.resolve("disk-node-" + i));
        }

        ErasureSet erasureSet = new ErasureSet(0, nodeIds);
        ErasureSetMapper mapper = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.CRCMOD, null, List.of(erasureSet));

        cluster = new Cluster()
                .withProcessIds(nodeIds)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    int index = nodeIds.indexOf(params.id());
                    return new StorageNode(peerIds, params, nodePaths.get(index), mapper, DATA_SHARDS, PARITY_SHARDS);
                })
                .start();

        ObjectStoreClient client = cluster.newClient(
                ProcessId.of("client-occ"),
                (peers, params) -> new ObjectStoreClient(peers, params, nodeIds.get(0))
        );

        storage = new ObjectStoreStorage(client);
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    void concurrentWritersAtSameVersionTriggerConflictAndSucceedOnRetry() {
        String tablePath = "warehouse/financials";
        var tableFuture = DeltaTable.forTable(tablePath, storage);
        DeltaTable table = cluster.tickUntilComplete(tableFuture);

        TableSchema schema = new TableSchema("id", List.of(
                new TableSchema.ColumnDefinition("id", TableSchema.ColumnType.STRING, true),
                new TableSchema.ColumnDefinition("dept", TableSchema.ColumnType.STRING, true),
                new TableSchema.ColumnDefinition("amount", TableSchema.ColumnType.INTEGER, true)
        ));

        // Both txn1 and txn2 start at version 0 snapshot
        var txn1Future = table.startTransaction();
        var txn2Future = table.startTransaction();
        cluster.tickUntil(() -> txn1Future.isCompleted() && txn2Future.isCompleted());
        OptimisticTransaction txn1 = txn1Future.getResult();
        OptimisticTransaction txn2 = txn2Future.getResult();

        assertEquals(0L, txn1.getReadVersion());
        assertEquals(0L, txn2.getReadVersion());

        // Writer 1 prepares batch 1
        var insert1Future = table.insert(schema, List.of(
                new TableRecord("D1", Map.of("id", "D1", "dept", "Engineering", "amount", 5000))
        ));
        cluster.tickUntilComplete(insert1Future);
        insert1Future.getResult().forEach(txn1::addAction);

        // Writer 2 prepares batch 2
        var insert2Future = table.insert(schema, List.of(
                new TableRecord("D2", Map.of("id", "D2", "dept", "Marketing", "amount", 3000))
        ));
        cluster.tickUntilComplete(insert2Future);
        insert2Future.getResult().forEach(txn2::addAction);

        // Writer 1 commits version 1 first
        var commit1Future = txn1.commit("ENGINEERING_BUDGET");
        cluster.tickUntilComplete(commit1Future);
        assertFalse(commit1Future.isFailed(), "Writer 1 commit should succeed");

        // Writer 2 attempts to commit version 1 -> Must fail with conflict!
        var commit2Future = txn2.commit("MARKETING_BUDGET");
        cluster.tickUntil(commit2Future::isFailed);
        assertTrue(commit2Future.getException() instanceof ConcurrentModificationException,
                "Writer 2 should fail with ConcurrentModificationException");

        // Writer 2 retries on the new snapshot (version 1)
        var retryTxnFuture = table.startTransaction();
        OptimisticTransaction retryTxn = cluster.tickUntilComplete(retryTxnFuture);
        assertEquals(1L, retryTxn.getReadVersion(), "Retried transaction should read version 1");

        // Re-apply actions to retry transaction and commit
        insert2Future.getResult().forEach(retryTxn::addAction);
        var retryCommitFuture = retryTxn.commit("MARKETING_BUDGET");
        cluster.tickUntilComplete(retryCommitFuture);
        assertFalse(retryCommitFuture.isFailed(), "Retried commit must succeed at version 2");

        // Verify final table state contains records from both commits
        var readAllFuture = table.readAll();
        List<TableRecord> allRecords = cluster.tickUntilComplete(readAllFuture);
        assertEquals(2, allRecords.size(), "Table should contain records from both committed transactions");
    }

    @Test
    void directWriteObjectIfAbsentRejectsDuplicateVersionFile() {
        String logFile = "warehouse/test_occ/_delta_log/00000000000000000001.json";
        byte[] v1 = "{\"commit\": 1}\n".getBytes();
        byte[] v2 = "{\"commit\": 2}\n".getBytes();

        // 1. Initial write-if-absent succeeds
        var write1 = storage.writeObjectIfAbsent(logFile, v1);
        cluster.tickUntilComplete(write1);
        assertFalse(write1.isFailed(), "Initial commit file must succeed");

        // 2. Second write-if-absent for same version file fails with ConcurrentModificationException
        var write2 = storage.writeObjectIfAbsent(logFile, v2);
        cluster.tickUntil(write2::isFailed);
        assertTrue(write2.getException() instanceof ConcurrentModificationException,
                "Conditional put-if-absent must fail with ConcurrentModificationException on collision");
    }
}
