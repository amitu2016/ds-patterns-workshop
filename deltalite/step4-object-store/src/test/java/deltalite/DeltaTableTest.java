package deltalite;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import deltalite.actions.Metadata;
import deltalite.actions.Protocol;
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
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class DeltaTableTest {

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
                ProcessId.of("delta-client"),
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
    void testTableInitialization() {
        String tablePath = "warehouse/events";
        var tableFuture = DeltaTable.forTable(tablePath, storage);
        DeltaTable table = cluster.tickUntilComplete(tableFuture);
        assertNotNull(table);

        var versionsFuture = table.getDeltaLog().listVersions();
        cluster.tickUntilComplete(versionsFuture);
        assertEquals(List.of(0L), versionsFuture.getResult(), "Version 0 should be initialized");

        var snapshotFuture = table.getDeltaLog().update();
        Snapshot snapshot = cluster.tickUntilComplete(snapshotFuture);
        assertEquals(0L, snapshot.getVersion());
        assertTrue(snapshot.getProtocol() != null, "Protocol action must exist at v0");
        assertTrue(snapshot.getMetadata() != null, "Metadata action must exist at v0");
    }

    @Test
    void testInsertAndReadAll() {
        String tablePath = "warehouse/customers";
        var tableFuture = DeltaTable.forTable(tablePath, storage);
        DeltaTable table = cluster.tickUntilComplete(tableFuture);

        TableSchema schema = new TableSchema("id", List.of(
                new TableSchema.ColumnDefinition("id", TableSchema.ColumnType.STRING, true),
                new TableSchema.ColumnDefinition("name", TableSchema.ColumnType.STRING, true),
                new TableSchema.ColumnDefinition("city", TableSchema.ColumnType.STRING, true)
        ));

        List<TableRecord> batch1 = List.of(
                new TableRecord("1", Map.of("id", "1", "name", "Alice", "city", "New York")),
                new TableRecord("2", Map.of("id", "2", "name", "Bob", "city", "San Francisco"))
        );

        var insertFuture = table.insertAndCommit(schema, batch1);
        cluster.tickUntilComplete(insertFuture);
        assertTrue(!insertFuture.isFailed(), "Insert should succeed");

        var readFuture = table.readAll();
        List<TableRecord> records = cluster.tickUntilComplete(readFuture);
        assertEquals(2, records.size());
        assertEquals("Alice", records.get(0).getString("name"));
        assertEquals("Bob", records.get(1).getString("name"));
    }

    @Test
    void testTimeTravelQuery() {
        String tablePath = "warehouse/accounts";
        var tableFuture = DeltaTable.forTable(tablePath, storage);
        DeltaTable table = cluster.tickUntilComplete(tableFuture);

        TableSchema schema = new TableSchema("id", List.of(
                new TableSchema.ColumnDefinition("id", TableSchema.ColumnType.STRING, true),
                new TableSchema.ColumnDefinition("owner", TableSchema.ColumnType.STRING, true),
                new TableSchema.ColumnDefinition("balance", TableSchema.ColumnType.INTEGER, true)
        ));

        // Commit v1: Initial accounts
        List<TableRecord> v1Records = List.of(
                new TableRecord("A", Map.of("id", "A", "owner", "Alice", "balance", 100)),
                new TableRecord("B", Map.of("id", "B", "owner", "Bob", "balance", 100))
        );
        var commit1 = table.insertAndCommit(schema, v1Records);
        cluster.tickUntilComplete(commit1);
        assertFalse(commit1.isFailed());

        // Commit v2: Additional account added
        List<TableRecord> v2Records = List.of(
                new TableRecord("C", Map.of("id", "C", "owner", "Charlie", "balance", 50))
        );
        var commit2 = table.insertAndCommit(schema, v2Records);
        cluster.tickUntilComplete(commit2);
        assertFalse(commit2.isFailed());

        // Verify time travel at v1: should contain only 2 records
        var readV1 = table.readAtVersion(1);
        cluster.tickUntilComplete(readV1);
        assertEquals(2, readV1.getResult().size(), "Historical snapshot at v1 should have 2 accounts");

        // Verify latest query at v2: should contain all 3 records
        var readV2 = table.readAtVersion(2);
        cluster.tickUntilComplete(readV2);
        assertEquals(3, readV2.getResult().size(), "Snapshot at v2 should have 3 accounts");
    }
}
