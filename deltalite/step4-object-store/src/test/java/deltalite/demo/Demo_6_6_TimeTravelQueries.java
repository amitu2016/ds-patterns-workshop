package deltalite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import deltalite.DeltaTable;
import deltalite.actions.Action;
import deltalite.actions.AddFile;
import deltalite.actions.RemoveFile;
import deltalite.store.ObjectStoreStorage;
import objectstorelite.client.ObjectStoreClient;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.server.StorageNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DEMO 6.6: Time Travel Queries on Delta Lake over Object Storage
 *
 * <p>Demonstrates:
 * <ol>
 *   <li>ACID commits on an object store using append-only Parquet data files and JSON commit logs.</li>
 *   <li>Non-destructive updates: Updates remove old Parquet references without mutating or deleting files.</li>
 *   <li>Time-travel queries: Seamlessly query the exact state of the table at Version 1 and Version 2.</li>
 * </ol>
 *
 * <p>TRY IT: set {@code MARK_OLD_FILE_REMOVED} to false. Nothing about the data changes — both
 * Parquet files are written either way, and both are still sitting in the object store. What
 * changes is what the log says is <i>active</i>: without the {@link RemoveFile}, version 2 claims
 * both files, so the table reports seven rows and Alice appears twice, at $1000 and at $800. The
 * log, not the file listing, is the table.
 */
public class Demo_6_6_TimeTravelQueries {

    /** TRY IT: set to false — version 2 keeps the v1 file active too, and rows double up. */
    static final boolean MARK_OLD_FILE_REMOVED = true;

    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    @TempDir
    Path clusterStorageRoot;

    @Test
    @DisplayName("6.6 · time travel queries and immutable Parquet versioning over object storage")
    void demonstrateTimeTravel() throws Exception {
        System.out.println("\n" + "=".repeat(76));
        System.out.println(" DEMO 6.6: TIME TRAVEL QUERIES ON DELTA LAKE OVER OBJECT STORAGE");
        System.out.println("=".repeat(76));

        // 1. Setup 6-node erasure-coded object store cluster (RS(4,2))
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

        try (Cluster cluster = new Cluster()
                .withProcessIds(nodeIds)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    int index = nodeIds.indexOf(params.id());
                    return new StorageNode(peerIds, params, nodePaths.get(index), mapper, DATA_SHARDS, PARITY_SHARDS);
                })
                .start()) {

            ObjectStoreClient client = cluster.newClient(
                    ProcessId.of("delta-client"),
                    (peers, params) -> new ObjectStoreClient(peers, params, nodeIds.get(0))
            );

            ObjectStoreStorage storage = new ObjectStoreStorage(client);
            String tablePath = "warehouse/bank_accounts";

            // 2. Initialize Table (Version 0)
            System.out.println("\n--- Step 1: Initializing Delta Table (Version 0) ---");
            var tableFuture = DeltaTable.forTable(tablePath, storage);
            DeltaTable table = cluster.tickUntilComplete(tableFuture);
            System.out.println("  Table initialized with protocol and metadata at Version 0.");

            TableSchema schema = new TableSchema("id", List.of(
                    new TableSchema.ColumnDefinition("id", TableSchema.ColumnType.STRING, true),
                    new TableSchema.ColumnDefinition("owner", TableSchema.ColumnType.STRING, true),
                    new TableSchema.ColumnDefinition("balance", TableSchema.ColumnType.INTEGER, true)
            ));

            // 3. Commit Version 1: Opening Balances
            System.out.println("\n--- Step 2: Committing Version 1 (Opening Balances) ---");
            List<TableRecord> v1Records = List.of(
                    new TableRecord("101", Map.of("id", "101", "owner", "Alice", "balance", 1000)),
                    new TableRecord("102", Map.of("id", "102", "owner", "Bob", "balance", 500)),
                    new TableRecord("103", Map.of("id", "103", "owner", "Charlie", "balance", 750))
            );
            var commit1Future = table.insertAndCommit(schema, v1Records);
            cluster.tickUntilComplete(commit1Future);
            assertFalse(commit1Future.isFailed());
            System.out.println("  Committed Version 1: Alice=$1000, Bob=$500, Charlie=$750");

            var v1Snapshot = table.getDeltaLog().getSnapshotAt(1);
            cluster.tickUntilComplete(v1Snapshot);
            List<AddFile> v1Files = v1Snapshot.getResult().getAllFiles();
            assertEquals(1, v1Files.size());
            String v1ParquetFile = v1Files.get(0).getPath();
            System.out.println("  Parquet data file for v1: " + v1ParquetFile);

            // 4. Commit Version 2: Transfer $200 from Alice to Bob + New Account Diana ($300)
            System.out.println("\n--- Step 3: Committing Version 2 (Transfer $200 from Alice to Bob + Diana) ---");
            var txnFuture = table.startTransaction();
            var txn = cluster.tickUntilComplete(txnFuture);

            // Mark the old parquet file as removed
            if (MARK_OLD_FILE_REMOVED) {
                txn.addAction(new RemoveFile(v1ParquetFile, System.currentTimeMillis()));
            } else {
                System.out.println("  (v1's Parquet file is NOT marked removed — it stays active)");
            }

            // Insert updated records into a new Parquet file
            List<TableRecord> v2Records = List.of(
                    new TableRecord("101", Map.of("id", "101", "owner", "Alice", "balance", 800)),
                    new TableRecord("102", Map.of("id", "102", "owner", "Bob", "balance", 700)),
                    new TableRecord("103", Map.of("id", "103", "owner", "Charlie", "balance", 750)),
                    new TableRecord("104", Map.of("id", "104", "owner", "Diana", "balance", 300))
            );
            var insert2Future = table.insert(schema, v2Records);
            cluster.tickUntilComplete(insert2Future);
            insert2Future.getResult().forEach(txn::addAction);

            var commit2Future = txn.commit("TRANSFER_AND_NEW_ACCOUNT");
            cluster.tickUntilComplete(commit2Future);
            assertFalse(commit2Future.isFailed());
            System.out.println("  Committed Version 2: Alice=$800, Bob=$700, Charlie=$750, Diana=$300");

            // 5. Time-Travel Query: Read Historical State at Version 1
            System.out.println("\n--- Step 4: Time Travel Query (SELECT * FROM bank_accounts VERSION AS OF 1) ---");
            var queryV1Future = table.readAtVersion(1);
            List<TableRecord> timeTravelV1 = cluster.tickUntilComplete(queryV1Future);
            System.out.printf("  Found %d accounts at Version 1:\n", timeTravelV1.size());
            for (TableRecord r : timeTravelV1) {
                System.out.printf("    - ID: %-4s | Owner: %-8s | Balance: $%d\n",
                        r.primaryKey(), r.getString("owner"), r.getInteger("balance"));
            }
            assertEquals(3, timeTravelV1.size());
            assertEquals(1000, timeTravelV1.stream().filter(r -> r.primaryKey().equals("101")).findFirst().get().getInteger("balance"));
            assertEquals(500, timeTravelV1.stream().filter(r -> r.primaryKey().equals("102")).findFirst().get().getInteger("balance"));

            // 6. Current Query: Read State at Version 2
            System.out.println("\n--- Step 5: Current Query (SELECT * FROM bank_accounts VERSION AS OF 2) ---");
            var queryV2Future = table.readAtVersion(2);
            List<TableRecord> currentV2 = cluster.tickUntilComplete(queryV2Future);
            System.out.printf("  Found %d accounts at Version 2:\n", currentV2.size());
            for (TableRecord r : currentV2) {
                System.out.printf("    - ID: %-4s | Owner: %-8s | Balance: $%d\n",
                        r.primaryKey(), r.getString("owner"), r.getInteger("balance"));
            }
            assertEquals(MARK_OLD_FILE_REMOVED ? 4 : 7, currentV2.size(),
                    "version 2 reads exactly the files the log marks active");

            List<Integer> aliceBalances = currentV2.stream()
                    .filter(r -> r.primaryKey().equals("101"))
                    .map(r -> r.getInteger("balance"))
                    .sorted()
                    .toList();
            if (MARK_OLD_FILE_REMOVED) {
                assertEquals(List.of(800), aliceBalances, "one active file, one Alice");
                assertEquals(700, currentV2.stream().filter(r -> r.primaryKey().equals("102")).findFirst().get().getInteger("balance"));
                assertEquals(300, currentV2.stream().filter(r -> r.primaryKey().equals("104")).findFirst().get().getInteger("balance"));
            } else {
                assertEquals(List.of(800, 1000), aliceBalances,
                        "both files are active, so Alice is read twice — once stale, once current");
            }

            // 7. Verify On-Disk Parquet Files in Object Storage
            System.out.println("\n--- Step 6: Verifying Object Storage State ---");
            var listLogFuture = storage.listObjects(tablePath + "/_delta_log/");
            cluster.tickUntilComplete(listLogFuture);
            System.out.println("  Delta transaction logs:");
            listLogFuture.getResult().forEach(k -> System.out.println("    📄 " + k));

            var listDataFuture = storage.listObjects(tablePath + "/data/");
            cluster.tickUntilComplete(listDataFuture);
            System.out.println("  Parquet data files (both v1 and v2 files preserved):");
            listDataFuture.getResult().forEach(k -> System.out.println("    📦 " + k));
            assertEquals(2, listDataFuture.getResult().size(), "Both Parquet data files exist concurrently on object store");

            System.out.println("\n── KEY TAKEAWAYS ──");
            System.out.println("  1. Delta Lake updates never mutate past Parquet files.");
            System.out.println("  2. The transaction log records which Parquet files are active for any version.");
            if (!MARK_OLD_FILE_REMOVED) {
                System.out.println("     ↑ and with the RemoveFile omitted, you just saw what happens when it lies:");
                System.out.println("       the same account read twice, at two different balances.");
            }
            System.out.println("  3. Time-travel queries read historical files directly without data duplication or snapshots.");
            System.out.println("=".repeat(76));
        }
    }
}
