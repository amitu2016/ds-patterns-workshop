package objectstorelite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import objectstorelite.client.ObjectStoreClient;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.model.ObjectMeta;
import objectstorelite.model.VersionEntry;
import objectstorelite.server.StorageNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SLIDE (TODO): Shard layout & versioned metadata   (demo 5.4)
 *
 * <p>Demonstrates MinIO's on-disk storage layout and {@code xl.meta} version tracking:
 * <pre>
 *   dataDir/
 *     buckets/
 *       default/
 *         warehouse/customers.parquet/
 *           xl.meta                          <- version history & shard topology
 *           &lt;version-1-uuid&gt;/
 *             part.1                         <- data / parity shard slice
 *             shard.meta
 *           &lt;version-2-uuid&gt;/
 *             part.1
 *             shard.meta
 * </pre>
 *
 * <p>Shows:
 * <ol>
 *     <li>Atomic versioned puts: Writing v1 then v2 preserves both versions on disk.</li>
 *     <li>Versioned {@code xl.meta}: Tracks timestamps, shard sizes, and active version.</li>
 *     <li>Delete markers: Soft deletion appends a tombstone version, keeping auditability.</li>
 * </ol>
 */
public class Demo_5_4_ShardLayoutAndMetadata {

    // TRY IT: change DATA_SHARDS to 8 and PARITY_SHARDS to 4 — RS(8,4) across 12 nodes. The
    //         shard directory grows to 12 entries and xl.meta records the new K/M, but the
    //         object's storage overhead stays at 1.5x: RS(8,4) costs the same as RS(4,2) while
    //         tolerating twice as many failures. Wider stripes are strictly better on
    //         durability-per-byte — which is why MinIO defaults to large erasure sets, and why
    //         the limit is how many independent drives you actually have.
    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    @TempDir
    Path clusterStorageRoot;

    @Test
    @DisplayName("5.4 · on-disk shard hierarchy and xl.meta version tracking across storage nodes")
    void demonstrateShardLayoutAndVersionedMetadata() throws Exception {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 5.4: SHARD LAYOUT & VERSIONED METADATA (xl.meta)");
        System.out.println("=".repeat(72));

        // 1. Setup 6 Storage Node processes with isolated storage directories
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

        ProcessId clientId = ProcessId.of("client-1");

        try (Cluster cluster = new Cluster()
                .withProcessIds(nodeIds)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    int index = nodeIds.indexOf(params.id());
                    return new StorageNode(peerIds, params, nodePaths.get(index), mapper, DATA_SHARDS, PARITY_SHARDS);
                })
                .start()) {

            ObjectStoreClient client = cluster.newClient(
                    clientId,
                    (peers, params) -> new ObjectStoreClient(peers, params, nodeIds.get(0))
            );

            String objectKey = "warehouse/customers.parquet";

            // 2. Put Version 1
            System.out.println("\n--- 1. Writing Version 1: Initial Customer Table ---");
            byte[] v1Data = ("cust_id,name,country\n" +
                    "101,Alice,India\n" +
                    "102,Bob,USA\n" +
                    "103,Charlie,Germany\n").repeat(10).getBytes(StandardCharsets.UTF_8);

            var putV1 = client.putObject(objectKey, v1Data);
            cluster.tickUntil(putV1::isCompleted);
            assertTrue(putV1.getResult().success());
            System.out.printf("  Put v1: %,d bytes written across %d storage nodes\n", v1Data.length, TOTAL_SHARDS);

            // Verify read v1
            var readV1Future = client.getObject(objectKey);
            cluster.tickUntil(readV1Future::isCompleted);
            assertTrue(readV1Future.getResult().success());
            byte[] readV1 = readV1Future.getResult().data();
            assertArrayEquals(v1Data, readV1);
            System.out.println("  Read v1: verified 100% bit-exact read");

            // 3. Put Version 2 (Overwrite)
            System.out.println("\n--- 2. Writing Version 2: Appending New Customers (Overwrite) ---");
            byte[] v2Data = ("cust_id,name,country\n" +
                    "101,Alice,India\n" +
                    "102,Bob,USA\n" +
                    "103,Charlie,Germany\n" +
                    "104,Diana,Japan\n" +
                    "105,Evan,UK\n").repeat(20).getBytes(StandardCharsets.UTF_8);

            var putV2 = client.putObject(objectKey, v2Data);
            cluster.tickUntil(putV2::isCompleted);
            assertTrue(putV2.getResult().success());
            System.out.printf("  Put v2: %,d bytes written across %d storage nodes\n", v2Data.length, TOTAL_SHARDS);

            var readV2Future = client.getObject(objectKey);
            cluster.tickUntil(readV2Future::isCompleted);
            assertTrue(readV2Future.getResult().success());
            byte[] readV2 = readV2Future.getResult().data();
            assertArrayEquals(v2Data, readV2);
            System.out.println("  Read v2: verified latest version returned");

            // 4. Delete Object (Soft delete marker in xl.meta)
            System.out.println("\n--- 3. Deleting Object: Appends Delete Marker to xl.meta ---");
            var deleteFuture = client.deleteObject(objectKey);
            cluster.tickUntil(deleteFuture::isCompleted);
            assertTrue(deleteFuture.getResult().success());
            System.out.println("  Delete marker committed across storage nodes");

            // 5. Inspect the on-disk directory structure of Node 0
            System.out.println("\n--- 4. On-Disk Shard Layout (Inspecting Node 0) ---");
            Path node0ObjectDir = nodePaths.get(0).resolve("buckets").resolve("default").resolve(objectKey);
            printDirectoryTree(node0ObjectDir, "  ");

            // 6. Inspect xl.meta version history
            Path xlMetaPath = node0ObjectDir.resolve("xl.meta");
            assertTrue(Files.exists(xlMetaPath), "xl.meta must exist on disk");
            ObjectMeta meta = ObjectMeta.read(xlMetaPath);

            System.out.println("\n--- 5. Parsed xl.meta Version History ---");
            List<VersionEntry> versions = meta.versions();
            System.out.printf("  Total recorded versions: %d\n", versions.size());
            for (int i = 0; i < versions.size(); i++) {
                VersionEntry v = versions.get(i);
                String status = v.deleted() ? "DELETE_MARKER" : "ACTIVE_DATA";
                System.out.printf("  [%d] Version ID: %s\n", i + 1, v.versionId());
                System.out.printf("      Status:     %s\n", status);
                System.out.printf("      ObjectSize: %,d bytes, ShardSize: %,d bytes\n", v.objectSize(), v.shardSize());
                System.out.printf("      Topology:   K=%d data, M=%d parity\n", v.dataShards(), v.parityShards());
            }

            assertEquals(3, versions.size(), "xl.meta must record v1, v2, and delete marker");
            assertTrue(versions.get(2).deleted(), "Latest entry must be delete marker");

            System.out.println("\n── key takeaway ──");
            System.out.println("  MinIO uses immutable shard files and an append-only xl.meta journal.");
            System.out.println("  Overwrites and deletes do NOT mutate past shards in place.");
            System.out.println("  This makes object storage crash-consistent, auditable, and snapshot-friendly!");
        }
    }

    private static void printDirectoryTree(Path dir, String indent) throws IOException {
        if (!Files.exists(dir)) {
            System.out.println(indent + "(directory does not exist)");
            return;
        }
        System.out.println(indent + "📁 " + dir.getFileName());
        try (var stream = Files.walk(dir, 2)) {
            stream.filter(p -> !p.equals(dir)).forEach(p -> {
                int depth = dir.relativize(p).getNameCount();
                String prefix = indent + "   ".repeat(depth);
                if (Files.isDirectory(p)) {
                    System.out.println(prefix + "📁 " + p.getFileName());
                } else {
                    try {
                        System.out.printf("%s📄 %-24s (%,d bytes)\n", prefix, p.getFileName(), Files.size(p));
                    } catch (IOException e) {
                        System.out.println(prefix + "📄 " + p.getFileName());
                    }
                }
            });
        }
    }
}
