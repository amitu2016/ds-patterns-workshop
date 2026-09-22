package objectstorelite.demo;

import objectstorelite.codec.ErasureCodec;
import objectstorelite.model.ShardFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SLIDE: Reed-Solomon Encode and Reconstruct   (demo 5.3)
 *
 * <p>Demonstrates full-scale Reed-Solomon failure reconstruction on real multimedia data:
 * the 62 MB video file {@code BigBuckBunny_320x180.mp4}.
 *
 * <p>Cluster topology:
 * <ul>
 *     <li>6 storage nodes in an Erasure Set with RS(4,2) configuration: 4 data + 2 parity shards.</li>
 *     <li>Each node stores its shard independently on its local storage partition.</li>
 * </ul>
 *
 * <p>Failure scenarios tested:
 * <ol>
 *     <li><b>Tolerated failure (≤ M=2 nodes dead)</b>: 2 drives (e.g. node-1 and node-4) are destroyed.
 *         Reed-Solomon uses the remaining 4 nodes (exact K quorum) to reconstruct the entire 62 MB video.
 *         SHA-256 hash matches the original bit-for-bit in ~1 second!</li>
 *     <li><b>Fatal failure (> M=2 nodes dead)</b>: 3 drives (e.g. node-0, node-1, node-2) are destroyed.
 *         Available nodes = 3 &lt; K=4 quorum. Read fails immediately with quorum exception.</li>
 * </ol>
 */
public class Demo_5_3_ReedSolomonFailureReconstruction {

    // TRY IT: change K (DATA_SHARDS) to 8 and M (PARITY_SHARDS) to 4 — RS(8,4) across 12 drives.
    //         Everything below derives from these two numbers: the node count, how many are
    //         killed in each scenario, and the storage overhead. Nothing else needs editing.
    //         Then you can lose any 4 drives simultaneously without data loss!
    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;
    public static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    /**
     * Picks {@code count} shard indices to destroy, alternating between the data shards and the
     * parity shards. Killing only parity would be the easy case — RS has to rebuild real data.
     */
    private static List<Integer> spreadKills(int count) {
        List<Integer> kills = new ArrayList<>();
        int fromData = 0, fromParity = 0;
        for (int i = 0; i < count; i++) {
            if (i % 2 == 0 && fromData < DATA_SHARDS) {
                kills.add(fromData++);
            } else if (fromParity < PARITY_SHARDS) {
                kills.add(DATA_SHARDS + fromParity++);
            } else {
                kills.add(fromData++);
            }
        }
        return kills;
    }

    public static final String VIDEO_RESOURCE = "BigBuckBunny_320x180.mp4";

    @TempDir
    Path tempClusterDir;

    @Test
    @DisplayName("5.3 · RS(K,M) reconstructs the video bit-for-bit when M nodes fail, and fails at M+1")
    void demonstrateVideoReconstructionUnderNodeFailures() throws Exception {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 5.3: REED-SOLOMON RECONSTRUCTION WITH 62MB VIDEO UNDER NODE FAILURES");
        System.out.println("=".repeat(72));

        // 1. Locate and inspect video resource
        InputStream resourceStream = getClass().getClassLoader().getResourceAsStream(VIDEO_RESOURCE);
        assertNotNull(resourceStream, "Test video file " + VIDEO_RESOURCE + " must be present");

        Path sourceVideo = tempClusterDir.resolve("original_" + VIDEO_RESOURCE);
        try (resourceStream; OutputStream os = new BufferedOutputStream(Files.newOutputStream(sourceVideo))) {
            resourceStream.transferTo(os);
        }

        long originalSize = Files.size(sourceVideo);
        String originalSha256 = computeSha256(sourceVideo);

        System.out.printf("\n--- 1. Source Video Ingestion ---\n");
        System.out.printf("  Filename:    %s\n", VIDEO_RESOURCE);
        System.out.printf("  File size:   %,d bytes (%.2f MB)\n", originalSize, originalSize / (1024.0 * 1024.0));
        System.out.printf("  SHA-256:     %s\n", originalSha256);

        // 2. Encode across 6 storage nodes
        System.out.printf("\n--- 2. RS(%d,%d) Sharding across %d Storage Nodes ---\n",
                DATA_SHARDS, PARITY_SHARDS, TOTAL_SHARDS);

        List<Path> nodeDirectories = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            Path nodeDir = tempClusterDir.resolve("storage-node-" + i);
            Files.createDirectories(nodeDir);
            nodeDirectories.add(nodeDir);
        }

        long encodeStart = System.currentTimeMillis();
        List<ShardFile> shardFiles;
        try (InputStream in = new BufferedInputStream(Files.newInputStream(sourceVideo))) {
            shardFiles = ErasureCodec.encodeStreamToFiles(
                    in,
                    tempClusterDir.resolve("staging"),
                    DATA_SHARDS,
                    PARITY_SHARDS,
                    ErasureCodec.DEFAULT_BLOCK_SIZE
            );
        }
        long encodeMs = System.currentTimeMillis() - encodeStart;

        // Distribute each shard to its respective storage node
        List<Path> storedShardPaths = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            Path targetShard = nodeDirectories.get(i).resolve("part." + (i + 1));
            Files.move(shardFiles.get(i).path(), targetShard);
            storedShardPaths.add(targetShard);

            long shardBytes = Files.size(targetShard);
            String role = (i < DATA_SHARDS) ? "DATA  " : "PARITY";
            System.out.printf("  Node %d [%s shard %d]: %,10d bytes  -> %s\n",
                    i, role, (i < DATA_SHARDS ? i : i - DATA_SHARDS), shardBytes, targetShard.getFileName());
        }
        System.out.printf("  Encoding completed in %d ms (%,.1f MB/s)\n",
                encodeMs, (originalSize / (1024.0 * 1024.0)) / (encodeMs / 1000.0));

        // 3. Kill exactly M nodes — the most RS(K,M) can survive.
        //    Spread across data and parity shards, so this is not the easy case.
        System.out.printf("%n--- 3. Scenario A: Failure of %d Nodes (the limit RS(%d,%d) tolerates) ---%n",
                PARITY_SHARDS, DATA_SHARDS, PARITY_SHARDS);

        List<Path> availableShardsScenarioA = new ArrayList<>(storedShardPaths);
        List<Integer> killedA = spreadKills(PARITY_SHARDS);
        for (int dead : killedA) {
            availableShardsScenarioA.set(dead, null); // simulates drive failure / dead node
            System.out.printf("  💥 Node %d (%s shard): KILLED (drive unreadable)%n",
                    dead, dead < DATA_SHARDS ? "Data" : "Parity");
        }
        System.out.printf("  Surviving healthy nodes:     %d of %d (exact K=%d quorum met)%n",
                TOTAL_SHARDS - PARITY_SHARDS, TOTAL_SHARDS, DATA_SHARDS);

        Path reconstructedVideoA = tempClusterDir.resolve("reconstructed_scenario_a.mp4");
        long decodeStart = System.currentTimeMillis();
        try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(reconstructedVideoA))) {
            ErasureCodec.decodeFilesToStream(
                    availableShardsScenarioA,
                    out,
                    originalSize,
                    DATA_SHARDS,
                    PARITY_SHARDS,
                    ErasureCodec.DEFAULT_BLOCK_SIZE
            );
        }
        long decodeMs = System.currentTimeMillis() - decodeStart;

        String reconstructedSha256A = computeSha256(reconstructedVideoA);
        System.out.printf("  Reconstruction time: %d ms (%,.1f MB/s)\n",
                decodeMs, (originalSize / (1024.0 * 1024.0)) / (Math.max(1, decodeMs) / 1000.0));
        System.out.printf("  Reconstructed SHA:   %s\n", reconstructedSha256A);
        System.out.printf("  Original SHA:        %s\n", originalSha256);

        assertEquals(originalSha256, reconstructedSha256A,
                "Reconstructed 62MB video must be 100% bit-exact with original!");
        System.out.println("  ✅ Bit-for-bit identical verification succeeded!");

        // 4. Kill one more than M — one shard past what the parity can rebuild.
        System.out.printf("%n--- 4. Scenario B: Failure of %d Nodes (one past the M=%d parity limit) ---%n",
                PARITY_SHARDS + 1, PARITY_SHARDS);

        List<Path> availableShardsScenarioB = new ArrayList<>(storedShardPaths);
        for (int dead = 0; dead <= PARITY_SHARDS; dead++) {
            availableShardsScenarioB.set(dead, null);
            System.out.printf("  💥 Node %d (%s shard): KILLED%n",
                    dead, dead < DATA_SHARDS ? "Data" : "Parity");
        }
        System.out.printf("  Surviving healthy nodes:   %d of %d (BELOW K=%d quorum!)%n",
                TOTAL_SHARDS - PARITY_SHARDS - 1, TOTAL_SHARDS, DATA_SHARDS);

        Path failedReconstructedVideo = tempClusterDir.resolve("failed.mp4");
        IOException thrown = assertThrows(IOException.class, () -> {
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(failedReconstructedVideo))) {
                ErasureCodec.decodeFilesToStream(
                        availableShardsScenarioB,
                        out,
                        originalSize,
                        DATA_SHARDS,
                        PARITY_SHARDS,
                        ErasureCodec.DEFAULT_BLOCK_SIZE
                );
            }
        });

        System.out.printf("  🛑 Read Quorum Rejected: \"%s\"\n", thrown.getMessage());
        assertTrue(thrown.getMessage().contains("Quorum not met"), "Must fail with quorum error");

        System.out.println("\n── key takeaway ──");
        System.out.printf("  Reed-Solomon RS(K,M) provides absolute mathematical durability:\n");
        System.out.printf("  - Any ≤ %d drive failures: 100%% byte-for-byte instant reconstruction.\n", PARITY_SHARDS);
        System.out.printf("  - > %d drive failures: Quorum fails safely, preventing silent corruption.\n", PARITY_SHARDS);
    }

    private static String computeSha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream is = new BufferedInputStream(Files.newInputStream(file))) {
            byte[] buf = new byte[64 * 1024];
            int read;
            while ((read = is.read(buf)) != -1) {
                digest.update(buf, 0, read);
            }
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
