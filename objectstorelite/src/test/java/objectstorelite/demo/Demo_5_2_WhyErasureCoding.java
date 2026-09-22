package objectstorelite.demo;

import objectstorelite.codec.ErasureCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SLIDE: Why Erasure Coding (RS)   (demo 5.2)
 *
 * <p>Demonstrates the economic and durability breakthrough of Reed-Solomon Erasure Coding
 * compared to traditional 3× replication (HDFS, Ceph, Cassandra).
 *
 * <p>The without / with pattern:
 * <ul>
 *     <li><b>without</b>: 3× replication requires 200% storage overhead (3.0× disk cost).
 *         Losing 2 drives leaves zero margin; losing all 3 loses data forever.</li>
 *     <li><b>with</b>: RS(4,2) erasure coding requires only 50% storage overhead (1.5× disk cost).
 *         Losing any 2 of 6 drives still allows 100% byte-for-byte data reconstruction.</li>
 * </ul>
 */
public class Demo_5_2_WhyErasureCoding {

    // TRY IT: change DATA_SHARDS to 8 and PARITY_SHARDS to 4 (RS(8,4)).
    //         Storage overhead stays 1.5x, but tolerates 4 drive failures instead of 2!
    //         Or try RS(12,4): storage overhead drops to 1.33x (only 33% overhead vs 200% for 3x replication)!
    public static final int DATA_SHARDS = 4;
    public static final int PARITY_SHARDS = 2;

    public static final int SAMPLE_DATA_MB = 100;

    @Test
    @DisplayName("5.2 without · 3x replication requires 300% raw disk and loses data if 3 drives fail")
    void withoutErasureCoding_threeWayReplicationCosts3x() {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 5.2 (WITHOUT): 3× REPLICATION STORAGE OVERHEAD & DURABILITY");
        System.out.println("=".repeat(72));

        long originalBytes = SAMPLE_DATA_MB * 1024L * 1024L;
        int replicationFactor = 3;
        long totalStorageBytes = originalBytes * replicationFactor;
        double storageMultiplier = (double) totalStorageBytes / originalBytes;
        double overheadPct = (storageMultiplier - 1.0) * 100.0;

        System.out.printf("\n--- Workload: %,d MB Object under 3× Replication ---\n", SAMPLE_DATA_MB);
        System.out.printf("  Primary data:    %,8d MB\n", SAMPLE_DATA_MB);
        System.out.printf("  Replica count:   %d copies\n", replicationFactor);
        System.out.printf("  Total disk used: %,8d MB\n", (totalStorageBytes / (1024 * 1024)));
        System.out.printf("  Cost multiplier: %.1f× (%,.0f%% storage overhead)\n", storageMultiplier, overheadPct);

        System.out.println("\n--- Failure Simulation ---");
        System.out.println("  Initial state:   [Replica 1]  [Replica 2]  [Replica 3]  -> Healthy");
        System.out.println("  1 drive fails:   [  DEAD   ]  [Replica 2]  [Replica 3]  -> Degraded (2 copies remain)");
        System.out.println("  2 drives fail:   [  DEAD   ]  [  DEAD   ]  [Replica 3]  -> Critical! (1 copy left, 0 margin)");
        System.out.println("  3 drives fail:   [  DEAD   ]  [  DEAD   ]  [  DEAD   ]  -> PERMANENT DATA LOSS!");

        System.out.println("\n── the problem ──");
        System.out.println("  To tolerate 2 drive failures safely in multi-petabyte systems, 3× replication");
        System.out.println("  forces companies to pay for 300 TB of raw NVMe/HDD for every 100 TB of real data.");
        System.out.println("  At cloud scale, that 200% overhead costs millions of dollars per year.");

        assertEquals(3.0, storageMultiplier, 0.001);
    }

    @Test
    @DisplayName("5.2 with · RS(4,2) cuts storage cost in half (1.5x) while tolerating 2 drive failures")
    void withErasureCoding_reedSolomonCutsCostInHalf() throws IOException {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 5.2 (WITH): REED-SOLOMON ERASURE CODING RS(" + DATA_SHARDS + "," + PARITY_SHARDS + ")");
        System.out.println("=".repeat(72));

        int totalShards = DATA_SHARDS + PARITY_SHARDS;
        long originalBytes = SAMPLE_DATA_MB * 1024L * 1024L;
        long shardSize = ErasureCodec.getBytesInShard(DATA_SHARDS, (int) originalBytes);
        long totalStorageBytes = shardSize * totalShards;
        double storageMultiplier = (double) totalStorageBytes / originalBytes;
        double overheadPct = (storageMultiplier - 1.0) * 100.0;

        System.out.printf("\n--- Workload: %,d MB Object under RS(%d,%d) Erasure Coding ---\n",
                SAMPLE_DATA_MB, DATA_SHARDS, PARITY_SHARDS);
        System.out.printf("  Primary data:    %,8d MB\n", SAMPLE_DATA_MB);
        System.out.printf("  Data shards (K): %d  (each %,d MB)\n", DATA_SHARDS, (shardSize / (1024 * 1024)));
        System.out.printf("  Parity shards(M):%d  (each %,d MB)\n", PARITY_SHARDS, (shardSize / (1024 * 1024)));
        System.out.printf("  Total disk used: %,8d MB across %d storage nodes\n",
                (totalStorageBytes / (1024 * 1024)), totalShards);
        System.out.printf("  Cost multiplier: %.2f× (only %,.1f%% overhead vs 200%% in 3× replication!)\n",
                storageMultiplier, overheadPct);

        System.out.printf("\n--- Cost Comparison for %,d MB ---\n", SAMPLE_DATA_MB);
        System.out.printf("  3× Replication: %,d MB disk used\n", (SAMPLE_DATA_MB * 3));
        System.out.printf("  RS(%d,%d):       %,d MB disk used\n", DATA_SHARDS, PARITY_SHARDS, (totalStorageBytes / (1024 * 1024)));
        System.out.printf("  DISK SAVINGS:   %,d MB (%.1f%% raw capacity saved!)\n",
                (SAMPLE_DATA_MB * 3) - (totalStorageBytes / (1024 * 1024)),
                (1.0 - (storageMultiplier / 3.0)) * 100.0);

        // 2. Concrete Byte Encoding & Recovery
        byte[] originalData = "Erasure-Coding-Cuts-Storage-Overhead-In-Half-While-Preserving-Durability!"
                .repeat(20).getBytes(StandardCharsets.UTF_8);

        byte[][] shards = ErasureCodec.encode(originalData, DATA_SHARDS, PARITY_SHARDS);
        boolean[] shardPresent = new boolean[totalShards];
        Arrays.fill(shardPresent, true);

        System.out.println("\n--- Simulating 2 Node Failures out of 6 ---");
        // Kill shard 1 and shard 4
        int deadShardA = 1;
        int deadShardB = 4;
        shardPresent[deadShardA] = false;
        shardPresent[deadShardB] = false;
        shards[deadShardA] = null;
        shards[deadShardB] = null;

        System.out.printf("  Node %d (Data Shard %d):   DEAD\n", deadShardA, deadShardA);
        System.out.printf("  Node %d (Parity Shard %d): DEAD\n", deadShardB, deadShardB);
        System.out.printf("  Healthy surviving nodes:   4 (exact K=%d quorum required)\n", DATA_SHARDS);

        byte[] reconstructed = ErasureCodec.decode(shards, shardPresent, originalData.length, DATA_SHARDS, PARITY_SHARDS);

        assertArrayEquals(originalData, reconstructed, "Reconstructed data must match 100% bit-for-bit");
        System.out.println("  ✅ Reconstructed 100% byte-for-byte identical data from remaining 4 nodes!");

        System.out.println("\n── key takeaway ──");
        System.out.printf("  RS(%d,%d) tolerates the same 2 drive failures as 3× replication,\n", DATA_SHARDS, PARITY_SHARDS);
        System.out.printf("  but slashes raw storage overhead from 200%% down to %.1f%% (a 50%% cost reduction).\n", overheadPct);
    }
}
