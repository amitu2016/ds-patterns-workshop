package objectstorelite.demo;

import objectstorelite.codec.ErasureSetMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SLIDE (TODO): Object key → erasure set   (demo 5.1)
 *
 * <p>Demonstrates MinIO-style deterministic set mapping:
 * {@code setIndex = hash(key, [deploymentId]) % numSets}.
 *
 * <p>Unlike naive node hashing or consistent hash rings, objects are partitioned into
 * fixed-size erasure sets of drives/nodes (e.g. 16 sets of 6 drives = 96 total drives).
 *
 * <p>Compares:
 * <ul>
 *     <li><b>CRCMOD</b>: Legacy MinIO algorithm using standard CRC32.</li>
 *     <li><b>SIPMOD</b>: Default MinIO algorithm using SipHash-2-4 keyed with cluster {@code deploymentId},
 *         preventing hash collisions and cross-deployment predictability.</li>
 * </ul>
 */
public class Demo_5_1_ErasureSetMapping {

    // TRY IT: change SET_COUNT to 8 or 16 and re-run. Watch how keys distribute evenly across sets.
    public static final int SET_COUNT = 4;
    public static final int SAMPLE_KEYS = 10_000;

    private static final byte[] DEPLOYMENT_A = "minio-cluster-01".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DEPLOYMENT_B = "minio-cluster-02".getBytes(StandardCharsets.UTF_8);

    @Test
    @DisplayName("5.1 · CRCMOD and SIPMOD map object keys deterministically across erasure sets")
    void demonstrateErasureSetMapping() {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 5.1: OBJECT KEY → ERASURE SET MAPPING (CRCMOD vs SIPMOD)");
        System.out.println("=".repeat(72));

        ErasureSetMapper crcMapper = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.CRCMOD, null, SET_COUNT);

        ErasureSetMapper sipMapperA = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.SIPMOD, DEPLOYMENT_A, SET_COUNT);

        ErasureSetMapper sipMapperB = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.SIPMOD, DEPLOYMENT_B, SET_COUNT);

        // 1. Single key placement & determinism
        String[] testKeys = {
                "photos/2026/beach.jpg",
                "warehouse/orders/part-000.parquet",
                "delta/table/_delta_log/000000.json",
                "videos/webinar-recording.mp4",
                "logs/server-01.log"
        };

        System.out.printf("\n--- 1. Deterministic Placement across %d Erasure Sets ---\n", SET_COUNT);
        System.out.printf("%-38s | %-10s | %-12s | %-12s\n", "Object Key", "CRCMOD", "SIPMOD (Dep A)", "SIPMOD (Dep B)");
        System.out.println("-".repeat(78));

        for (String key : testKeys) {
            int crcSet = crcMapper.indexFor(key);
            int sipSetA = sipMapperA.indexFor(key);
            int sipSetB = sipMapperB.indexFor(key);

            System.out.printf("%-38s | Set %-6d | Set %-8d | Set %-8d\n", key, crcSet, sipSetA, sipSetB);

            // Determinism assertion
            assertEquals(crcSet, crcMapper.indexFor(key), "CRCMOD must be 100% deterministic");
            assertEquals(sipSetA, sipMapperA.indexFor(key), "SIPMOD must be 100% deterministic");
        }

        // 2. Distribution analysis across 10,000 keys
        System.out.printf("\n--- 2. Distribution of %,d keys across %d Erasure Sets ---\n", SAMPLE_KEYS, SET_COUNT);

        Map<Integer, Integer> crcCounts = new HashMap<>();
        Map<Integer, Integer> sipCountsA = new HashMap<>();

        for (int i = 0; i < SAMPLE_KEYS; i++) {
            String key = "datasets/table/partition=" + (i % 20) + "/file-" + i + ".parquet";
            int crcSet = crcMapper.indexFor(key);
            int sipSet = sipMapperA.indexFor(key);

            crcCounts.put(crcSet, crcCounts.getOrDefault(crcSet, 0) + 1);
            sipCountsA.put(sipSet, sipCountsA.getOrDefault(sipSet, 0) + 1);
        }

        System.out.println("Set ID  | CRCMOD Count (Share)  | SIPMOD Count (Share)");
        System.out.println("-----------------------------------------------------");
        for (int s = 0; s < SET_COUNT; s++) {
            int cCount = crcCounts.getOrDefault(s, 0);
            int sCount = sipCountsA.getOrDefault(s, 0);
            System.out.printf("Set %2d  | %,6d (%5.1f%%)         | %,6d (%5.1f%%)\n",
                    s, cCount, (cCount * 100.0 / SAMPLE_KEYS),
                    sCount, (sCount * 100.0 / SAMPLE_KEYS));
        }

        // 3. Deployment Salt Isolation
        int saltDiffers = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "key-" + i;
            if (sipMapperA.indexFor(key) != sipMapperB.indexFor(key)) {
                saltDiffers++;
            }
        }
        System.out.println("\n--- 3. SIPMOD Deployment Salt Isolation ---");
        System.out.printf("  Different deploymentId changed placement for %d of 1,000 keys (%4.1f%%)\n",
                saltDiffers, (saltDiffers * 100.0 / 1000));
        assertTrue(saltDiffers > 0, "Different deployment IDs must change placement distribution");

        System.out.println("\n── key takeaway ──");
        System.out.println("  MinIO uses Erasure Sets instead of a consistent hash ring.");
        System.out.println("  Each object deterministically routes to one erasure set of K+M drives.");
        System.out.println("  Adding drives adds new erasure sets — existing sets and objects stay intact!");
    }
}
