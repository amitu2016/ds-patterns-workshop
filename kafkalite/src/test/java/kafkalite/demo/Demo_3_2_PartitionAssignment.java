package kafkalite.demo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLIDE: Fixing the partition count   (demo 3.2)
 *
 * <p>Demonstrates how Kafka and modern distributed databases decouple data from physical nodes
 * using <b>Fixed Virtual Partitions</b>:
 * <ol>
 *   <li>The number of partitions is fixed (e.g. 12 partitions).</li>
 *   <li>{@code partition = hash(key) % numPartitions} is <b>immutable</b>: keys NEVER change partitions.</li>
 *   <li>Partitions are assigned to brokers. When a 4th broker is added (3 → 4 brokers):
 *       <ul>
 *         <li>Only 3 out of 12 partitions move (exactly 25%, proportional to 1/N).</li>
 *         <li>9 out of 12 partitions remain completely untouched.</li>
 *         <li>Zero keys need re-hashing or internal reshuffling.</li>
 *       </ul>
 *   </li>
 * </ol>
 */
class Demo_3_2_PartitionAssignment {

    // TRY IT: change NUM_PARTITIONS to 24 or 36. Whatever the count, exactly 1/N of the
    //         partitions move when the Nth broker joins — that ratio is the whole point.
    //         Keep it divisible by both 3 and 4, or the assignment cannot be even.
    static final int NUM_PARTITIONS = 12;
    static final int INITIAL_BROKERS = 3;
    static final int TOTAL_KEYS = 10_000;

    @Test
    @DisplayName("3.2 · fixing partition count decouples keys from nodes; adding a broker moves only 1/N of the partitions")
    void fixedPartitionsMinimizeDataMovement() {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 3.2: FIXING THE PARTITION COUNT (VIRTUAL PARTITIONING)");
        System.out.println("=".repeat(72));

        int newBrokers = INITIAL_BROKERS + 1;
        // The lesson is the ratio, so the arithmetic has to stay exact.
        assertEquals(0, NUM_PARTITIONS % INITIAL_BROKERS,
                "NUM_PARTITIONS must divide evenly across " + INITIAL_BROKERS + " brokers");
        assertEquals(0, NUM_PARTITIONS % newBrokers,
                "NUM_PARTITIONS must divide evenly across " + newBrokers + " brokers");

        // 1. Immutable key -> partition mapping
        List<String> keys = new ArrayList<>(TOTAL_KEYS);
        Map<String, Integer> keyToPartition = new HashMap<>();
        int[] keysPerPartition = new int[NUM_PARTITIONS];

        for (int i = 0; i < TOTAL_KEYS; i++) {
            String key = "user-account-" + i;
            keys.add(key);
            int partition = Math.floorMod(key.hashCode(), NUM_PARTITIONS);
            keyToPartition.put(key, partition);
            keysPerPartition[partition]++;
        }

        System.out.printf("\n--- Step 1: %d keys assigned to %d FIXED partitions (immutable) ---\n",
                TOTAL_KEYS, NUM_PARTITIONS);
        for (int p = 0; p < NUM_PARTITIONS; p++) {
            System.out.printf("  P%-2d: %,4d keys\n", p, keysPerPartition[p]);
        }

        // 2. Initial Partition -> Broker Assignment (12 partitions across 3 brokers = 4 each)
        // Partitions 1..12 (P1..P12) mapped 4 to each broker:
        // Broker 0: P1, P2, P3, P4
        // Broker 1: P5, P6, P7, P8
        // Broker 2: P9, P10, P11, P12
        Map<Integer, Integer> initialPartitionToBroker = new LinkedHashMap<>();
        int partitionsPerBroker = NUM_PARTITIONS / INITIAL_BROKERS;
        for (int p = 1; p <= NUM_PARTITIONS; p++) {
            int broker = (p - 1) / partitionsPerBroker;
            initialPartitionToBroker.put(p, broker);
        }

        System.out.printf("\n--- Step 2: Assign %d partitions across %d brokers (%d per broker) ---\n",
                NUM_PARTITIONS, INITIAL_BROKERS, partitionsPerBroker);
        for (int b = 0; b < INITIAL_BROKERS; b++) {
            final int brokerId = b;
            List<String> parts = new ArrayList<>();
            initialPartitionToBroker.forEach((p, br) -> { if (br == brokerId) parts.add("P" + p); });
            System.out.printf("  Broker %d hosts %d partitions: %s\n", b, parts.size(), parts);
        }

        // 3. Add a broker and rebalance. Each existing broker offloads its surplus — the
        //    difference between what it holds and the new fair share — from the tail of its
        //    block. Nothing is recomputed from keys; whole partitions simply change owner.
        int targetPerBroker = NUM_PARTITIONS / newBrokers;
        int offloadPerBroker = partitionsPerBroker - targetPerBroker;

        System.out.printf("\n--- Step 3: Add Broker %d (cluster grows to %d brokers; target: %d per broker) ---\n",
                INITIAL_BROKERS, newBrokers, targetPerBroker);

        Map<Integer, Integer> newPartitionToBroker = new LinkedHashMap<>(initialPartitionToBroker);
        for (int b = 0; b < INITIAL_BROKERS; b++) {
            int lastPartitionOfBroker = (b + 1) * partitionsPerBroker;
            for (int i = 0; i < offloadPerBroker; i++) {
                newPartitionToBroker.put(lastPartitionOfBroker - i, INITIAL_BROKERS);
            }
        }

        List<String> movedPartitions = new ArrayList<>();
        List<String> stationaryPartitions = new ArrayList<>();

        for (int p = 1; p <= NUM_PARTITIONS; p++) {
            int oldB = initialPartitionToBroker.get(p);
            int newB = newPartitionToBroker.get(p);
            if (oldB != newB) {
                movedPartitions.add("P" + p);
            } else {
                stationaryPartitions.add("P" + p);
            }
        }

        for (int b = 0; b < newBrokers; b++) {
            final int brokerId = b;
            List<String> parts = new ArrayList<>();
            newPartitionToBroker.forEach((p, br) -> { if (br == brokerId) parts.add("P" + p); });
            System.out.printf("  Broker %d hosts %d partitions: %s\n", b, parts.size(), parts);
        }

        // 4. Calculate exact key movement
        int keysMigrated = 0;
        for (String pStr : movedPartitions) {
            int pId = Integer.parseInt(pStr.substring(1));
            keysMigrated += keysPerPartition[pId - 1];
        }
        double partitionChurnPct = (movedPartitions.size() * 100.0) / NUM_PARTITIONS;
        double keyChurnPct = (keysMigrated * 100.0) / TOTAL_KEYS;

        System.out.println("\n--- Movement Summary ---");
        System.out.printf("  Stationary partitions: %2d / %d (%.1f%%)\n",
                stationaryPartitions.size(), NUM_PARTITIONS, (stationaryPartitions.size() * 100.0 / NUM_PARTITIONS));
        System.out.printf("  Migrated partitions:   %2d / %d (%.1f%%) → Partitions %s moved to Broker %d\n",
                movedPartitions.size(), NUM_PARTITIONS, partitionChurnPct, movedPartitions, INITIAL_BROKERS);
        System.out.printf("  Keys changing partition:   0 / %,d (0.0%%)  ← IMMUTABLE\n", TOTAL_KEYS);
        System.out.printf("  Keys migrating to Broker %d: %,d / %,d (%.1f%%)  ← EXACTLY 1/N\n",
                INITIAL_BROKERS, keysMigrated, TOTAL_KEYS, keyChurnPct);

        System.out.println("\n── what succeeded ──");
        System.out.printf("By fixing the partition count at %d:%n", NUM_PARTITIONS);
        System.out.println("  1. Naive Hashing (Demo 3.1):  75.0% of keys moved across the cluster!");
        System.out.printf("  2. Virtual Partitions (3.2):  only %.1f%% of data moved (strictly 1/%d)!\n",
                keyChurnPct, newBrokers);
        System.out.println("  3. 0 keys changed partition index; only entire partition files transfer to the new node.");
        System.out.println("=".repeat(72) + "\n");

        // Exactly 1/N of the partitions move, whatever N and whatever the partition count.
        assertEquals(targetPerBroker, movedPartitions.size(),
                "the new broker should receive exactly its fair share: " + NUM_PARTITIONS + "/" + newBrokers);
        assertEquals(NUM_PARTITIONS - targetPerBroker, stationaryPartitions.size(),
                "every other partition stays put");
        assertTrue(keyChurnPct > 20.0 && keyChurnPct < 30.0, "Only ~25% of keys should migrate to new broker");
    }
}
