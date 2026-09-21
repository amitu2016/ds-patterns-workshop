package kafkalite.demo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLIDE: What breaks when a node is added   (demo 3.1)
 *
 * <p>Demonstrates the fatal flaw of naive hash partitioning:
 * {@code node = hash(key) % numNodes}.
 *
 * <p>When a single node is added to a 3-node cluster (3 → 4 nodes), keys are re-hashed
 * modulo 4. Over <b>75% of all keys move to different nodes</b>, causing massive network
 * saturation, cache stampedes, or disk re-shuffling.
 */
class Demo_3_1_KeyMovement {

    // TRY IT: change to 4 and re-run. How many of the 10,000 keys change node? (3→4 moves ~75%;
    //         4→5 moves ~80%. It gets worse as the cluster grows, never better.)
    static final int INITIAL_NODES = 3;
    static final int TOTAL_KEYS = 10_000;

    @Test
    @DisplayName("3.1 · naive hash partitioning (%N → %N+1) reshuffles most of the keyspace")
    void addingNodeCausesMassiveKeyChurn() {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 3.1: WHAT BREAKS WHEN A NODE IS ADDED (NAIVE HASHING)");
        System.out.println("=".repeat(72));

        int newNodes = INITIAL_NODES + 1;
        List<String> keys = new ArrayList<>(TOTAL_KEYS);
        for (int i = 0; i < TOTAL_KEYS; i++) {
            keys.add("user-account-" + i);
        }

        // 1. Initial placement: hash(key) % 3
        Map<Integer, Integer> initialPlacement = new HashMap<>();
        Map<String, Integer> keyToInitialNode = new HashMap<>();
        for (String key : keys) {
            int node = Math.floorMod(key.hashCode(), INITIAL_NODES);
            keyToInitialNode.put(key, node);
            initialPlacement.put(node, initialPlacement.getOrDefault(node, 0) + 1);
        }

        System.out.printf("\n--- Initial State: %d keys distributed across %d nodes ---\n", TOTAL_KEYS, INITIAL_NODES);
        for (int node = 0; node < INITIAL_NODES; node++) {
            int count = initialPlacement.getOrDefault(node, 0);
            System.out.printf("  Node %d: %,5d keys (%4.1f%%)\n", node, count, (count * 100.0 / TOTAL_KEYS));
        }

        // 2. Add Node: hash(key) % 4
        System.out.printf("\n--- Adding Node %d: cluster size grows from %d → %d nodes ---\n",
                INITIAL_NODES, INITIAL_NODES, newNodes);

        Map<Integer, Integer> newPlacement = new HashMap<>();
        int movedKeys = 0;
        int stayedKeys = 0;

        for (String key : keys) {
            int oldNode = keyToInitialNode.get(key);
            int newNode = Math.floorMod(key.hashCode(), newNodes);
            newPlacement.put(newNode, newPlacement.getOrDefault(newNode, 0) + 1);

            if (oldNode != newNode) {
                movedKeys++;
            } else {
                stayedKeys++;
            }
        }

        for (int node = 0; node < newNodes; node++) {
            int count = newPlacement.getOrDefault(node, 0);
            System.out.printf("  Node %d: %,5d keys (%4.1f%%)\n", node, count, (count * 100.0 / TOTAL_KEYS));
        }

        double churnPct = (movedKeys * 100.0) / TOTAL_KEYS;
        double stayedPct = (stayedKeys * 100.0) / TOTAL_KEYS;

        System.out.println("\n--- Churn Statistics ---");
        System.out.printf("  Keys staying on same node: %,5d (%4.1f%%)\n", stayedKeys, stayedPct);
        System.out.printf("  Keys forced to migrate:    %,5d (%4.1f%%)\n", movedKeys, churnPct);

        System.out.println("\n── what failed ──");
        System.out.printf("With hash(key) %% N, adding 1 node forced %.1f%% of the dataset to move.\n", churnPct);
        System.out.println("In a cache, this causes an instant 75% cache miss spike.");
        System.out.println("In a storage or streaming system, 3 out of every 4 records must cross the network.");
        System.out.println("To fix this, we must decouple keys from nodes using FIXED VIRTUAL PARTITIONS.");
        System.out.println("=".repeat(72) + "\n");

        // Statistically, for hash % 3 vs hash % 4, ~75% of keys move (probability 3/4)
        assertTrue(churnPct > 70.0 && churnPct < 80.0,
                "Expected ~75% of keys to move, but was " + churnPct + "%");
    }
}
