package cassandralite.demo;

import cassandralite.cluster.CassandraCluster;
import cassandralite.gossip.Gossiper;
import cassandralite.heartbeat.ServerState;
import com.tickloom.ProcessId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Slide 1.6: Failure detection.
 *
 * <p>Demonstrates:
 * 1. Healthy cluster heartbeating: baseline inter-arrival interval is established in the
 *    {@link cassandralite.heartbeat.ArrivalWindow}.
 * 2. Node failure: when heartbeats cease, &phi; accrues continuously as time passes.
 * 3. Conviction threshold: once &phi; exceeds &Phi; = 8.0 (probability of heartbeat arriving this late
 *    is less than 10^-8), the node is deterministically convicted and marked {@link ServerState#DOWN}.
 * 4. Automatic recovery: when the node resumes and a new heartbeat arrives, it is immediately restored
 *    to {@link ServerState#UP}.
 *
 * <p>Run via: {@code make demo-1.6}
 */
// SLIDE (TODO): Node Failure Detection and Recovery
@DisplayName("Demo 1.6: Failure Detection (Phi Accrual)")
public class Demo_1_6_FailureDetection {

    // TRY IT: Change CONVICT_THRESHOLD to 5.0 or 12.0 to see how quickly nodes are convicted!
    static final double CONVICT_THRESHOLD = 8.0;

    @Test
    void phiAccrualDetectsNodeFailureAndRecoversOnResumption() throws Exception {
        System.out.println("======================================================================");
        System.out.println("  Demo 1.6 · \u03c6 (Phi) Accrual Failure Detection");
        System.out.println("  (Hayashibara et al. / Apache Cassandra CASSANDRA-2597)");
        System.out.println("======================================================================\n");

        ProcessId node1Id = ProcessId.of("node-1");
        ProcessId node2Id = ProcessId.of("node-2");
        ProcessId node3Id = ProcessId.of("node-3");

        try (CassandraCluster cluster = CassandraCluster.create(3, 1)) {
            Gossiper node1 = cluster.getNode(node1Id);
            Gossiper node2 = cluster.getNode(node2Id);
            Gossiper node3 = cluster.getNode(node3Id);

            // Phase 1: Establish steady-state heartbeats
            System.out.println("--- Phase 1: Healthy Cluster Warmup (10 rounds) ---");
            for (int round = 1; round <= 10; round++) {
                cluster.tick();
            }

            assertTrue(node1.isAlive(node3Id), "Node 3 should be alive during warmup");
            assertEquals(ServerState.UP, node1.getServerState(node3Id));
            double initialPhi = node1.getPhi(node3Id);
            System.out.printf("  Initial steady state: node-3 state=%s, \u03c6=%.2f (mean heartbeat interval=%.2f ticks)\n",
                    node1.getServerState(node3Id), initialPhi,
                    node1.failureDetector().getArrivalWindow(node3Id).mean());

            // Phase 2: Simulate failure of node-3 (pause ticking/heartbeats)
            System.out.println("\n--- Phase 2: Failure Injection (node-3 pauses heartbeating) ---");
            node3.pause();
            System.out.println("  💥 node-3 paused! Heartbeats cease while cluster ticks advance...\n");

            System.out.printf("  %-10s | %-12s | %-12s | %-20s\n", "Tick", "Elapsed", "\u03c6 (Phi)", "Liveness Verdict");
            System.out.println("  -----------+--------------+--------------+---------------------");

            boolean convicted = false;
            int convictionTick = -1;

            for (int step = 1; step <= 30; step++) {
                cluster.tick();
                long currentTick = node1.currentTick();
                double phi = node1.getPhi(node3Id);
                ServerState state = node1.getServerState(node3Id);

                if (step % 4 == 0 || state == ServerState.DOWN) {
                    System.out.printf("  Tick %-5d | %-8d ticks | \u03c6 = %-7.2f | %s\n",
                            currentTick,
                            currentTick - node1.failureDetector().getArrivalWindow(node3Id).getLastArrivalTick(),
                            phi,
                            state == ServerState.UP ? "UP (healthy/suspect)" : "🛑 CONVICTED -> DOWN (\u03c6 \u2265 8.0)");
                }

                if (state == ServerState.DOWN) {
                    convicted = true;
                    convictionTick = (int) currentTick;
                    break;
                }
            }

            System.out.println("  -----------+--------------+--------------+---------------------");
            assertTrue(convicted, "node-3 should have been convicted to DOWN");
            assertFalse(node1.isAlive(node3Id), "node-1 must report node-3 as dead");
            System.out.printf("\n  ✅ node-3 convicted to DOWN at tick %d once \u03c6 passed threshold %.1f\n",
                    convictionTick, CONVICT_THRESHOLD);

            // Phase 3: Healing / Resumption
            System.out.println("\n--- Phase 3: Node Resumption & Automatic Recovery ---");
            node3.resume();
            System.out.println("  🔄 node-3 resumed! Resuming gossip and heartbeat broadcast...");

            // Tick cluster to deliver resumed heartbeat to node-1
            cluster.tick();
            cluster.tick();

            assertTrue(node1.isAlive(node3Id), "node-3 should recover to alive");
            assertEquals(ServerState.UP, node1.getServerState(node3Id));
            System.out.printf("  ✅ node-3 state restored to %s (\u03c6 = %.2f)\n",
                    node1.getServerState(node3Id), node1.getPhi(node3Id));

            System.out.println("\n======================================================================");
            System.out.println("  Key Takeaway: \u03c6 Accrual decouples heartbeat monitoring from action.");
            System.out.println("  Continuous probability metric avoids false convictions during jitter,");
            System.out.println("  yet convicts rapidly when true failures occur.");
            System.out.println("======================================================================\n");
        }
    }
}
