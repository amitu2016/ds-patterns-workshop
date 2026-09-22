package kafkalite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kafkalite.common.Config;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.server.BrokerServer;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SLIDE: One of the brokers is chosen as the Controller   (demo 2.1)
 *
 * <p>Demonstrates:
 * <ol>
 *   <li>Atomic controller election via ZooKeeper's ephemeral {@code /controller} node.</li>
 *   <li>Only the elected controller watches {@code /brokers/ids}.</li>
 *   <li>The threading bridge: ZooKeeper callbacks fire on ZooKeeper's {@code ...-EventThread},
 *       enqueue to {@link kafkalite.zookeeper.ZkEventQueue}, and are drained on the thread driving {@code tick()}.</li>
 *   <li>Metadata propagation: the controller pushes {@link kafkalite.cluster.UpdateMetadataRequest}
 *       over the network so regular brokers learn cluster membership.</li>
 * </ol>
 */
class Demo_2_1_ControllerElection extends ZookeeperTestHarness {

    // TRY IT: add ProcessId.of("broker-4") to BROKERS. Exactly one is still controller,
    //         and all four caches converge — nothing else in this demo changes.
    private static final List<ProcessId> BROKERS = List.of(
            ProcessId.of("broker-1"),
            ProcessId.of("broker-2"),
            ProcessId.of("broker-3"),
            ProcessId.of("broker-4"));

    /** The broker ids every cache should converge on: 1..N for N brokers. */
    private static Set<Integer> expectedBrokerIds() {
        return IntStream.rangeClosed(1, BROKERS.size()).boxed().collect(Collectors.toSet());
    }

    @Test
    @DisplayName("2.1 · brokers elect exactly one controller; metadata is pushed to all on the tick thread")
    void threeBrokersElectOneControllerAndReceiveMetadata() throws Exception {
        Map<ProcessId, Config> configs = configsFor(BROKERS.toArray(new ProcessId[0]));

        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    BrokerServer brokerServer = new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                    return brokerServer;
                })
                .start()) {

            System.out.printf("%n--- Starting %d brokers with controllers enabled ---%n", BROKERS.size());

            // 1. Wait for all brokers to initialize, register with ZooKeeper, and complete election
            cluster.tickUntil(cluster::areAllNodesInitialized);

            // Give a few ticks for ZK watches to drain and UpdateMetadata messages to deliver
            cluster.tickUntil(() -> BROKERS.stream()
                    .allMatch(id -> cluster.<BrokerServer>getNode(id).aliveBrokerIds()
                            .equals(expectedBrokerIds())));

            List<BrokerServer> brokers = BROKERS.stream()
                    .map(id -> cluster.<BrokerServer>getNode(id))
                    .toList();

            int electedLeader = brokers.get(0).controller().activeControllerId();
            System.out.println("\n--- Controller Election Result ---");
            System.out.println("Elected controller broker id: " + electedLeader);
            for (BrokerServer b : brokers) {
                System.out.println("broker-" + b.config().getBrokerId()
                        + " views controller as: " + b.controller().activeControllerId());
            }

            // All brokers must agree on who the controller is
            for (BrokerServer b : brokers) {
                assertEquals(electedLeader, b.controller().activeControllerId());
            }

            // Exactly one broker is actively acting as controller
            long controllerCount = brokers.stream()
                    .filter(b -> b.controller().isController())
                    .count();
            assertEquals(1, controllerCount, "Exactly one broker must be the active controller");

            BrokerServer activeController = brokers.stream()
                    .filter(b -> b.controller().isController())
                    .findFirst()
                    .orElseThrow();

            System.out.println("\nActive controller: broker-" + activeController.config().getBrokerId()
                    + " with epoch " + activeController.controller().epoch());

            // ---- Verify Threading Model ----
            // Real evidence: watch callback fires on ZK's EventThread, but state changes run on tick thread
            String zkThread = activeController.controller().lastZkCallbackThread();
            String tickThread = activeController.controller().lastDrainedThread();

            System.out.println("\n--- Threading Model Verification ---");
            System.out.println("ZooKeeper Watch Callback Thread: " + zkThread);
            System.out.println("State Processing Tick Thread:     " + tickThread);

            assertNotNull(zkThread, "ZooKeeper watch must have fired");
            assertNotNull(tickThread, "Event must have been drained on the tick thread");
            assertTrue(zkThread.contains("EventThread"),
                    "Callback must execute on ZooKeeper's EventThread, was: " + zkThread);
            assertFalse(tickThread.contains("EventThread"),
                    "Drained handler must NOT execute on ZooKeeper's thread, was: " + tickThread);

            // ---- Verify Metadata Caches ----
            System.out.println("\n--- Metadata Cache Verification ---");
            for (BrokerServer b : brokers) {
                System.out.println("broker-" + b.config().getBrokerId()
                        + " metadata cache: " + b.aliveBrokerIds());
                assertEquals(expectedBrokerIds(), b.aliveBrokerIds());
            }

            System.out.println("""

                    ── what just happened ──
                    Every broker raced to create /controller.
                    ZooKeeper's atomic ephemeral creation ensured exactly ONE winner.
                    Only the winner (the controller) registered a watch on /brokers/ids.
                    When watch notifications arrived on ZooKeeper's EventThread, they were
                    enqueued to ZkEventQueue and drained cleanly on tickloom's tick thread.
                    The controller then broadcast UpdateMetadata to all brokers, populating
                    their local MetadataCache.
                    """);
        }
    }
}
