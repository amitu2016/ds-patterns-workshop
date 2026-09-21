package kafkalite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kafkalite.cluster.LeaderAndIsrRequest;
import kafkalite.cluster.LeaderAndReplicas;
import kafkalite.cluster.PartitionStateInfo;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.server.BrokerServer;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SLIDE: Two controllers at once   (demo 2.2)
 *
 * <p>Demonstrates what happens during a controller failover:
 * <ol>
 *   <li>Broker 1 is elected controller with epoch 1, and assigns partition {@code orders-0} to itself.</li>
 *   <li>Broker 1's ZooKeeper session drops (e.g. GC pause or network partition from ZooKeeper).</li>
 *   <li>Broker 2 notices the ephemeral {@code /controller} node vanished, wins re-election, and claims epoch 2.</li>
 *   <li>Broker 2 reassigns partition {@code orders-0} to Broker 2 with epoch 2.</li>
 *   <li>Stale Broker 1 wakes up, unaware it was replaced, and tries to send commands with epoch 1.</li>
 * </ol>
 *
 * <p><b>The without/with pair:</b>
 * <ul>
 *   <li>{@link #withoutEpochFencing_staleControllerOverwritesAssignments()}: without epoch checks,
 *       the broker blindly accepts the stale controller's command, leading to split-brain.</li>
 *   <li>{@link #withEpochFencing_staleControllerIsRejected()}: with epoch checks, the broker rejects
 *       any command stamped with an epoch lower than its current view.</li>
 * </ul>
 */
class Demo_2_2_EpochFencing extends ZookeeperTestHarness {

    // TRY IT: in the `without` test, change setEpochFencingEnabled(false) to true. The test then
    //         FAILS with "Timeout waiting for condition" — and that failure is the point: it is
    //         waiting for the leader to revert to broker-1, which now never happens because the
    //         stale request is rejected. One boolean is the whole difference between the two
    //         halves of this demo.
    //
    //         Larger exercise: assign several partitions instead of just orders-0. The zombie
    //         corrupts every one of them, because nothing about the bug is partition-specific.
    private static final ProcessId BROKER_1 = ProcessId.of("broker-1");
    private static final ProcessId BROKER_2 = ProcessId.of("broker-2");
    private static final ProcessId BROKER_3 = ProcessId.of("broker-3");

    private static final TopicAndPartition ORDERS_0 = new TopicAndPartition("orders", 0);

    @Test
    @DisplayName("2.2 (without) · stale controller with epoch 1 overwrites epoch 2 assignments, causing split-brain")
    void withoutEpochFencing_staleControllerOverwritesAssignments() throws Exception {
        Map<ProcessId, Config> configs = configsFor(BROKER_1, BROKER_2, BROKER_3);

        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    BrokerServer brokerServer = new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                    return brokerServer;
                })
                .start()) {

            cluster.tickUntil(cluster::areAllNodesInitialized);

            BrokerServer broker1 = cluster.getNode(BROKER_1);
            BrokerServer broker2 = cluster.getNode(BROKER_2);
            BrokerServer broker3 = cluster.getNode(BROKER_3);

            // ---- Phase 1: Controller 1 (epoch 1) assigns orders-0 to Broker 1 ----
            System.out.println("\n--- Phase 1: Controller 1 (epoch 1) active ---");
            assertEquals(1, broker1.controller().epoch());

            LeaderAndIsrRequest epoch1Assignment = new LeaderAndIsrRequest(
                    1, 1,
                    List.of(new LeaderAndReplicas(ORDERS_0, new PartitionStateInfo(1, List.of(broker1.aliveBrokers().get(0)))))
            );
            broker1.controller().channelManager().sendLeaderAndIsr(3, epoch1Assignment);

            cluster.tickUntil(() -> broker3.getLeaderFor(ORDERS_0) == 1);
            System.out.println("broker-3 current leader for orders-0: broker-" + broker3.getLeaderFor(ORDERS_0)
                    + " (epoch " + broker3.currentControllerEpoch() + ")");

            // ---- Phase 2: Broker 1's ZK session ends -> Broker 2 becomes controller (epoch 2) ----
            System.out.println("\n--- Phase 2: Broker 1 pauses/disconnects from ZooKeeper ---");
            broker1.zookeeper().close();

            cluster.tickUntil(() -> broker2.controller().isController() && broker2.controller().epoch() == 2);
            System.out.println("Broker 2 took over as controller with epoch 2!");

            // Controller 2 reassigns orders-0 to Broker 2 with epoch 2
            LeaderAndIsrRequest epoch2Assignment = new LeaderAndIsrRequest(
                    2, 2,
                    List.of(new LeaderAndReplicas(ORDERS_0, new PartitionStateInfo(2, List.of(broker2.aliveBrokers().get(0)))))
            );
            broker2.controller().channelManager().sendLeaderAndIsr(3, epoch2Assignment);
            cluster.tickUntil(() -> broker3.getLeaderFor(ORDERS_0) == 2);
            System.out.println("broker-3 accepted new assignment from Controller 2: leader is broker-"
                    + broker3.getLeaderFor(ORDERS_0) + " (epoch " + broker3.currentControllerEpoch() + ")");

            // ---- Phase 3: Without epoch fencing, zombie Broker 1 sends stale epoch 1 assignment ----
            System.out.println("\n--- Phase 3: Zombie Broker 1 resumes and sends stale epoch 1 command ---");
            System.out.println("Disabling epoch fencing on broker-3 to demonstrate the bug...");
            broker3.setEpochFencingEnabled(false);

            // Stale Controller 1 sends orders-0 assigned to Broker 1 with old epoch 1
            broker1.controller().channelManager().sendLeaderAndIsr(3, epoch1Assignment);

            cluster.tickUntil(() -> broker3.getLeaderFor(ORDERS_0) == 1);
            System.out.println("❌ SPLIT-BRAIN: broker-3 accepted stale epoch 1 assignment! Leader reverted to broker-"
                    + broker3.getLeaderFor(ORDERS_0));

            assertEquals(1, broker3.getLeaderFor(ORDERS_0),
                    "Without epoch fencing, stale controller silently overwrote valid assignment!");

            System.out.println("""

                    ── what failed ──
                    Broker 1 had lost its ZooKeeper session, but its process had not halted.
                    When it resumed, it sent a LeaderAndIsr request using its old epoch 1.
                    Because broker-3 did not enforce epoch ordering, it accepted the old
                    command and reverted orders-0 back to broker-1, causing split-brain.
                    """);
        }
    }

    @Test
    @DisplayName("2.2 (with) · epoch fencing rejects stale controller request with epoch 1, maintaining consistency")
    void withEpochFencing_staleControllerIsRejected() throws Exception {
        Map<ProcessId, Config> configs = configsFor(BROKER_1, BROKER_2, BROKER_3);

        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    BrokerServer brokerServer = new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                    return brokerServer;
                })
                .start()) {

            cluster.tickUntil(cluster::areAllNodesInitialized);

            BrokerServer broker1 = cluster.getNode(BROKER_1);
            BrokerServer broker2 = cluster.getNode(BROKER_2);
            BrokerServer broker3 = cluster.getNode(BROKER_3);

            // ---- Phase 1: Controller 1 (epoch 1) assigns orders-0 to Broker 1 ----
            System.out.println("\n--- Phase 1: Controller 1 (epoch 1) active ---");
            assertEquals(1, broker1.controller().epoch());

            LeaderAndIsrRequest epoch1Assignment = new LeaderAndIsrRequest(
                    1, 1,
                    List.of(new LeaderAndReplicas(ORDERS_0, new PartitionStateInfo(1, List.of(broker1.aliveBrokers().get(0)))))
            );
            broker1.controller().channelManager().sendLeaderAndIsr(3, epoch1Assignment);

            cluster.tickUntil(() -> broker3.getLeaderFor(ORDERS_0) == 1);
            System.out.println("broker-3 current leader for orders-0: broker-" + broker3.getLeaderFor(ORDERS_0)
                    + " (epoch " + broker3.currentControllerEpoch() + ")");

            // ---- Phase 2: Broker 1's ZK session ends -> Broker 2 becomes controller (epoch 2) ----
            System.out.println("\n--- Phase 2: Broker 1 pauses/disconnects from ZooKeeper ---");
            broker1.zookeeper().close();

            cluster.tickUntil(() -> broker2.controller().isController() && broker2.controller().epoch() == 2);
            System.out.println("Broker 2 took over as controller with epoch 2!");

            // Controller 2 reassigns orders-0 to Broker 2 with epoch 2
            LeaderAndIsrRequest epoch2Assignment = new LeaderAndIsrRequest(
                    2, 2,
                    List.of(new LeaderAndReplicas(ORDERS_0, new PartitionStateInfo(2, List.of(broker2.aliveBrokers().get(0)))))
            );
            broker2.controller().channelManager().sendLeaderAndIsr(3, epoch2Assignment);
            cluster.tickUntil(() -> broker3.getLeaderFor(ORDERS_0) == 2);
            System.out.println("broker-3 accepted new assignment from Controller 2: leader is broker-"
                    + broker3.getLeaderFor(ORDERS_0) + " (epoch " + broker3.currentControllerEpoch() + ")");

            // ---- Phase 3: With epoch fencing, zombie Broker 1's stale command is REJECTED ----
            System.out.println("\n--- Phase 3: Zombie Broker 1 resumes and sends stale epoch 1 command ---");
            System.out.println("Epoch fencing is ENABLED on broker-3 (epoch = " + broker3.currentControllerEpoch() + ")");

            int staleBefore = broker3.staleRequestCount();
            broker1.controller().channelManager().sendLeaderAndIsr(3, epoch1Assignment);

            // Give cluster a moment to deliver and process message
            cluster.tickUntil(() -> broker3.staleRequestCount() > staleBefore);

            // Leader for orders-0 remains broker-2!
            assertEquals(2, broker3.getLeaderFor(ORDERS_0),
                    "broker-3 must reject stale epoch 1 and preserve valid epoch 2 leader assignment");
            assertEquals(1, broker3.staleRequestCount(), "broker-3 must record 1 rejected stale request");

            System.out.println("✅ PROTECTED: broker-3 rejected stale epoch 1 command from zombie controller 1!");
            System.out.println("Current orders-0 leader remains broker-" + broker3.getLeaderFor(ORDERS_0));

            System.out.println("""

                    ── what succeeded ──
                    The generation clock (controllerEpoch) protected the cluster.
                    Even though Broker 1 still believed it was the controller, every request
                    carried epoch 1. Broker 3, having already witnessed epoch 2 from the
                    newly elected controller, rejected the stale request unconditionally.
                    Split-brain avoided.
                    """);
        }
    }
}
