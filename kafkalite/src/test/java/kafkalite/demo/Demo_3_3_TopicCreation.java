package kafkalite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kafkalite.admin.CreateTopicCommand;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.server.BrokerServer;
import kafkalite.zookeeper.PartitionInfo;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLIDE: Topic Creation uses fixed number of partitions   (demo 3.3)
 *
 * <p>Demonstrates the end-to-end Kafka topic creation and replica assignment workflow:
 * <ol>
 *   <li>Three brokers start and elect one controller.</li>
 *   <li>An admin executes {@link CreateTopicCommand} to create topic {@code "orders"} with
 *       6 partitions and replication factor 2.</li>
 *   <li>{@link kafkalite.common.ReplicaAssigner} generates balanced replica assignments without hotspots.</li>
 *   <li>Assignments are stored persistently in ZooKeeper at {@code /brokers/topics/orders}.</li>
 *   <li>The controller's {@code TopicChangeHandler} detects the new znode on ZooKeeper's
 *       EventThread and enqueues to {@link kafkalite.zookeeper.ZkEventQueue}.</li>
 *   <li>On the tick thread, the controller elects partition leaders and dispatches
 *       {@link kafkalite.cluster.LeaderAndIsrRequest} strictly over the network to all replica brokers.</li>
 *   <li>Each broker assumes its leader/follower roles with leadership balanced evenly across the cluster.</li>
 * </ol>
 */
class Demo_3_3_TopicCreation extends ZookeeperTestHarness {

    // TRY IT: change NUM_PARTITIONS to 9. Leadership remains balanced 3-3-3 across brokers!
    static final int NUM_PARTITIONS = 6;
    static final int REPLICATION_FACTOR = 2;
    static final String TOPIC_NAME = "orders";

    private static final ProcessId BROKER_1 = ProcessId.of("broker-1");
    private static final ProcessId BROKER_2 = ProcessId.of("broker-2");
    private static final ProcessId BROKER_3 = ProcessId.of("broker-3");

    @Test
    @DisplayName("3.3 · topic creation assigns replicas evenly; controller elects leaders and propagates assignments over the network")
    void topicCreationAndReplicaAssignment() throws Exception {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 3.3: TOPIC CREATION & REPLICA ASSIGNMENT");
        System.out.println("=".repeat(72));

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

            // 1. Wait for cluster to initialize
            cluster.tickUntil(cluster::areAllNodesInitialized);

            cluster.tickUntil(() -> {
                BrokerServer b1 = cluster.getNode(BROKER_1);
                BrokerServer b2 = cluster.getNode(BROKER_2);
                BrokerServer b3 = cluster.getNode(BROKER_3);
                return b1.aliveBrokerIds().equals(Set.of(1, 2, 3))
                        && b2.aliveBrokerIds().equals(Set.of(1, 2, 3))
                        && b3.aliveBrokerIds().equals(Set.of(1, 2, 3));
            });

            BrokerServer broker1 = cluster.getNode(BROKER_1);
            BrokerServer broker2 = cluster.getNode(BROKER_2);
            BrokerServer broker3 = cluster.getNode(BROKER_3);

            int controllerId = broker1.controller().activeControllerId();
            BrokerServer controllerBroker = cluster.getNode(ProcessId.of("broker-" + controllerId));
            System.out.printf("\n--- Step 1: Cluster online. Controller elected: Broker %d ---\n", controllerId);

            // 2. Create Topic
            System.out.printf("\n--- Step 2: Create topic '%s' with %d partitions, RF=%d ---\n",
                    TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);

            CreateTopicCommand createTopicCommand = new CreateTopicCommand(broker1.zookeeper());
            createTopicCommand.createTopic(TOPIC_NAME, NUM_PARTITIONS, REPLICATION_FACTOR);

            // 3. Verify ZooKeeper storage
            List<PartitionInfo> zkPartitionInfos = broker1.zookeeper().getPartitionInfoForTopic(TOPIC_NAME);
            List<PartitionInfo> sortedZkInfos = new ArrayList<>(zkPartitionInfos);
            sortedZkInfos.sort(Comparator.comparingInt(PartitionInfo::partitionId));

            System.out.println("\n--- Step 3: Stored in ZooKeeper (/brokers/topics/" + TOPIC_NAME + ") ---");
            for (PartitionInfo info : sortedZkInfos) {
                System.out.printf("  Partition %d -> Replicas: %s  (Leader: Broker %d)\n",
                        info.partitionId(), info.brokerIds(), info.brokerIds().get(0));
            }

            // 4. Tick until controller processes topic creation and brokers receive LeaderAndIsr
            TopicAndPartition lastPartition = new TopicAndPartition(TOPIC_NAME, NUM_PARTITIONS - 1);
            cluster.tickUntil(() -> {
                return broker1.partitionAssignments().containsKey(lastPartition)
                        || broker2.partitionAssignments().containsKey(lastPartition)
                        || broker3.partitionAssignments().containsKey(lastPartition);
            });

            cluster.tickUntil(() -> {
                int totalAccepted = broker1.acceptedLeaderAndIsrCount()
                        + broker2.acceptedLeaderAndIsrCount()
                        + broker3.acceptedLeaderAndIsrCount();
                return totalAccepted >= 3;
            });

            // 5. Inspect broker roles
            System.out.println("\n--- Step 4: Inter-broker LeaderAndIsr Propagation ---");
            printBrokerRoles(broker1);
            printBrokerRoles(broker2);
            printBrokerRoles(broker3);

            // Verify threading proof on controller
            String zkThread = controllerBroker.controller().lastZkCallbackThread();
            String drainedThread = controllerBroker.controller().lastDrainedThread();
            System.out.println("\n--- Threading Evidence on Controller ---");
            System.out.println("  Watch callback thread (ZooKeeper EventThread): " + zkThread);
            System.out.println("  Execution thread (tickloom drain thread):     " + drainedThread);

            assertNotNull(zkThread, "ZooKeeper watch must have fired");
            assertNotNull(drainedThread, "Event must have drained on tick thread");

            // Verify balanced leadership across brokers
            int totalLeaders = broker1.leaderPartitions().size()
                    + broker2.leaderPartitions().size()
                    + broker3.leaderPartitions().size();
            assertEquals(NUM_PARTITIONS, totalLeaders, "All partitions must have an elected leader");

            int expectedPerBroker = NUM_PARTITIONS / 3;
            assertEquals(expectedPerBroker, broker1.leaderPartitions().size(), "Broker 1 leadership count");
            assertEquals(expectedPerBroker, broker2.leaderPartitions().size(), "Broker 2 leadership count");
            assertEquals(expectedPerBroker, broker3.leaderPartitions().size(), "Broker 3 leadership count");

            System.out.println("\n── what succeeded ──");
            System.out.printf("Topic '%s' was created with %d partitions.\n", TOPIC_NAME, NUM_PARTITIONS);
            System.out.printf("Leadership is evenly balanced (%d partitions per broker) with no hotspots.\n", expectedPerBroker);
            System.out.println("Each broker received its partition assignments strictly via network messages.");
            System.out.println("=".repeat(72) + "\n");
        }
    }

    private void printBrokerRoles(BrokerServer broker) {
        int id = broker.config().getBrokerId();
        List<String> leaders = broker.leaderPartitions().stream().map(TopicAndPartition::toString).toList();
        List<String> followers = broker.followerPartitions().stream().map(TopicAndPartition::toString).toList();
        System.out.printf("  Broker %d: Leader for %d partitions %s | Follower for %d partitions %s\n",
                id, leaders.size(), leaders, followers.size(), followers);
    }
}
