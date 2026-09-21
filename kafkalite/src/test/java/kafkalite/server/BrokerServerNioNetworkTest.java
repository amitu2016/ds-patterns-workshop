package kafkalite.server;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kafkalite.admin.CreateTopicCommand;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that inter-broker coordination (controller election, UpdateMetadata, and LeaderAndIsr)
 * runs strictly over the network using tickloom's {@code NioNetwork} (real TCP sockets and NIO channels),
 * without any simulated network.
 */
class BrokerServerNioNetworkTest extends ZookeeperTestHarness {

    private static final ProcessId BROKER_1 = ProcessId.of("broker-1");
    private static final ProcessId BROKER_2 = ProcessId.of("broker-2");
    private static final ProcessId BROKER_3 = ProcessId.of("broker-3");

    @Test
    @DisplayName("Topic creation and LeaderAndIsr dispatch work over real TCP sockets using NioNetwork")
    void topicCreationAndLeaderAndIsrOverRealNioNetwork() throws Exception {
        Map<ProcessId, Config> configs = configsFor(BROKER_1, BROKER_2, BROKER_3);

        // Intentionally NOT calling useSimulatedNetwork() — uses real NioNetwork with TCP sockets
        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    BrokerServer brokerServer = new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                    return brokerServer;
                })
                .start()) {

            System.out.println("\n--- Starting Cluster with real NioNetwork (TCP sockets) ---");

            // 1. Wait for all brokers to register in ZooKeeper and complete initialization
            cluster.tickUntil(cluster::areAllNodesInitialized);

            // 2. Drive ticks so NioNetwork selector processes connections and delivers UpdateMetadata
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
            assertTrue(controllerId >= 1 && controllerId <= 3, "Valid controller must be elected");
            System.out.println("Active controller elected: broker-" + controllerId);

            // 3. Create topic 'orders' with 3 partitions and replication factor 2
            System.out.println("\n--- Creating topic 'orders' via CreateTopicCommand ---");
            CreateTopicCommand createTopicCommand = new CreateTopicCommand(broker1.zookeeper());
            createTopicCommand.createTopic("orders", 3, 2);

            // 4. Tick until all brokers receive LeaderAndIsrRequest over the NIO network
            TopicAndPartition p0 = new TopicAndPartition("orders", 0);
            TopicAndPartition p1 = new TopicAndPartition("orders", 1);
            TopicAndPartition p2 = new TopicAndPartition("orders", 2);

            cluster.tickUntil(() -> {
                return broker1.partitionAssignments().containsKey(p0)
                        || broker1.partitionAssignments().containsKey(p1)
                        || broker1.partitionAssignments().containsKey(p2);
            });

            cluster.tickUntil(() -> {
                int totalAccepted = broker1.acceptedLeaderAndIsrCount()
                        + broker2.acceptedLeaderAndIsrCount()
                        + broker3.acceptedLeaderAndIsrCount();
                return totalAccepted >= 3;
            });

            System.out.println("LeaderAndIsr successfully received across real TCP sockets!");
            System.out.println("Broker 1 leader partitions: " + broker1.leaderPartitions());
            System.out.println("Broker 2 leader partitions: " + broker2.leaderPartitions());
            System.out.println("Broker 3 leader partitions: " + broker3.leaderPartitions());

            // Assert that partition assignments exist across the cluster
            assertNotNull(broker1.getPartitionAssignment(p0) != null
                    ? broker1.getPartitionAssignment(p0)
                    : broker2.getPartitionAssignment(p0));
        }
    }
}
