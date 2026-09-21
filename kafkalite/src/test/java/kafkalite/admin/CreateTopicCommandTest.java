package kafkalite.admin;

import kafkalite.common.Config;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.zookeeper.PartitionInfo;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Ported from {@code com.workshop.admin.CreateTopicCommandTest}.
 *
 * <p>Validates topic creation against real ZooKeeper.
 */
class CreateTopicCommandTest extends ZookeeperTestHarness {

    private ZookeeperClient zkClient1;
    private ZookeeperClient zkClient2;
    private ZookeeperClient zkClient3;
    private CreateTopicCommand createTopicCommand;

    @BeforeEach
    void init() {
        zkClient1 = new ZookeeperClient(configFor(1));
        zkClient2 = new ZookeeperClient(configFor(2));
        zkClient3 = new ZookeeperClient(configFor(3));

        zkClient1.registerSelf();
        zkClient2.registerSelf();
        zkClient3.registerSelf();

        createTopicCommand = new CreateTopicCommand(zkClient1);
    }

    @AfterEach
    void cleanup() {
        if (zkClient1 != null) zkClient1.close();
        if (zkClient2 != null) zkClient2.close();
        if (zkClient3 != null) zkClient3.close();
    }

    @Test
    void testCreateTopicBasic() {
        createTopicCommand.createTopic("test-topic", 3, 2);

        List<String> topics = zkClient1.getAllTopics();
        assertTrue(topics.contains("test-topic"));

        List<PartitionInfo> partitionInfos = zkClient1.getPartitionInfoForTopic("test-topic");
        assertEquals(3, partitionInfos.size());

        partitionInfos.forEach(info -> {
            assertEquals(2, info.brokerIds().size(),
                    "Partition " + info.partitionId() + " should have 2 replicas");
        });
    }

    @Test
    void testCreateTopicWithMultiplePartitions() {
        createTopicCommand.createTopic("large-topic", 10, 2);

        List<PartitionInfo> partitionInfos = zkClient1.getPartitionInfoForTopic("large-topic");
        assertEquals(10, partitionInfos.size());

        for (int i = 0; i < 10; i++) {
            final int partitionId = i;
            assertTrue(partitionInfos.stream().anyMatch(info -> info.partitionId() == partitionId),
                    "Partition " + i + " should exist");
        }
    }

    @Test
    void testReplicationFactorTooLarge() {
        assertThrows(IllegalArgumentException.class, () ->
                createTopicCommand.createTopic("invalid-topic", 2, 4),
                "Should throw exception when replication factor > number of brokers"
        );

        List<String> topics = zkClient1.getAllTopics();
        assertFalse(topics.contains("invalid-topic"));
    }

    @Test
    void testCreateTopicWithNoBrokers() {
        zkClient1.close();
        zkClient2.close();
        zkClient3.close();

        Config adminConfig = configFor(99);
        ZookeeperClient adminZk = new ZookeeperClient(adminConfig);
        CreateTopicCommand cmd = new CreateTopicCommand(adminZk);

        assertThrows(IllegalStateException.class, () ->
                cmd.createTopic("no-broker-topic", 2, 2)
        );
        adminZk.close();
    }

    @Test
    void testReplicasOnDifferentBrokers() {
        createTopicCommand.createTopic("diverse-topic", 5, 2);

        List<PartitionInfo> partitionInfos = zkClient1.getPartitionInfoForTopic("diverse-topic");
        assertEquals(5, partitionInfos.size());

        partitionInfos.forEach(info -> {
            List<Integer> brokerIds = info.brokerIds();
            assertEquals(2, brokerIds.size());
            assertNotEquals(brokerIds.get(0), brokerIds.get(1),
                    "Partition " + info.partitionId() + " replicas should be on different brokers");
        });
    }
}
