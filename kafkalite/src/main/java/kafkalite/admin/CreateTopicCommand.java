package kafkalite.admin;

import kafkalite.common.ReplicaAssigner;
import kafkalite.zookeeper.PartitionInfo;
import kafkalite.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Mirrors {@code kafka.admin.TopicCommand.createTopic} / {@code com.workshop.admin.CreateTopicCommand}.
 *
 * <p>Coordinates topic creation:
 * <ol>
 *   <li>Discovers available brokers from ZooKeeper ({@code /brokers/ids}).</li>
 *   <li>Computes balanced partition-to-broker assignments with {@link ReplicaAssigner}.</li>
 *   <li>Stores assignments persistently in ZooKeeper at {@code /brokers/topics/{topicName}}.</li>
 * </ol>
 *
 * <p>Once written to ZooKeeper, the controller detects the new znode via {@code TopicChangeHandler},
 * elects leaders for each partition, and notifies all brokers via {@code LeaderAndIsrRequest}
 * and {@code UpdateMetadataRequest}.
 */
public class CreateTopicCommand {
    private static final Logger logger = LoggerFactory.getLogger(CreateTopicCommand.class);

    private final ZookeeperClient zookeeperClient;
    private final ReplicaAssigner replicaAssigner;

    public CreateTopicCommand(ZookeeperClient zookeeperClient, ReplicaAssigner replicaAssigner) {
        this.zookeeperClient = Objects.requireNonNull(zookeeperClient, "zookeeperClient");
        this.replicaAssigner = Objects.requireNonNull(replicaAssigner, "replicaAssigner");
    }

    public CreateTopicCommand(ZookeeperClient zookeeperClient) {
        this(zookeeperClient, new ReplicaAssigner());
    }

    /**
     * Creates a new topic in ZooKeeper.
     *
     * @param topicName name of the topic
     * @param noOfPartitions number of partitions
     * @param replicationFactor replication factor per partition
     */
    public void createTopic(String topicName, int noOfPartitions, int replicationFactor) {
        if (topicName == null || topicName.isBlank()) {
            throw new IllegalArgumentException("Topic name cannot be empty");
        }
        Set<Integer> brokerIds = zookeeperClient.getAllBrokerIds();
        if (brokerIds.isEmpty()) {
            throw new IllegalStateException("No brokers are registered in the cluster");
        }
        if (replicationFactor > brokerIds.size()) {
            throw new IllegalArgumentException(
                    "Replication factor (" + replicationFactor + ") cannot be larger than " +
                            "number of available brokers (" + brokerIds.size() + ")"
            );
        }

        List<Integer> sortedBrokers = new ArrayList<>(brokerIds);
        sortedBrokers.sort(Integer::compareTo);

        Set<PartitionInfo> partitionAssignments = replicaAssigner.assignReplicasToBrokers(
                sortedBrokers,
                noOfPartitions,
                replicationFactor
        );

        List<PartitionInfo> orderedAssignments = new ArrayList<>(partitionAssignments);
        orderedAssignments.sort(Comparator.comparingInt(PartitionInfo::partitionId));

        zookeeperClient.setPartitionInfoForTopic(topicName, orderedAssignments);
        logger.info("Created topic '{}' with {} partitions and replication factor {}: {}",
                topicName, noOfPartitions, replicationFactor, orderedAssignments);
    }
}
