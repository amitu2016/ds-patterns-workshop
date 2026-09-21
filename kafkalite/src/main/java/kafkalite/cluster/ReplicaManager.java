package kafkalite.cluster;

import kafkalite.api.FetchIsolation;
import kafkalite.api.Message;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.partition.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages all partition replicas hosted on a broker.
 *
 * <p>Mirrors {@code kafka.server.ReplicaManager}.
 * Responsibilities:
 * <ul>
 *   <li>Creating and storing local {@link Partition} instances.</li>
 *   <li>Handling {@link LeaderAndIsrRequest} to transition partitions between Leader and Follower roles.</li>
 *   <li>Executing client produce appends and fetch reads.</li>
 *   <li>Tracking follower fetch progress and advancing High-Watermark (HW).</li>
 * </ul>
 */
public class ReplicaManager implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicaManager.class);

    private final Config config;
    private final File logDir;
    private final Map<TopicAndPartition, Partition> partitions = new ConcurrentHashMap<>();

    public ReplicaManager(Config config) {
        this.config = config;
        this.logDir = new File(config.getLogDirs().get(0));
    }

    public ReplicaManager(int brokerId, File logDir) {
        this.config = new Config(brokerId, "localhost", 9092, "localhost:2181", List.of(logDir.getAbsolutePath()));
        this.logDir = logDir;
    }

    /**
     * Applies a LeaderAndIsrRequest from the controller.
     */
    public synchronized void becomeLeaderOrFollower(LeaderAndIsrRequest request) {
        int currentBrokerId = config.getBrokerId();
        for (LeaderAndReplicas lr : request.leaderReplicas()) {
            TopicAndPartition tp = lr.topicPartition();
            PartitionStateInfo stateInfo = lr.partitionStateInfo();
            if (stateInfo.leaderBrokerId() == currentBrokerId) {
                makeLeader(tp, stateInfo);
            } else {
                makeFollower(tp, stateInfo.leaderBrokerId());
            }
        }
    }

    public synchronized Partition makeLeader(TopicAndPartition tp, PartitionStateInfo stateInfo) {
        Partition partition = getOrCreatePartition(tp);
        partition.makeLeader(stateInfo);
        return partition;
    }

    public synchronized Partition makeFollower(TopicAndPartition tp, int leaderBrokerId) {
        Partition partition = getOrCreatePartition(tp);
        partition.makeFollower(leaderBrokerId);
        return partition;
    }

    public synchronized Partition getOrCreatePartition(TopicAndPartition tp) {
        return partitions.computeIfAbsent(tp, key -> {
            try {
                return new Partition(config.getBrokerId(), logDir, key);
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize partition " + key, e);
            }
        });
    }

    public Partition getPartition(TopicAndPartition tp) {
        return partitions.get(tp);
    }

    public long append(TopicAndPartition tp, Message message) throws IOException {
        Partition partition = partitions.get(tp);
        if (partition == null) {
            throw new IllegalArgumentException("Partition " + tp + " does not exist on broker " + config.getBrokerId());
        }
        if (!partition.isLeader()) {
            throw new IllegalStateException("Broker " + config.getBrokerId() + " is NOT leader for partition " + tp);
        }
        return partition.append(message);
    }

    public List<Message> read(TopicAndPartition tp, long startOffset, FetchIsolation isolation) throws IOException {
        Partition partition = partitions.get(tp);
        if (partition == null) {
            return List.of();
        }
        return partition.read(startOffset, isolation);
    }

    /**
     * Returns partitions where this broker is currently a follower, paired with the leader broker ID.
     */
    public Map<TopicAndPartition, Integer> followerPartitions() {
        Map<TopicAndPartition, Integer> followers = new LinkedHashMap<>();
        for (Map.Entry<TopicAndPartition, Partition> entry : partitions.entrySet()) {
            if (!entry.getValue().isLeader() && entry.getValue().leaderBrokerId() >= 0) {
                followers.put(entry.getKey(), entry.getValue().leaderBrokerId());
            }
        }
        return followers;
    }

    public Map<TopicAndPartition, Partition> allPartitions() {
        return Map.copyOf(partitions);
    }

    public int brokerId() {
        return config.getBrokerId();
    }

    @Override
    public void close() {
        for (Partition p : partitions.values()) {
            try {
                p.close();
            } catch (IOException e) {
                logger.warn("Error closing partition {}", p.topicAndPartition(), e);
            }
        }
        partitions.clear();
    }
}
