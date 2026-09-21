package kafkalite.partition;

import kafkalite.api.FetchIsolation;
import kafkalite.api.Message;
import kafkalite.cluster.Broker;
import kafkalite.cluster.PartitionStateInfo;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A partition is a unit of parallelism and replication in kafkalite.
 * Each partition wraps an append-only commit {@link Log}.
 *
 * <p><b>Replication Roles:</b>
 * <ul>
 *   <li><b>Leader:</b> Accepts client writes, tracks replication progress for all replicas,
 *       and calculates the monotonically advancing {@code highWatermark}.</li>
 *   <li><b>Follower:</b> Fetches records from the leader and appends them to its local log.</li>
 * </ul>
 *
 * <p><b>Consistency & High Watermark:</b>
 * The {@code highWatermark} is the maximum offset replicated by all in-sync replicas (ISRs).
 * Consumers reading with {@link FetchIsolation#HIGH_WATERMARK} cannot read uncommitted data
 * above {@code highWatermark}, eliminating dirty reads and preventing data loss on leader failover.
 */
public class Partition implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Partition.class);
    private static final String LOG_FILE_SUFFIX = ".log";

    private final int brokerId;
    private final TopicAndPartition topicAndPartition;
    private final File logFile;
    private final Log log;

    private volatile boolean isLeader = false;
    private volatile int leaderBrokerId = -1;
    private final Map<Integer, Long> replicaOffsets = new ConcurrentHashMap<>();
    private volatile long highWatermark = 0L;

    public Partition(Config config, TopicAndPartition topicAndPartition) throws IOException {
        this(config.getBrokerId(), new File(config.getLogDirs().get(0)), topicAndPartition);
    }

    public Partition(int brokerId, File logDir, TopicAndPartition topicAndPartition) throws IOException {
        this.brokerId = brokerId;
        this.topicAndPartition = topicAndPartition;
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        this.logFile = new File(logDir, topicAndPartition.topic() + "-" + topicAndPartition.partition() + LOG_FILE_SUFFIX);
        this.log = new Log(logFile);
    }

    /**
     * Appends a message to this partition's commit log.
     */
    public synchronized long append(Message message) throws IOException {
        long offset = log.append(message);
        if (isLeader) {
            updateReplicaOffset(brokerId, offset);
        }
        return offset;
    }

    public synchronized long append(byte[] key, byte[] value) throws IOException {
        return append(new Message(key, value));
    }

    /**
     * Reads messages starting at {@code startOffset} according to the requested {@link FetchIsolation}.
     */
    public synchronized List<Message> read(long startOffset, FetchIsolation isolation) throws IOException {
        long maxOffset = (isolation == FetchIsolation.HIGH_WATERMARK) ? highWatermark : log.lastOffset();
        if (startOffset <= 0 || startOffset > maxOffset) {
            return List.of();
        }
        return log.read(startOffset, maxOffset);
    }

    public synchronized List<Message> read(long startOffset) throws IOException {
        return read(startOffset, FetchIsolation.HIGH_WATERMARK);
    }

    /**
     * Transitions this partition to LEADER role.
     */
    public synchronized void makeLeader(PartitionStateInfo stateInfo) {
        this.isLeader = true;
        this.leaderBrokerId = stateInfo.leaderBrokerId();

        // Initialize replica tracking for all replicas
        replicaOffsets.clear();
        for (Broker b : stateInfo.allReplicas()) {
            replicaOffsets.put(b.id(), 0L);
        }
        // Record our own log end offset
        updateReplicaOffset(brokerId, log.lastOffset());
        logger.info("Broker {} became LEADER for {} with replicas {}", brokerId, topicAndPartition, replicaOffsets.keySet());
    }

    /**
     * Transitions this partition to FOLLOWER role.
     */
    public synchronized void makeFollower(int leaderId) {
        this.isLeader = false;
        this.leaderBrokerId = leaderId;
        logger.info("Broker {} became FOLLOWER for {} (leader: {})", brokerId, topicAndPartition, leaderId);
    }

    /**
     * Updates replica progress offset and recalculates High-Watermark (HW).
     */
    public synchronized void updateReplicaOffset(int replicaId, long offset) {
        replicaOffsets.put(replicaId, offset);
        if (!replicaOffsets.isEmpty()) {
            long minOffset = Collections.min(replicaOffsets.values());
            if (minOffset > highWatermark) {
                logger.info("Broker {} partition {}: High Watermark advanced from {} to {}",
                        brokerId, topicAndPartition, highWatermark, minOffset);
                highWatermark = minOffset;
            }
        }
    }

    public synchronized void setHighWatermark(long hw) {
        this.highWatermark = hw;
    }

    public boolean isLeader() {
        return isLeader;
    }

    public int leaderBrokerId() {
        return leaderBrokerId;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long lastOffset() {
        return log.lastOffset();
    }

    public TopicAndPartition topicAndPartition() {
        return topicAndPartition;
    }

    public Map<Integer, Long> replicaOffsets() {
        return Map.copyOf(replicaOffsets);
    }

    public Log log() {
        return log;
    }

    @Override
    public void close() throws IOException {
        log.close();
    }
}
