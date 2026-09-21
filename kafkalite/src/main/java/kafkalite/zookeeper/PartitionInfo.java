package kafkalite.zookeeper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Mirrors {@code kafka.zk.TopicZNode} / {@code com.workshop.zookeeper.PartitionInfo}.
 *
 * <p>Represents partition assignment information for a topic stored in ZooKeeper at:
 * {@code /brokers/topics/{topicName}}.
 * Stores which partition ID maps to which broker IDs (replicas).
 */
public class PartitionInfo {
    private final int partitionId;
    private final List<Integer> brokerIds;

    @JsonCreator
    public PartitionInfo(@JsonProperty("partitionId") int partitionId,
                         @JsonProperty("brokerIds") List<Integer> brokerIds) {
        this.partitionId = partitionId;
        this.brokerIds = brokerIds == null ? List.of() : List.copyOf(brokerIds);
    }

    private PartitionInfo() {
        this(0, Collections.emptyList());
    }

    public int partitionId() {
        return partitionId;
    }

    public List<Integer> brokerIds() {
        return brokerIds;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public List<Integer> getBrokerIds() {
        return brokerIds;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        PartitionInfo that = (PartitionInfo) obj;
        return this.partitionId == that.partitionId &&
                Objects.equals(this.brokerIds, that.brokerIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionId, brokerIds);
    }

    @Override
    public String toString() {
        return "PartitionInfo[partitionId=" + partitionId + ", brokerIds=" + brokerIds + "]";
    }
}
