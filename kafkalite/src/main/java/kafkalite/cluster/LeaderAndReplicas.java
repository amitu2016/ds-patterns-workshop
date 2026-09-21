package kafkalite.cluster;

import kafkalite.common.TopicAndPartition;

import java.util.Objects;

/**
 * Mirrors {@code kafka.controller.LeaderAndIsr} / {@code LeaderAndReplicas}.
 *
 * <p>Associates a topic partition with its leadership assignment.
 */
public record LeaderAndReplicas(TopicAndPartition topicPartition, PartitionStateInfo partitionStateInfo) {
    public LeaderAndReplicas {
        Objects.requireNonNull(topicPartition, "topicPartition must not be null");
        Objects.requireNonNull(partitionStateInfo, "partitionStateInfo must not be null");
    }
}
