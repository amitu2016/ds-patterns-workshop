package kafkalite.common;

import java.util.Objects;

/**
 * <p>Serializable because a Spark task carrying partition metadata is shipped to a worker;
 * real Kafka's {@code TopicPartition} is serializable for the same reason.
 *
 * Mirrors {@code kafka.common.TopicAndPartition}.
 *
 * <p>Identifies a specific partition within a topic.
 */
public record TopicAndPartition(String topic, int partition) implements java.io.Serializable {
    public TopicAndPartition {
        Objects.requireNonNull(topic, "topic must not be null");
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
