package kafkalite.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import kafkalite.common.TopicAndPartition;

/**
 * Request to produce (append) a message to a partition.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProduceRequest(
        @JsonProperty("topicAndPartition") TopicAndPartition topicAndPartition,
        @JsonProperty("message") Message message,
        @JsonProperty("requiredAcks") short requiredAcks
) {
    public ProduceRequest(TopicAndPartition topicAndPartition, Message message) {
        this(topicAndPartition, message, (short) 1);
    }
}
