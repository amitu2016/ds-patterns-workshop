package kafkalite.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import kafkalite.common.TopicAndPartition;

/**
 * Response for a produce request with the assigned offset.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProduceResponse(
        @JsonProperty("topicAndPartition") TopicAndPartition topicAndPartition,
        @JsonProperty("offset") long offset,
        @JsonProperty("errorCode") short errorCode
) {
    public static final short SUCCESS = 0;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short NOT_LEADER_FOR_PARTITION = 6;

    public ProduceResponse(TopicAndPartition topicAndPartition, long offset) {
        this(topicAndPartition, offset, SUCCESS);
    }
}
