package kafkalite.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import kafkalite.common.TopicAndPartition;

import java.util.List;

/**
 * Response for a fetch request containing records and the partition high-watermark.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FetchResponse(
        @JsonProperty("topicAndPartition") TopicAndPartition topicAndPartition,
        @JsonProperty("messages") List<Message> messages,
        @JsonProperty("highWatermark") long highWatermark,
        @JsonProperty("errorCode") short errorCode
) {
    public static final short SUCCESS = 0;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    public static final short NOT_LEADER_FOR_PARTITION = 6;

    public FetchResponse(TopicAndPartition topicAndPartition, List<Message> messages, long highWatermark) {
        this(topicAndPartition, messages, highWatermark, SUCCESS);
    }
}
