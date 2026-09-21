package kafkalite.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import kafkalite.common.TopicAndPartition;

/**
 * Request to fetch messages starting at an offset.
 *
 * <p>When {@code replicaId == -1}, this is a client consumer request subject to {@link FetchIsolation#HIGH_WATERMARK}.
 * When {@code replicaId >= 0}, this is a follower replica fetching from the leader, which uses {@link FetchIsolation#LOG_END}
 * and causes the leader to record follower progress for high watermark calculation.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FetchRequest(
        @JsonProperty("topicAndPartition") TopicAndPartition topicAndPartition,
        @JsonProperty("offset") long offset,
        @JsonProperty("replicaId") int replicaId,
        @JsonProperty("fetchIsolation") FetchIsolation fetchIsolation
) {
    public static final int CONSUMER_REPLICA_ID = -1;

    public FetchRequest(TopicAndPartition topicAndPartition, long offset) {
        this(topicAndPartition, offset, CONSUMER_REPLICA_ID, FetchIsolation.HIGH_WATERMARK);
    }

    public FetchRequest(TopicAndPartition topicAndPartition, long offset, int replicaId) {
        this(topicAndPartition, offset, replicaId, replicaId >= 0 ? FetchIsolation.LOG_END : FetchIsolation.HIGH_WATERMARK);
    }

    @JsonIgnore
    public boolean isFromFollower() {
        return replicaId >= 0;
    }
}
