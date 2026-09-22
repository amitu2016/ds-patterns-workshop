package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import objectstorelite.model.ShardMetadata;

public record PutShardRequest(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("versionId") String versionId,
        @JsonProperty("shardIndex") int shardIndex,
        @JsonProperty("shardData") byte[] shardData,
        @JsonProperty("metadata") ShardMetadata metadata
) {
    @JsonCreator
    public PutShardRequest {
    }
}
