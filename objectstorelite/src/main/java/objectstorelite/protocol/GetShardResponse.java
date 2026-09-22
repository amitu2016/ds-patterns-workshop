package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import objectstorelite.model.ShardMetadata;

public record GetShardResponse(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("shardIndex") int shardIndex,
        @JsonProperty("success") boolean success,
        @JsonProperty("shardData") byte[] shardData,
        @JsonProperty("metadata") ShardMetadata metadata,
        @JsonProperty("error") String error
) {
    @JsonCreator
    public GetShardResponse {
    }

    public static GetShardResponse ok(String bucket, String key, int shardIndex, byte[] data, ShardMetadata metadata) {
        return new GetShardResponse(bucket, key, shardIndex, true, data, metadata, null);
    }

    public static GetShardResponse fail(String bucket, String key, int shardIndex, String error) {
        return new GetShardResponse(bucket, key, shardIndex, false, null, null, error);
    }
}
