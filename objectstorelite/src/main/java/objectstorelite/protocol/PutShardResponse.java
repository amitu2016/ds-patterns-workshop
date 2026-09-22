package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record PutShardResponse(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("shardIndex") int shardIndex,
        @JsonProperty("success") boolean success,
        @JsonProperty("error") String error
) {
    @JsonCreator
    public PutShardResponse {
    }

    public static PutShardResponse ok(String bucket, String key, int shardIndex) {
        return new PutShardResponse(bucket, key, shardIndex, true, null);
    }

    public static PutShardResponse fail(String bucket, String key, int shardIndex, String error) {
        return new PutShardResponse(bucket, key, shardIndex, false, error);
    }
}
