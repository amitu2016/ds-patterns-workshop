package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record GetShardRequest(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("versionId") String versionId,
        @JsonProperty("shardIndex") int shardIndex,
        @JsonProperty("offset") long offset,
        @JsonProperty("length") int length
) {
    @JsonCreator
    public GetShardRequest {
    }

    public static GetShardRequest full(String bucket, String key, String versionId, int shardIndex) {
        return new GetShardRequest(bucket, key, versionId, shardIndex, 0, -1);
    }

    public static GetShardRequest range(String bucket, String key, String versionId, int shardIndex, long offset, int length) {
        return new GetShardRequest(bucket, key, versionId, shardIndex, offset, length);
    }
}
