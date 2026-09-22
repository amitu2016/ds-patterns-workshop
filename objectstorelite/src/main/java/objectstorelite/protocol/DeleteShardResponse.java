package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record DeleteShardResponse(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("success") boolean success,
        @JsonProperty("error") String error
) {
    @JsonCreator
    public DeleteShardResponse {
    }

    public static DeleteShardResponse ok(String bucket, String key) {
        return new DeleteShardResponse(bucket, key, true, null);
    }

    public static DeleteShardResponse fail(String bucket, String key, String error) {
        return new DeleteShardResponse(bucket, key, false, error);
    }
}
