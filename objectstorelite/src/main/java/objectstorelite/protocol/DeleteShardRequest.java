package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public record DeleteShardRequest(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("versionId") String versionId
) {
    @JsonCreator
    public DeleteShardRequest {
    }
}
