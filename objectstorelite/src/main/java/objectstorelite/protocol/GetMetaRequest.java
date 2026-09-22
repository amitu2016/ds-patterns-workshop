package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import objectstorelite.model.VersionEntry;

public record GetMetaRequest(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key
) {
    @JsonCreator
    public GetMetaRequest {
    }
}
