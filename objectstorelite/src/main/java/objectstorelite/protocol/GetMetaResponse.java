package objectstorelite.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import objectstorelite.model.VersionEntry;

public record GetMetaResponse(
        @JsonProperty("bucket") String bucket,
        @JsonProperty("key") String key,
        @JsonProperty("exists") boolean exists,
        @JsonProperty("latestVersion") VersionEntry latestVersion,
        @JsonProperty("error") String error
) {
    @JsonCreator
    public GetMetaResponse {
    }

    public static GetMetaResponse ok(String bucket, String key, VersionEntry latestVersion) {
        return new GetMetaResponse(bucket, key, true, latestVersion, null);
    }

    public static GetMetaResponse notFound(String bucket, String key) {
        return new GetMetaResponse(bucket, key, false, null, null);
    }

    public static GetMetaResponse fail(String bucket, String key, String error) {
        return new GetMetaResponse(bucket, key, false, null, error);
    }
}
