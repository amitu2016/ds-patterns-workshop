package objectstorelite.protocol;

public record GetObjectRangeRequest(String bucket, String key, long offset, int length) {
    public GetObjectRangeRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
    }
}
