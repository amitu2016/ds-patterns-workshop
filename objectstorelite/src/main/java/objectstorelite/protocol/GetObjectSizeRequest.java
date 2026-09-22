package objectstorelite.protocol;

public record GetObjectSizeRequest(String bucket, String key) {
    public GetObjectSizeRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
    }
}
