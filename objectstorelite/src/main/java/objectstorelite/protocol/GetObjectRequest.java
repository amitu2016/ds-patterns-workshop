package objectstorelite.protocol;

public record GetObjectRequest(String bucket, String key) {
    public GetObjectRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
    }
}
