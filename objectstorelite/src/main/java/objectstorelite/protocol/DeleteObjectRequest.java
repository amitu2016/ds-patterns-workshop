package objectstorelite.protocol;

public record DeleteObjectRequest(String bucket, String key) {
    public DeleteObjectRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
    }
}
