package objectstorelite.protocol;

public record PutObjectRequest(String bucket, String key, byte[] data, boolean ifNoneMatch) {
    public PutObjectRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
    }

    public PutObjectRequest(String bucket, String key, byte[] data) {
        this(bucket, key, data, false);
    }
}
