package objectstorelite.protocol;

public record PutObjectResponse(boolean success, String bucket, String key, String versionId, String error) {
    public static PutObjectResponse ok(String bucket, String key, String versionId) {
        return new PutObjectResponse(true, bucket, key, versionId, null);
    }

    public static PutObjectResponse fail(String bucket, String key, String error) {
        return new PutObjectResponse(false, bucket, key, null, error);
    }
}
