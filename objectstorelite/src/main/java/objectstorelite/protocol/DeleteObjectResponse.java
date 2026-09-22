package objectstorelite.protocol;

public record DeleteObjectResponse(boolean success, String bucket, String key, String versionId, String error) {
    public static DeleteObjectResponse ok(String bucket, String key, String versionId) {
        return new DeleteObjectResponse(true, bucket, key, versionId, null);
    }

    public static DeleteObjectResponse fail(String bucket, String key, String error) {
        return new DeleteObjectResponse(false, bucket, key, null, error);
    }
}
