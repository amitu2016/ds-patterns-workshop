package objectstorelite.protocol;

public record GetObjectResponse(boolean success, String bucket, String key, byte[] data, String error) {
    public static GetObjectResponse ok(String bucket, String key, byte[] data) {
        return new GetObjectResponse(true, bucket, key, data, null);
    }

    public static GetObjectResponse fail(String bucket, String key, String error) {
        return new GetObjectResponse(false, bucket, key, null, error);
    }
}
