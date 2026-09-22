package objectstorelite.protocol;

public record GetObjectRangeResponse(boolean success, String bucket, String key, byte[] data, String error) {
    public static GetObjectRangeResponse ok(String bucket, String key, byte[] data) {
        return new GetObjectRangeResponse(true, bucket, key, data, null);
    }

    public static GetObjectRangeResponse fail(String bucket, String key, String error) {
        return new GetObjectRangeResponse(false, bucket, key, null, error);
    }
}
