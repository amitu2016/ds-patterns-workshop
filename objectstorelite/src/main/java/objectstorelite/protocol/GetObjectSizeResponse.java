package objectstorelite.protocol;

public record GetObjectSizeResponse(boolean success, String bucket, String key, long size, String error) {
    public static GetObjectSizeResponse ok(String bucket, String key, long size) {
        return new GetObjectSizeResponse(true, bucket, key, size, null);
    }

    public static GetObjectSizeResponse fail(String bucket, String key, String error) {
        return new GetObjectSizeResponse(false, bucket, key, -1, error);
    }
}
