package objectstorelite.protocol;

import java.util.List;

public record ListObjectsResponse(boolean success, String bucket, String prefix,
                                  List<String> keys, String error) {
    public static ListObjectsResponse ok(String bucket, String prefix, List<String> keys) {
        return new ListObjectsResponse(true, bucket, prefix, keys, null);
    }

    public static ListObjectsResponse fail(String bucket, String prefix, String error) {
        return new ListObjectsResponse(false, bucket, prefix, List.of(), error);
    }
}
