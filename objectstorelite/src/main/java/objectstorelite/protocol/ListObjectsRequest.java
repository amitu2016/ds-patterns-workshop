package objectstorelite.protocol;

/**
 * Lists the keys in a bucket under a prefix. Mirrors S3's {@code ListObjectsV2}.
 *
 * <p>Needed by any table format built on object storage: a Delta table discovers its
 * versions by listing {@code _delta_log/}, because the log has no index of itself.
 */
public record ListObjectsRequest(String bucket, String prefix) {
    public ListObjectsRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
        if (prefix == null) prefix = "";
    }
}
