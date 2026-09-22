package objectstorelite.protocol;

/** Internal: asks one storage node for the keys it holds under a prefix. */
public record ListKeysRequest(String bucket, String prefix) {
    public ListKeysRequest {
        if (bucket == null || bucket.isBlank()) bucket = "default";
        if (prefix == null) prefix = "";
    }
}
