package objectstorelite.protocol;

import java.util.List;

/** Internal: one node's view of the keyspace. The coordinator unions these. */
public record ListKeysResponse(boolean success, List<String> keys, String error) {
    public static ListKeysResponse ok(List<String> keys) {
        return new ListKeysResponse(true, keys, null);
    }

    public static ListKeysResponse fail(String error) {
        return new ListKeysResponse(false, List.of(), error);
    }
}
