package objectstorelite.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Minimal metadata persisted alongside each shard or returned in shard responses.
 */
public record ShardMetadata(
        @JsonProperty("shardIndex") int shardIndex,
        @JsonProperty("shardSize") long shardSize,
        @JsonProperty("objectSize") long objectSize,
        @JsonProperty("dataShards") int dataShards,
        @JsonProperty("parityShards") int parityShards
) {
    @JsonCreator
    public ShardMetadata {
    }

    public String serialize() {
        return "index=" + shardIndex + "\n" +
                "shardSize=" + shardSize + "\n" +
                "objectSize=" + objectSize + "\n" +
                "dataShards=" + dataShards + "\n" +
                "parityShards=" + parityShards + "\n";
    }

    public static ShardMetadata parse(String raw) {
        int idx = -1;
        long shardSize = -1;
        long objectSize = -1;
        int data = -1;
        int parity = -1;
        String[] lines = raw.split("\\R");
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            String[] kv = line.split("=", 2);
            if (kv.length != 2) {
                continue;
            }
            switch (kv[0]) {
                case "index" -> idx = Integer.parseInt(kv[1]);
                case "shardSize" -> shardSize = Long.parseLong(kv[1]);
                case "objectSize" -> objectSize = Long.parseLong(kv[1]);
                case "dataShards" -> data = Integer.parseInt(kv[1]);
                case "parityShards" -> parity = Integer.parseInt(kv[1]);
                default -> {}
            }
        }
        if (idx < 0 || shardSize < 0 || objectSize < 0 || data < 0 || parity < 0) {
            throw new IllegalArgumentException("Incomplete shard metadata: " + raw);
        }
        return new ShardMetadata(idx, shardSize, objectSize, data, parity);
    }

    public String quorumKey() {
        return objectSize + ":" + dataShards + ":" + parityShards + ":" + shardSize;
    }
}
