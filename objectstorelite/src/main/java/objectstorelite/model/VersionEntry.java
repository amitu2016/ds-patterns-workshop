package objectstorelite.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Represents a single object version persisted inside xl.meta.
 */
public record VersionEntry(
        @JsonProperty("versionId") String versionId,
        @JsonProperty("modTimeMillis") long modTimeMillis,
        @JsonProperty("deleted") boolean deleted,
        @JsonProperty("objectSize") long objectSize,
        @JsonProperty("dataShards") int dataShards,
        @JsonProperty("parityShards") int parityShards,
        @JsonProperty("shardSize") long shardSize,
        @JsonProperty("shardFiles") List<String> shardFiles
) {
    @JsonCreator
    public VersionEntry {
        shardFiles = shardFiles != null ? List.copyOf(shardFiles) : List.of();
    }

    public static VersionEntry createNew(long objectSize,
                                         int dataShards,
                                         int parityShards,
                                         long shardSize,
                                         List<String> shardFiles) {
        return createWithId(UUID.randomUUID().toString(), objectSize, dataShards, parityShards, shardSize, shardFiles);
    }

    public static VersionEntry createWithId(String versionId,
                                            long objectSize,
                                            int dataShards,
                                            int parityShards,
                                            long shardSize,
                                            List<String> shardFiles) {
        return new VersionEntry(
                versionId,
                System.currentTimeMillis(),
                false,
                objectSize,
                dataShards,
                parityShards,
                shardSize,
                shardFiles
        );
    }

    public static VersionEntry createDeleteMarker(String versionId) {
        return new VersionEntry(
                versionId,
                System.currentTimeMillis(),
                true,
                0,
                0,
                0,
                0,
                List.of()
        );
    }

    public String serialize() {
        return "versionId=" + versionId + "\n" +
                "modTime=" + modTimeMillis + "\n" +
                "deleted=" + deleted + "\n" +
                "objectSize=" + objectSize + "\n" +
                "dataShards=" + dataShards + "\n" +
                "parityShards=" + parityShards + "\n" +
                "shardSize=" + shardSize + "\n" +
                "shardFiles=" + String.join(",", shardFiles) + "\n";
    }

    public static VersionEntry parse(String raw) {
        String versionId = null;
        long modTime = -1;
        boolean deleted = false;
        long objectSize = -1;
        int dataShards = -1;
        int parityShards = -1;
        long shardSize = -1;
        List<String> shardFiles = new ArrayList<>();

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
                case "versionId" -> versionId = kv[1];
                case "modTime" -> modTime = Long.parseLong(kv[1]);
                case "deleted" -> deleted = Boolean.parseBoolean(kv[1]);
                case "objectSize" -> objectSize = Long.parseLong(kv[1]);
                case "dataShards" -> dataShards = Integer.parseInt(kv[1]);
                case "parityShards" -> parityShards = Integer.parseInt(kv[1]);
                case "shardSize" -> shardSize = Long.parseLong(kv[1]);
                case "shardFiles" -> {
                    if (!kv[1].isBlank()) {
                        shardFiles = new ArrayList<>(Arrays.asList(kv[1].split(",")));
                    }
                }
                default -> {}
            }
        }

        if (versionId == null || modTime < 0) {
            throw new IllegalArgumentException("Incomplete version entry: " + raw);
        }
        return new VersionEntry(versionId, modTime, deleted, objectSize, dataShards, parityShards, shardSize, shardFiles);
    }
}
