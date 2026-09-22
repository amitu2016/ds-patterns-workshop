package deltalite;

import com.tickloom.future.TickCompletableFuture;
import deltalite.actions.Action;
import deltalite.store.Storage;
import deltalite.util.FileNames;
import deltalite.util.JsonUtil;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * DeltaLog manages the append-only JSON transaction log of a Delta table stored in an object store.
 *
 * <p>Key differences from Steps 1–3 (local filesystem):
 * <ul>
 *   <li>I/O operations return {@link TickCompletableFuture} rather than blocking threads.</li>
 *   <li>Atomic publish and put-if-absent are achieved via object store conditional PUTs
 *       ({@code If-None-Match: *}) through {@link Storage#writeObjectIfAbsent}, rather than
 *       {@code Files.createLink}.</li>
 *   <li>Log replay is done in-memory over loaded commit action bytes.</li>
 * </ul>
 */
public class DeltaLog {

    private final String tablePath;
    private final Storage storage;
    private Snapshot currentSnapshot;

    public DeltaLog(String tablePath, Storage storage) {
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath");
        this.storage = Objects.requireNonNull(storage, "storage");
        this.currentSnapshot = new Snapshot(this, -1, new ArrayList<>());
    }

    public static DeltaLog forTable(String tablePath, Storage storage) {
        return new DeltaLog(tablePath, storage);
    }

    public String getTablePath() {
        return tablePath;
    }

    public Storage getStorage() {
        return storage;
    }

    public Snapshot getCurrentSnapshot() {
        return currentSnapshot;
    }

    public String logPrefix() {
        if (tablePath.isEmpty()) {
            return "_delta_log/";
        }
        return tablePath.endsWith("/") ? tablePath + "_delta_log/" : tablePath + "/_delta_log/";
    }

    public String versionPath(long version) {
        return logPrefix() + FileNames.deltaFile(version);
    }

    /**
     * Lists all commit versions found in the log directory, sorted ascending.
     */
    public TickCompletableFuture<List<Long>> listVersions() {
        return storage.listObjects(logPrefix()).thenApply(keys -> {
            List<Long> versions = new ArrayList<>();
            for (String key : keys) {
                String fileName = key.contains("/") ? key.substring(key.lastIndexOf('/') + 1) : key;
                if (fileName.matches("\\d{20}\\.json")) {
                    String versionStr = fileName.substring(0, fileName.indexOf('.'));
                    versions.add(Long.parseLong(versionStr));
                }
            }
            return versions.stream().sorted().collect(Collectors.toList());
        });
    }

    /**
     * Returns the highest committed version number, or -1 if no commits exist.
     */
    public TickCompletableFuture<Long> getLatestVersion() {
        return listVersions().thenApply(versions -> versions.isEmpty() ? -1L : versions.get(versions.size() - 1));
    }

    /**
     * Checks if the table has been initialized with at least one commit.
     */
    public TickCompletableFuture<Boolean> tableExists() {
        return listVersions().thenApply(versions -> !versions.isEmpty());
    }

    /**
     * Reads and parses all actions from a specific commit file.
     */
    public TickCompletableFuture<List<Action>> readVersion(long version) {
        String path = versionPath(version);
        return storage.readObject(path).thenApply(bytes -> {
            List<Action> actions = new ArrayList<>();
            String content = new String(bytes, StandardCharsets.UTF_8);
            String[] lines = content.split("\\r?\\n");
            for (String line : lines) {
                if (!line.trim().isEmpty()) {
                    actions.add(JsonUtil.fromJson(line));
                }
            }
            return actions;
        });
    }

    /**
     * Reads actions sequentially across a list of versions, preserving commit order
     * without requiring an external allOf future combinator.
     */
    public TickCompletableFuture<List<Action>> readVersions(List<Long> versions) {
        TickCompletableFuture<List<Action>> resultFuture = new TickCompletableFuture<>();
        List<Action> accumulated = new ArrayList<>();
        readNextVersion(versions.iterator(), accumulated, resultFuture);
        return resultFuture;
    }

    private void readNextVersion(Iterator<Long> iterator, List<Action> accumulated, TickCompletableFuture<List<Action>> resultFuture) {
        if (!iterator.hasNext()) {
            resultFuture.complete(accumulated);
            return;
        }
        long v = iterator.next();
        readVersion(v).whenComplete((actions, err) -> {
            if (err != null) {
                resultFuture.fail(err);
            } else {
                accumulated.addAll(actions);
                readNextVersion(iterator, accumulated, resultFuture);
            }
        });
    }

    /**
     * Replays the log up to the latest version and updates the internal snapshot.
     */
    public TickCompletableFuture<Snapshot> update() {
        return listVersions().thenCompose(versions -> {
            if (versions.isEmpty()) {
                return TickCompletableFuture.completed(currentSnapshot);
            }
            long latest = versions.get(versions.size() - 1);
            if (latest == currentSnapshot.getVersion()) {
                return TickCompletableFuture.completed(currentSnapshot);
            }
            return readVersions(versions).thenApply(allActions -> {
                currentSnapshot = new Snapshot(this, latest, allActions);
                return currentSnapshot;
            });
        });
    }

    /**
     * Constructs a snapshot at an arbitrary historical version V (time travel).
     */
    public TickCompletableFuture<Snapshot> getSnapshotAt(long targetVersion) {
        return listVersions().thenCompose(versions -> {
            List<Long> filtered = versions.stream()
                    .filter(v -> v <= targetVersion)
                    .sorted()
                    .collect(Collectors.toList());
            if (filtered.isEmpty()) {
                return TickCompletableFuture.completed(new Snapshot(this, -1, List.of()));
            }
            return readVersions(filtered).thenApply(actions ->
                    new Snapshot(this, targetVersion, actions));
        });
    }

    /**
     * Atomically writes a new version file using put-if-absent (If-None-Match: *).
     *
     * @param version the version number to claim
     * @param actions the list of actions in this transaction commit
     * @throws java.util.ConcurrentModificationException if version already exists
     */
    public TickCompletableFuture<Void> write(long version, List<Action> actions) {
        StringBuilder sb = new StringBuilder();
        for (Action action : actions) {
            sb.append(JsonUtil.toJson(action)).append("\n");
        }
        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        String path = versionPath(version);

        return storage.writeObjectIfAbsent(path, bytes)
                .thenCompose(v -> update().thenApply(s -> null));
    }
}
