package deltalite;

import deltalite.actions.*;
import deltalite.util.FileNames;
import deltalite.util.JsonUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * DeltaLog is responsible for managing the transaction log of a Delta table.
 * It handles reading and writing log files, maintaining table state, and
 * providing the necessary infrastructure for optimistic concurrency control.
 */
public class DeltaLog {

    /**
     * Path to the table's log directory
     */
    private final Path logPath;

    /**
     * Path to the table's data directory
     */
    private final Path dataPath;

    /**
     * Lock for coordination of concurrent operations
     */
    private final ReentrantLock deltaLogLock = new ReentrantLock();

    /**
     * Current snapshot of the table state
     */
    private Snapshot currentSnapshot;
    /**
     * Creates a DeltaLog for the specified table path.
     *
     * @param tablePath the path to the Delta table
     */
    private DeltaLog(String tablePath) {
        Path tablePathObj = Paths.get(tablePath);
        this.logPath = tablePathObj.resolve("_delta_log");
        this.dataPath = tablePathObj.resolve("data");
        this.currentSnapshot = new Snapshot(this, -1, new ArrayList<>());
    }

    /**
     * Gets or creates a DeltaLog for the specified table path.
     *
     * @param tablePath the path to the Delta table
     * @return a DeltaLog instance
     */
    public static DeltaLog forTable(String tablePath) {
        return new DeltaLog(tablePath);
    }

    /**
     * Updates the snapshot to the latest version.
     *
     * @return the updated snapshot
     * @throws IOException if an I/O error occurs
     */
    public Snapshot update() throws IOException {
        try {
            deltaLogLock.lock();

            // Get the latest version
            long latestVersion = getLatestVersion();

            // If no change or no log files exist yet, return current snapshot
            if (latestVersion == currentSnapshot.getVersion()) {
                return currentSnapshot;
            }

            List<Action> allActions = new ArrayList<>();

            // No usable checkpoint, read all actions from all versions
            List<Long> versions = listVersions();
            for (Long version : versions) {
                allActions.addAll(readVersion(version));
            }

            // Create a new snapshot with the complete state
            currentSnapshot = new Snapshot(this, latestVersion, allActions);
            return currentSnapshot;
        } finally {
            deltaLogLock.unlock();
        }
    }

    /**
     * Reads the actions for a specific version.
     *
     * @param version the version to read
     * @return the list of actions for that version
     * @throws IOException if an I/O error occurs
     */
    public List<Action> readVersion(long version) throws IOException {
        List<Action> actions = new ArrayList<>();
        Path versionFile = logPath.resolve(FileNames.deltaFile(version));

        if (!Files.exists(versionFile)) {
            return actions;
        }

        List<String> lines = Files.readAllLines(versionFile);
        for (String line : lines) {
            if (!line.trim().isEmpty()) {
                Action action = JsonUtil.fromJson(line);
                actions.add(action);
            }
        }

        return actions;
    }

    /**
     * Gets the current snapshot of the table state.
     *
     * @return the current snapshot
     * @throws IOException if an I/O error occurs
     */
    public Snapshot snapshot() throws IOException {
        return update();
    }

    /**
     * Lists all versions in the log directory.
     *
     * @return a list of version numbers
     * @throws IOException if an I/O error occurs
     */
    public List<Long> listVersions() throws IOException {
        if (!Files.exists(logPath)) {
            return new ArrayList<>();
        }

        return Files.list(logPath)
                .filter(file -> file.getFileName().toString().matches("\\d{20}\\.json"))
                .map(file -> {
                    String fileName = file.getFileName().toString();
                    String versionStr = fileName.substring(0, fileName.indexOf('.'));
                    return Long.parseLong(versionStr);
                })
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Gets the latest version in the log.
     *
     * @return the latest version, or -1 if no versions exist
     * @throws IOException if an I/O error occurs
     */
    public long getLatestVersion() throws IOException {
        List<Long> versions = listVersions();
        return versions.isEmpty() ? -1 : versions.get(versions.size() - 1);
    }


    /**
     * Writes actions to a new version file.
     *
     * @param version the version to write
     * @param actions the actions to write
     * @throws IOException if an I/O error occurs
     */
    public void write(long version, List<Action> actions) throws IOException {
        try {
            deltaLogLock.lock();

            // Ensure log directory exists
            Files.createDirectories(logPath);

            // Create the log file for this version
            Path versionFile = logPath.resolve(FileNames.deltaFile(version));

            // Write to a temp file, then publish it under the version name.
            //
            // The structure here is Delta's, from HDFSLogStore.writeInternal: create a temp
            // path, write the actions, rename it into place without overwriting, and delete
            // the temp if the rename did not land. Two guarantees are needed, and they are
            // easy to conflate:
            //
            //   put-if-absent  — only one writer may claim version N, so a second writer
            //                    cannot overwrite a commit.
            //   atomic publish — the version file appears complete or not at all, so a reader
            //                    can never observe half a transaction's actions.
            //
            // Delta gets both from Hadoop's FileContext.rename(src, dst, Options.Rename.NONE),
            // which is atomic AND throws FileAlreadyExistsException on HDFS.
            //
            // java.nio has no equivalent, and this is the interesting part:
            //   * CREATE_NEW claims the name but lets a reader read the file mid-write.
            //   * Files.move(ATOMIC_MOVE) publishes atomically but POSIX rename() OVERWRITES
            //     unconditionally, silently losing the first commit. Verified twice: the version
            //     of this method using ATOMIC_MOVE failed DeltaLogAtomicityTest, and OpenJDK's
            //     UnixFileSystem.move() (src/java.base/unix/classes -- Linux as well as macOS)
            //     delegates straight to rename() when atomicMove is set, skipping the
            //     exists/REPLACE_EXISTING check its non-atomic path performs. Asking for
            //     ATOMIC_MOVE removes a safety check rather than adding one.
            //
            // Delta hits exactly this on a local filesystem and works around it with a racy
            // pre-check, whose comment says so outright:
            //
            //     if (!overwrite && fc.util.exists(path)) {
            //       // This is needed for the tests to throw error with local file system
            //
            // We use Files.createLink instead: atomic, and fails with EEXIST. Stronger than
            // Delta's local-filesystem path, which is why Delta is documented as unsafe for
            // concurrent writers there, and why S3 — atomic PUT, but overwriting — needed a
            // conditional PUT or a DynamoDB lock for years.
            Path tempFile = logPath.resolve(FileNames.deltaFile(version) + ".tmp."
                    + java.util.UUID.randomUUID());
            try (var writer = Files.newBufferedWriter(tempFile)) {
                for (Action action : actions) {
                    writer.write(JsonUtil.toJson(action));
                    writer.newLine();
                }
                writer.flush();
            }
            try {
                Files.createLink(versionFile, tempFile);
            } catch (IOException e) {
                Files.deleteIfExists(tempFile);
                throw e;
            }
            Files.delete(tempFile);

            // Update the snapshot after writing
            update();

        } finally {
            deltaLogLock.unlock();
        }
    }

    /**
     * Gets the path to the table.
     *
     * @return the table path as a string
     */
    public String getTablePath() {
        return logPath.getParent().toString();
    }

    /**
     * Checks if the table exists (has at least one log file).
     *
     * @return true if the table exists, false otherwise
     */
    public boolean tableExists() {
        try {
            return Files.exists(logPath) && !listVersions().isEmpty();
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Starts a new optimistic transaction for this table.
     *
     * @return a new transaction
     * @throws IOException if an I/O error occurs
     */
    public OptimisticTransaction startTransaction() throws IOException {
        return new OptimisticTransaction(getTablePath());
    }
} 