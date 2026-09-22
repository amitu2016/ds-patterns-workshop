package deltalite;

import deltalite.actions.Action;
import deltalite.actions.AddFile;
import deltalite.actions.Metadata;
import deltalite.actions.Protocol;
import deltalite.util.ParquetUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

/**
 * Represents a Delta table which is a directory containing data files and transaction logs.
 * This implementation provides a simplified version of the Delta Lake protocol.
 */
public class DeltaTable {
    
    private final String tablePath;
    private final DeltaLog deltaLog;
    
    /**
     * Creates a new Delta table at the specified path.
     *
     * @param tablePath the path where the table will be stored
     * @throws IOException if an I/O error occurs
     */
    public DeltaTable(String tablePath) throws IOException {
        this.tablePath = tablePath;
        this.deltaLog = DeltaLog.forTable(tablePath);
        
        // Create directories if they don't exist
        Path path = Paths.get(tablePath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
        
        Path dataPath = Paths.get(tablePath, "data");
        if (!Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
        }
        
        // Initialize the table if it doesn't exist
        if (!deltaLog.tableExists()) {
            initialize();
        }
    }
    
    /**
     * Initializes a new Delta table with protocol and metadata.
     *
     * @throws IOException if an I/O error occurs
     */
    /**
     * Writes the table's first commit: {@code Protocol} and {@code Metadata}, no data.
     *
     * <p>Mirrors real Delta, where version 0 of every table establishes the reader/writer
     * protocol versions and the table's identity before any data exists. It is why a Delta
     * table's first data commit is version 1, not 0 — the table is created, then written to.
     */
    private void initialize() throws IOException {
        deltaLog.write(0, List.of(
                new Protocol(),
                new Metadata(UUID.randomUUID().toString(), "deltalite-table", "parquet")));
    }

    /**
     * Publishes a set of actions as the next version of the table.
     *
     * <p>The next version comes from the log, not from a counter in this object. That matters
     * even before concurrency is on the table: two {@code DeltaTable} instances opened on the
     * same path are two independent writers, and an in-memory counter would have them both
     * start at 0 and overwrite each other's commits.
     *
     * <p>Versions are 0-based, as in real Delta: the first commit is
     * {@code _delta_log/00000000000000000000.json}.
     *
     * <p>This still assumes it wins the race for that version number — nothing here detects
     * another writer taking it first. That is what step 3's optimistic commit adds.
     */
    public long commit(List<Action> actions) throws IOException {
        long nextVersion = deltaLog.getLatestVersion() + 1;
        deltaLog.write(nextVersion, actions);
        return nextVersion;
    }
    /**
     * Inserts records into the Delta table. Each record is a map of column names to values.
     *
     * @param records the records to insert
     * @return the number of records inserted
     * @throws IOException if an I/O error occurs
     */
    public List<Action> insert(List<Map<String, String>> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return Collections.emptyList();
        }
        // Generate a unique file name
        String fileId = UUID.randomUUID().toString();
        long timestamp = Instant.now().toEpochMilli();
        String fileName = String.format("part-%s.parquet", fileId);
        
        // Create the full path to the data file
        Path dataFilePath = Paths.get(tablePath, "data", fileName);
        
        // Write the records to a Parquet file
        long fileSize = ParquetUtil.writeRecords(records, dataFilePath);


        // Create an AddFile action
        AddFile addFile = new AddFile(
                "data/" + fileName,
                fileSize,
                timestamp);
        
        return List.of(addFile);
    }
    
    /**
     * Reads all records from the Delta table.
     *
     * @return a list of records, where each record is a map of column names to values
     * @throws IOException if an I/O error occurs
     */
    public List<Map<String, String>> readAll() throws IOException {
        return readAtVersion(deltaLog.getLatestVersion());
    }

    /**
     * Reads the table as it existed at a given version.
     *
     * <p>This is the whole difference from step 1. There, "what is in this table" meant
     * "whatever .parquet files are in the directory right now" — a question with no answer
     * at a point in time. Here the log answers it: replay commits 0..version, collect the
     * {@code AddFile} actions, and read exactly those files. A data file that exists on disk
     * but is not named by a committed action is not part of the table.
     *
     * <p>Mirrors {@code DeltaLog.getSnapshotAt(version)} followed by a scan of its AddFiles.
     */
    public List<Map<String, String>> readAtVersion(long version) throws IOException {
        List<Map<String, String>> allRecords = new ArrayList<>();
        if (version < 0) {
            return allRecords;   // nothing has been committed yet
        }

        Snapshot snapshot = deltaLog.getSnapshotAt(version);
        for (AddFile addFile : snapshot.getActiveFiles()) {
            Path dataFilePath = Paths.get(tablePath, addFile.getPath());
            allRecords.addAll(ParquetUtil.readRecords(dataFilePath));
        }
        return allRecords;
    }

    /** The version of the most recent commit, or -1 if the table has never been written to. */
    public long currentVersion() throws IOException {
        return deltaLog.getLatestVersion();
    }
}