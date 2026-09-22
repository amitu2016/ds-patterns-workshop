package deltalite;

import deltalite.actions.Action;
import deltalite.actions.Metadata;
import deltalite.actions.Protocol;
import deltalite.actions.AddFile;
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
     * Writes the table's first commit: {@code Protocol} and {@code Metadata}, no data.
     *
     * <p>Mirrors real Delta, where version 0 of every table establishes the reader/writer
     * protocol versions and the table's identity before any data exists.
     */
    private void initialize() throws IOException {
        deltaLog.write(0, List.of(
                new Protocol(),
                new Metadata(java.util.UUID.randomUUID().toString(), "deltalite-table", "parquet")));
    }

    /**
     * Starts a new optimistic transaction for this table.
     *
     * @return a new transaction
     * @throws IOException if an I/O error occurs
     */
    public OptimisticTransaction startTransaction() throws IOException {
        return deltaLog.startTransaction();
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
        List<Map<String, String>> allRecords = new ArrayList<>();

        // Get all active files at a snapshot
        List<AddFile> activeFiles = deltaLog.snapshot().getAllFiles();

        // Read each file and collect records
        for (AddFile addFile : activeFiles) {
            Path filePath = Paths.get(tablePath.toString(), addFile.getPath());
            List<Map<String, String>> fileRecords = ParquetUtil.readRecords(filePath);
            allRecords.addAll(fileRecords);
        }

        return allRecords;
    }
}