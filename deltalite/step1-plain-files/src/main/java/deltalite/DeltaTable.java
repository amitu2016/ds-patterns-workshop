package deltalite;

import deltalite.util.ParquetUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a Delta table which is a directory containing data files and transaction logs.
 * This implementation provides a simplified version of the Delta Lake protocol.
 */
public class DeltaTable {

    private final String tablePath;

    /**
     * Creates a new Delta table at the specified path.
     *
     * @param tablePath the path where the table will be stored
     * @throws IOException if an I/O error occurs
     */
    public DeltaTable(String tablePath) throws IOException {
        this.tablePath = tablePath;

        // Create directories if they don't exist
        Path path = Paths.get(tablePath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }

        Path dataPath = Paths.get(tablePath, "data");
        if (!Files.exists(dataPath)) {
            Files.createDirectories(dataPath);
        }

    }

    /**
     * Inserts records into the Delta table. Each record is a map of column names to values.
     *
     * @param records the records to insert
     * @return the number of records inserted
     * @throws IOException if an I/O error occurs
     */
    public int insert(List<Map<String, String>> records) throws IOException {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        // One Parquet file per insert, named so two writers never collide.
        //
        // Note what is NOT recorded anywhere: that these records arrived together, when they
        // arrived, or in what order relative to other inserts. The file name is the only
        // metadata, and it says nothing. That is the gap step 2's commit log fills.
        String fileName = String.format("part-%s.parquet", UUID.randomUUID());
        Path dataFilePath = Paths.get(tablePath, "data", fileName);

        ParquetUtil.writeRecords(records, dataFilePath);

        return records.size();
    }

    /**
     * Reads all records from the Delta table.
     *
     * @return a list of records, where each record is a map of column names to values
     * @throws IOException if an I/O error occurs
     */
    public List<Map<String, String>> readAll() throws IOException {
        List<Map<String, String>> allRecords = new ArrayList<>();
        Path dataPath = Paths.get(tablePath, "data");
        if (!Files.exists(dataPath)) {
            return allRecords;
        }

        // The whole of "what is in this table" at step 1: list the directory and read
        // whatever happens to be there. There is no manifest saying which files belong to
        // the table, so a listing is the only source of truth — and a listing has no
        // notion of a point in time. Step 2 replaces this with a commit log.
        try (var entries = Files.list(dataPath)) {
            List<Path> dataFiles = entries
                    .filter(f -> f.toString().endsWith(".parquet"))
                    .sorted()
                    .toList();
            for (Path dataFile : dataFiles) {
                allRecords.addAll(ParquetUtil.readRecords(dataFile));
            }
        }

        return allRecords;
    }

    /** How many files currently back this table. Demos print it; nothing else needs it. */
    public int dataFileCount() throws IOException {
        Path dataPath = Paths.get(tablePath, "data");
        if (!Files.exists(dataPath)) {
            return 0;
        }
        try (var entries = Files.list(dataPath)) {
            return (int) entries.filter(f -> f.toString().endsWith(".parquet")).count();
        }
    }
} 