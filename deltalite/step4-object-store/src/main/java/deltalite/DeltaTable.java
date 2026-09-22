package deltalite;

import com.tickloom.future.TickCompletableFuture;
import deltalite.actions.Action;
import deltalite.actions.AddFile;
import deltalite.actions.Metadata;
import deltalite.actions.Protocol;
import deltalite.store.Storage;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.io.PrefetchedInputFile;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.io.IOException;
import java.util.*;

/**
 * DeltaTable represents a Delta Lake table hosted on an object store.
 *
 * <p>Data files are stored as columnar Apache Parquet files under {@code data/},
 * and transactions are committed as JSON actions to {@code _delta_log/}.
 *
 * <p>All operations return {@link TickCompletableFuture} to maintain non-blocking
 * event-driven execution on tickloom.
 */
public class DeltaTable {

    private final String tablePath;
    private final Storage storage;
    private final DeltaLog deltaLog;

    public DeltaTable(String tablePath, Storage storage) {
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath");
        this.storage = Objects.requireNonNull(storage, "storage");
        this.deltaLog = DeltaLog.forTable(tablePath, storage);
    }

    /**
     * Factory method that creates or loads a DeltaTable, writing version 0 if the table
     * does not yet exist.
     */
    /**
     * The column whose value decides which directory a data file lands in, or null for an
     * unpartitioned table. Mirrors {@code Metadata.partitionColumns}, which real Delta carries in
     * the log; here one column is enough to show the mechanism.
     */
    private String partitionColumn;

    /**
     * Partitions subsequent writes by {@code column}, Hive-style:
     * {@code data/<column>=<value>/part-<uuid>.parquet}.
     *
     * <p>Two things follow, and the second is the reason partitioning exists. Each file holds rows
     * for exactly one value, so a reader filtering on that column can skip whole files without
     * opening them — cheaper than the row-group statistics parquetlite uses, because the value is
     * in the path. And the value is recorded in the commit as {@code AddFile.partitionValues}, so
     * the skip is decidable from the transaction log alone, without listing storage.
     *
     * <p>Choose a column that only moves forward. See {@code docs/CAPSTONE-IMPLEMENTATION.md}:
     * partitioning a ledger by {@code posting_date} keeps writes landing in today's directory,
     * whereas {@code effective_date} would let a backdated entry rewrite a months-old partition.
     */
    public DeltaTable partitionedBy(String column) {
        this.partitionColumn = column;
        return this;
    }

    public String partitionColumn() {
        return partitionColumn;
    }

    public static TickCompletableFuture<DeltaTable> forTable(String tablePath, Storage storage) {
        DeltaTable table = new DeltaTable(tablePath, storage);
        return table.deltaLog.tableExists().thenCompose(exists -> {
            if (exists) {
                return table.deltaLog.update().thenApply(s -> table);
            }
            // Initialize version 0: protocol + metadata
            List<Action> initActions = List.of(
                    new Protocol(),
                    new Metadata(UUID.randomUUID().toString(), "deltalite-table", "parquet")
            );
            return table.deltaLog.write(0, initActions).thenApply(v -> table);
        });
    }

    public String getTablePath() {
        return tablePath;
    }

    public Storage getStorage() {
        return storage;
    }

    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    /**
     * Starts a new optimistic transaction at the current snapshot.
     */
    public TickCompletableFuture<OptimisticTransaction> startTransaction() {
        return deltaLog.update().thenApply(snapshot -> new OptimisticTransaction(deltaLog, snapshot));
    }

    /**
     * Writes records into a new Parquet data file in object storage and returns the resulting
     * {@link AddFile} action.
     */
    public TickCompletableFuture<List<Action>> insert(TableSchema schema, List<TableRecord> records) {
        try {
            if (partitionColumn == null) {
                return writeOneFile(schema, records, Map.of());
            }
            // One file per distinct value, so the value can be read off the path and off the log.
            // LinkedHashMap keeps the write order deterministic, which keeps the demo reproducible.
            Map<String, List<TableRecord>> byValue = new LinkedHashMap<>();
            for (TableRecord record : records) {
                Object value = record.get(partitionColumn);
                if (value == null) {
                    throw new IOException("record has no value for partition column '"
                            + partitionColumn + "': " + record.primaryKey());
                }
                byValue.computeIfAbsent(String.valueOf(value), v -> new ArrayList<>()).add(record);
            }

            List<Action> actions = new ArrayList<>();
            TickCompletableFuture<List<Action>> result = TickCompletableFuture.completed(actions);
            for (Map.Entry<String, List<TableRecord>> entry : byValue.entrySet()) {
                result = result.thenCompose(soFar ->
                        writeOneFile(schema, entry.getValue(), Map.of(partitionColumn, entry.getKey()))
                                .thenApply(written -> {
                                    soFar.addAll(written);
                                    return soFar;
                                }));
            }
            return result;
        } catch (IOException e) {
            TickCompletableFuture<List<Action>> failed = new TickCompletableFuture<>();
            failed.fail(e);
            return failed;
        }
    }

    /**
     * Convenience method to write records and commit them in a single transaction.
     */
    public TickCompletableFuture<Void> insertAndCommit(TableSchema schema, List<TableRecord> records) {
        return startTransaction().thenCompose(txn ->
                insert(schema, records).thenCompose(actions -> {
                    actions.forEach(txn::addAction);
                    return txn.commit("WRITE");
                }));
    }

    /** Writes one Parquet object and returns the {@code AddFile} that makes it visible on commit. */
    private TickCompletableFuture<List<Action>> writeOneFile(TableSchema schema,
                                                             List<TableRecord> records,
                                                             Map<String, String> partitionValues) {
        byte[] parquetBytes;
        try {
            parquetBytes = ParquetWriterHelper.writeToBytes(
                    schema, records, ParquetWriterHelper.DEFAULT_ROW_GROUP_SIZE);
        } catch (IOException e) {
            TickCompletableFuture<List<Action>> failed = new TickCompletableFuture<>();
            failed.fail(e);
            return failed;
        }
        String prefix = tablePath.isEmpty() ? "" : (tablePath.endsWith("/") ? tablePath : tablePath + "/");
        String directory = partitionValues.isEmpty()
                ? "data/"
                : "data/" + partitionValues.entrySet().iterator().next().getKey()
                          + "=" + partitionValues.values().iterator().next() + "/";
        String dataFile = prefix + directory + "part-" + UUID.randomUUID() + ".parquet";

        return storage.writeObject(dataFile, parquetBytes).thenApply(v -> {
            AddFile add = new AddFile(dataFile, new HashMap<>(partitionValues), parquetBytes.length,
                    System.currentTimeMillis(), true, new HashMap<>(), "");
            return List.<Action>of(add);
        });
    }

    /**
     * Reads all records from the current active snapshot.
     */
    public TickCompletableFuture<List<TableRecord>> readAll() {
        return deltaLog.update().thenCompose(snapshot -> readFiles(snapshot.getAllFiles()));
    }

    /**
     * Time-travel query: reads all records as they existed at a historical commit version.
     */
    public TickCompletableFuture<List<TableRecord>> readAtVersion(long version) {
        return deltaLog.getSnapshotAt(version).thenCompose(snapshot -> readFiles(snapshot.getAllFiles()));
    }

    /**
     * Reads records across a collection of active data files.
     */
    public TickCompletableFuture<List<TableRecord>> readFiles(List<AddFile> files) {
        TickCompletableFuture<List<TableRecord>> resultFuture = new TickCompletableFuture<>();
        List<TableRecord> records = new ArrayList<>();
        readNextFile(files.iterator(), records, resultFuture);
        return resultFuture;
    }

    private void readNextFile(Iterator<AddFile> iterator, List<TableRecord> accumulated, TickCompletableFuture<List<TableRecord>> resultFuture) {
        if (!iterator.hasNext()) {
            resultFuture.complete(accumulated);
            return;
        }
        AddFile addFile = iterator.next();
        storage.readObject(addFile.getPath()).whenComplete((bytes, err) -> {
            if (err != null) {
                resultFuture.fail(err);
            } else {
                try {
                    List<TableRecord> fileRecords = ObjectStoreParquetReader.readAllRecords(
                            PrefetchedInputFile.wholeObject(bytes));
                    accumulated.addAll(fileRecords);
                    readNextFile(iterator, accumulated, resultFuture);
                } catch (Exception e) {
                    resultFuture.fail(e);
                }
            }
        });
    }
}
