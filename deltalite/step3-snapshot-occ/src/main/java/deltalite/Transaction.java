package deltalite;

import deltalite.actions.Action;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a transaction for performing operations on a Delta table.
 * A transaction maintains a list of actions and commits them atomically.
 */
public class Transaction {
    protected final Path tablePath;
    protected final List<Action> actions;
    protected final String appId;
    protected final AtomicBoolean committed;
    
    /**
     * Creates a new transaction for the given table path.
     *
     * @param tablePath the path to the Delta table
     */
    public Transaction(String tablePath) {
        this.tablePath = Paths.get(tablePath);
        this.actions = new ArrayList<>();
        this.appId = UUID.randomUUID().toString();
        this.committed = new AtomicBoolean(false);
        
        // Create directories if they don't exist
        try {
            Files.createDirectories(this.tablePath);
            Files.createDirectories(getLogPath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directories for transaction", e);
        }
    }
    
    /**
     * Gets the path to the _delta_log directory.
     *
     * @return the log directory path
     */
    public Path getLogPath() {
        return tablePath.resolve("_delta_log");
    }
    
    /**
     * Adds an action to the transaction.
     *
     * @param action the action to add
     * @return this transaction for chaining
     */
    public Transaction addAction(Action action) {
        if (committed.get()) {
            throw new IllegalStateException("Cannot add actions to a committed transaction");
        }
        actions.add(action);
        return this;
    }
    
    /**
     * Gets all actions in this transaction.
     *
     * @return the list of actions
     */
    public List<Action> getActions() {
        return new ArrayList<>(actions);
    }
    
    /**
     * Commits the transaction by writing all actions to a new JSON log file.
     *
     * @return this transaction
     * @throws IOException if there is an error writing the log file
     */
    public Transaction commit() throws IOException {
        if (committed.getAndSet(true)) {
            throw new IllegalStateException("Transaction already committed");
        }
        
        // Get or create a DeltaLog for this table
        DeltaLog deltaLog = DeltaLog.forTable(tablePath.toString());
        
        // Determine the next version number
        long version = deltaLog.getLatestVersion() + 1;
        
        // Write actions to the log
        deltaLog.write(version, actions);
        
        return this;
    }
    
    /**
     * Reads all actions from the transaction log.
     *
     * @return the list of all actions from all log files
     * @throws IOException if there is an error reading the log files
     */
    public List<Action> readTransactionLog() throws IOException {
        // Get the DeltaLog for this table
        DeltaLog deltaLog = DeltaLog.forTable(tablePath.toString());
        
        // Update to the latest snapshot
        Snapshot snapshot = deltaLog.update();
        
        // Get all versions
        List<Long> versions = deltaLog.listVersions();
        
        // Read all actions from all versions
        List<Action> allActions = new ArrayList<>();
        for (Long version : versions) {
            allActions.addAll(deltaLog.readVersion(version));
        }
        
        return allActions;
    }
} 