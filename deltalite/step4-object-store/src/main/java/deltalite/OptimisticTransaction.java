package deltalite;

import com.tickloom.future.TickCompletableFuture;
import deltalite.actions.Action;
import deltalite.actions.CommitInfo;

import java.time.Instant;
import java.util.*;

/**
 * Implements Optimistic Concurrency Control (OCC) for transactions against a Delta table
 * hosted on an object store.
 *
 * <p>Writers snapshot the table at version $N$. When committing:
 * 1. Checks if the table was modified past $N$. If so, throws {@link ConcurrentModificationException}.
 * 2. Attempts to atomically publish version $N + 1$ via {@code DeltaLog.write(N + 1, actions)},
 *    which translates to a conditional PUT ({@code If-None-Match: *}) in the object store.
 * 3. If two concurrent transactions race to write version $N + 1$, the object store accepts
 *    exactly one, and the other receives a PreconditionFailed / ConcurrentModificationException,
 *    prompting a retry at the new snapshot.
 */
public class OptimisticTransaction {

    private final DeltaLog deltaLog;
    private final Snapshot snapshot;
    private final long readVersion;
    private final List<Action> actions = new ArrayList<>();

    public OptimisticTransaction(DeltaLog deltaLog, Snapshot snapshot) {
        this.deltaLog = Objects.requireNonNull(deltaLog, "deltaLog");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
        this.readVersion = snapshot.getVersion();
    }

    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public long getReadVersion() {
        return readVersion;
    }

    public void addAction(Action action) {
        actions.add(action);
    }

    public List<Action> getActions() {
        return new ArrayList<>(actions);
    }

    public TickCompletableFuture<Void> commit() {
        return commit("TRANSACTION");
    }

    public TickCompletableFuture<Void> commit(String operation) {
        return deltaLog.getLatestVersion().thenCompose(latestVersion -> {
            if (latestVersion > readVersion) {
                TickCompletableFuture<Void> failed = new TickCompletableFuture<>();
                failed.fail(new ConcurrentModificationException(
                        String.format("Conflict detected: table modified to version %d past transaction read version %d",
                                latestVersion, readVersion)));
                return failed;
            }

            CommitInfo commitInfo = CommitInfo.create(operation)
                    .withParameter("startVersion", String.valueOf(readVersion))
                    .withParameter("commitTime", String.valueOf(Instant.now().toEpochMilli()));
            actions.add(commitInfo);

            long nextVersion = readVersion + 1;
            return deltaLog.write(nextVersion, actions);
        });
    }
}
