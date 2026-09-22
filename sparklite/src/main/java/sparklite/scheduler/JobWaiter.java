package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Collects one partition's records per task and completes a job's future when all have arrived.
 * Corresponds to {@code org.apache.spark.scheduler.JobWaiter}.
 *
 * <p><b>Order comes from the index, never from arrival.</b> Tasks finish in whatever order workers
 * and the network produce, and for most jobs that order differs run to run. Appending results as
 * they land would silently shuffle the output of an ordered source — a Kafka partition read in
 * offset order, or Parquet row groups in file order — so results go into a slot and the slots are
 * concatenated at the end.
 *
 * <p>Fail-fast: the first failure completes the job exceptionally and later results are ignored. A
 * partial result is not a smaller answer, it is a wrong one.
 *
 * @param <T> element type of the RDD being computed
 */
public final class JobWaiter<T> {

    private final TickCompletableFuture<List<T>> completionFuture = new TickCompletableFuture<>();
    private final List<List<T>> partitionResults;
    private final int totalTasks;
    private int finishedTasks = 0;

    public JobWaiter(int totalTasks) {
        if (totalTasks < 0) {
            throw new IllegalArgumentException("totalTasks must not be negative: " + totalTasks);
        }
        this.totalTasks = totalTasks;
        this.partitionResults = new ArrayList<>(Collections.nCopies(totalTasks, null));
        if (totalTasks == 0) {
            completionFuture.complete(List.of());
        }
    }

    public TickCompletableFuture<List<T>> completionFuture() {
        return completionFuture;
    }

    /** Records one partition's output. The job completes once every partition has reported. */
    public void taskSucceeded(int partitionIndex, List<T> records) {
        Objects.requireNonNull(records, "records");
        if (!completionFuture.isPending()) {
            return;                      // already failed, or already complete
        }
        if (partitionIndex < 0 || partitionIndex >= totalTasks) {
            jobFailed(new IndexOutOfBoundsException(
                    "partition " + partitionIndex + " is outside a job of " + totalTasks + " task(s)"));
            return;
        }
        if (partitionResults.get(partitionIndex) != null) {
            jobFailed(new IllegalStateException("partition " + partitionIndex + " reported twice"));
            return;
        }

        partitionResults.set(partitionIndex, records);
        finishedTasks++;
        if (finishedTasks == totalTasks) {
            List<T> merged = new ArrayList<>();
            for (List<T> partition : partitionResults) {
                merged.addAll(partition);    // every slot is filled once finishedTasks == totalTasks
            }

            completionFuture.complete(merged);
        }
    }

    public void jobFailed(Throwable error) {
        if (completionFuture.isPending()) {
            completionFuture.fail(error);
        }
    }
}
