package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import sparklite.rdd.Partition;
import sparklite.rdd.RDDLite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Runs every partition of an RDD in the calling process, with no workers and no serialization.
 * Corresponds to Spark's local mode ({@code LocalSchedulerBackend}), which a {@code SparkContext}
 * created with {@code master = "local[*]"} uses.
 *
 * <p>Partitions fan out and {@link JobWaiter} puts their results back in partition order. What
 * local mode removes is the cluster, not the fan-out — though with an in-memory RDD you cannot tell:
 * {@code ParallelCollectionRDDLite.compute} completes its future before returning, so partition
 * {@code i} finishes before partition {@code i+1} starts and arrival order always equals partition
 * order. Real Spark's {@code compute} returns an {@code Iterator} synchronously for every RDD;
 * sparklite's returns a future only because the storage-backed RDDs have no thread to block on.
 *
 * <p><b>This does not tick, and does not need to: the caller drives completion.</b> {@code runJob}
 * hands back a future the instant the fan-out is issued. With an in-memory RDD that future is
 * already complete. With one whose partitions read over the network it is pending, and the caller
 * resolves it the way everything else in this repo does:
 *
 * <pre>{@code
 * rdd.setClient(storageClient);                          // no worker here to inject it
 * var records = cluster.tickUntilComplete(LocalScheduler.runJob(rdd));
 * }</pre>
 *
 * <p>Verified against {@code ParquetRDDLite} over a live objectstorelite cluster: the job is
 * pending before the first tick and yields every record after. So the constraint is not synchrony —
 * it is that <b>something must supply the client</b>, because {@link sparklite.worker.SparkLiteWorker}
 * is what normally injects it and there is no worker in local mode. Supply it yourself and an
 * asynchronous RDD runs here perfectly well.
 */
public final class LocalScheduler {

    private LocalScheduler() {
    }

    public static <T> TickCompletableFuture<List<T>> runJob(RDDLite<T> rdd) {
        Partition[] partitions = rdd.getPartitions();
        JobWaiter<T> waiter = new JobWaiter<>(partitions.length);

        for (int i = 0; i < partitions.length; i++) {
            final int partitionIndex = i;
            rdd.compute(partitions[i]).whenComplete((records, error) -> {
                if (error != null) {
                    waiter.jobFailed(error);
                    return;
                }
                waiter.taskSucceeded(partitionIndex, drain(records));
            });
        }
        return waiter.completionFuture();
    }

    private static <T> List<T> drain(Iterator<T> records) {
        List<T> collected = new ArrayList<>();
        while (records.hasNext()) {
            collected.add(records.next());
        }
        return collected;
    }
}
