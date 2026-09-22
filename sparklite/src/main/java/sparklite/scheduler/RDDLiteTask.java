package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import sparklite.rdd.Partition;
import sparklite.rdd.RDDLite;

import java.util.ArrayList;
import java.util.List;

/**
 * A task that computes an {@link RDDLite} on a target partition and returns all computed
 * records from that partition as a {@code List<T>}.
 * Corresponds to {@code org.apache.spark.scheduler.ResultTask}.
 *
 * @param <T> Element type of the RDD
 */
public class RDDLiteTask<T> extends Task<List<T>> {
    private final RDDLite<T> rdd;

    public RDDLiteTask(int taskId, int stageId, int partitionId, RDDLite<T> rdd) {
        super(taskId, stageId, partitionId);
        this.rdd = rdd;
    }

    public RDDLite<T> getRdd() {
        return rdd;
    }

    @Override
    public TickCompletableFuture<List<T>> execute(Partition partition) {
        return rdd.compute(partition).thenApply(iterator -> {
            List<T> results = new ArrayList<>();
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
            return results;
        });
    }

    @Override
    public String toString() {
        return String.format("RDDLiteTask(id=%d, stage=%d, partition=%d)",
                getTaskId(), getStageId(), getPartitionId());
    }
}
