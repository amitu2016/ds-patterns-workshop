package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import sparklite.rdd.Partition;

import java.io.Serializable;

/**
 * Abstract base class for tasks executed on workers.
 * A task represents a unit of work that operates on a specific partition of an RDDLite.
 * Corresponds to {@code org.apache.spark.scheduler.Task}.
 *
 * @param <T> The output type the task produces
 */
public abstract class Task<T> implements Serializable {
    private final int taskId;
    private final int stageId;
    private final int partitionId;

    protected Task(int taskId, int stageId, int partitionId) {
        this.taskId = taskId;
        this.stageId = stageId;
        this.partitionId = partitionId;
    }

    /**
     * Executes the task on the given partition.
     *
     * @param partition The partition to execute on
     * @return TickCompletableFuture containing the computation result
     */
    public abstract TickCompletableFuture<T> execute(Partition partition);

    public int getTaskId() {
        return taskId;
    }

    public int getStageId() {
        return stageId;
    }

    public int getPartitionId() {
        return partitionId;
    }
}
