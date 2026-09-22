package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparklite.rdd.RDDLite;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * High-level scheduler that computes execution stages from RDD lineage
 * and submits tasks to the {@link TaskScheduler}.
 * Corresponds to {@code org.apache.spark.scheduler.DAGScheduler}.
 */
public class DAGScheduler {
    private static final Logger logger = LoggerFactory.getLogger(DAGScheduler.class);

    private final TaskScheduler taskScheduler;
    private int nextJobId = 0;
    private int nextStageId = 0;
    private int nextTaskId = 0;

    public DAGScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = Objects.requireNonNull(taskScheduler, "taskScheduler");
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    /**
     * Submits an RDD computation as a Job, returning a future containing
     * all collected results from all partitions.
     */
    public <T> TickCompletableFuture<List<T>> submitJob(RDDLite<T> rdd) {
        int jobId = nextJobId++;
        int stageId = nextStageId++;

        int numPartitions = rdd.getPartitions().length;

        logger.info("Submitting Job {} (Stage {}) with {} partitions", jobId, stageId, numPartitions);

        ResultStage stage = new ResultStage(stageId, rdd, numPartitions, jobId);
        List<RDDLiteTask<T>> tasks = createTasksFor(numPartitions, rdd, stage);

        return taskScheduler.submitTasks(tasks, rdd).thenApply(results -> {
            stage.setComplete(true);
            logger.info("Job {} completed: collected {} total records", jobId, results.size());
            return results;
        });
    }

    private <T> List<RDDLiteTask<T>> createTasksFor(int numPartitions, RDDLite<T> rdd, ResultStage stage) {
        List<RDDLiteTask<T>> tasks = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++) {
            RDDLiteTask<T> task = new RDDLiteTask<>(nextTaskId++, stage.getStageId(), i, rdd);
            stage.addTask(task);
            tasks.add(task);
        }
        return tasks;
    }
}

