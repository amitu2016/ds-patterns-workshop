package sparklite.scheduler;

import sparklite.rdd.RDDLite;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a stage in the execution DAG.
 * Corresponds to {@code org.apache.spark.scheduler.Stage}.
 */
public class Stage {
    private final int stageId;
    private final RDDLite<?> rdd;
    private final int numPartitions;
    private final List<Task<?>> tasks = new ArrayList<>();
    private boolean isComplete = false;

    public Stage(int stageId, RDDLite<?> rdd, int numPartitions) {
        this.stageId = stageId;
        this.rdd = rdd;
        this.numPartitions = numPartitions;
    }

    public int getStageId() {
        return stageId;
    }

    public RDDLite<?> getRdd() {
        return rdd;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public List<Task<?>> getTasks() {
        return tasks;
    }

    public void addTask(Task<?> task) {
        this.tasks.add(task);
    }

    public boolean isComplete() {
        return isComplete;
    }

    public void setComplete(boolean complete) {
        this.isComplete = complete;
    }
}
