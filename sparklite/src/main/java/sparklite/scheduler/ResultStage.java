package sparklite.scheduler;

import sparklite.rdd.RDDLite;

/**
 * A stage that executes an action and produces results back to the driver.
 * Corresponds to {@code org.apache.spark.scheduler.ResultStage}.
 */
public class ResultStage extends Stage {
    private final Object jobId;

    public ResultStage(int stageId, RDDLite<?> rdd, int numPartitions, Object jobId) {
        super(stageId, rdd, numPartitions);
        this.jobId = jobId;
    }

    public Object getJobId() {
        return jobId;
    }
}
