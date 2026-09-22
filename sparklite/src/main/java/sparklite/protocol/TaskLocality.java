package sparklite.protocol;

/**
 * Scheduling locality levels, corresponding to {@code org.apache.spark.scheduler.TaskLocality}.
 */
public enum TaskLocality {
    /** Task is scheduled within the same JVM process where data is cached */
    PROCESS_LOCAL,

    /** Task is scheduled on a worker running on the same physical or simulated storage node */
    NODE_LOCAL,

    /** Task has no locality preference */
    NO_PREF,

    /** Task is scheduled on any available worker (remote) */
    ANY
}
