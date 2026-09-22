package sparklite.protocol;

/**
 * Task execution request sent from {@code SparkLiteDriver} to a target {@code SparkLiteWorker}.
 *
 * @param taskId              Globally unique task ID
 * @param stageId             Owning stage ID
 * @param partitionId         Target partition index
 * @param locality            Locality level decided during scheduling
 * @param serializedTask      Serialized Task instance
 * @param serializedPartition Serialized Partition instance
 */
public record SubmitTaskRequest(
        int taskId,
        int stageId,
        int partitionId,
        TaskLocality locality,
        byte[] serializedTask,
        byte[] serializedPartition
) {
}
