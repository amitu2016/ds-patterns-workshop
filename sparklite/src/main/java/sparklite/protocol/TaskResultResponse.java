package sparklite.protocol;

/**
 * Task completion message sent from {@code SparkLiteWorker} back to {@code SparkLiteDriver}.
 *
 * @param taskId           Unique task ID
 * @param stageId          Owning stage ID
 * @param partitionId      Partition index
 * @param success          Whether the task finished successfully
 * @param serializedResult Serialized task computation result
 * @param error            Error message if task failed
 */
public record TaskResultResponse(
        int taskId,
        int stageId,
        int partitionId,
        boolean success,
        byte[] serializedResult,
        String error
) {
}
