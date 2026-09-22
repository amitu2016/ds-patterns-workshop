package sparklite.protocol;

/**
 * Worker registration response sent from {@code SparkLiteDriver} back to {@code SparkLiteWorker}.
 *
 * @param workerId Unique worker identifier
 * @param success  Whether registration was accepted
 */
public record RegisterWorkerResponse(String workerId, boolean success) {
}
