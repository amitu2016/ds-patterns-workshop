package sparklite.protocol;

/**
 * Worker registration request sent from {@code SparkLiteWorker} to {@code SparkLiteDriver}.
 *
 * @param workerId   Unique worker process identifier
 * @param cores      Number of execution cores/slots
 * @param hostNodeId Host / node identifier for locality matching
 */
public record RegisterWorkerRequest(String workerId, int cores, String hostNodeId) {
}
