package sparklite.protocol;

import com.tickloom.messaging.MessageType;

public final class SparkLiteProtocol {
    private SparkLiteProtocol() {}

    public static final MessageType REGISTER_WORKER = MessageType.of("RegisterWorker");
    public static final MessageType REGISTER_WORKER_RESPONSE = MessageType.of("RegisterWorkerResponse");
    public static final MessageType SUBMIT_TASK = MessageType.of("SubmitTask");
    public static final MessageType TASK_RESULT = MessageType.of("TaskResult");
}
