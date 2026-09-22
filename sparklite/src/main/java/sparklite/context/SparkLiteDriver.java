package sparklite.context;

import com.tickloom.Process;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparklite.protocol.RegisterWorkerRequest;
import sparklite.protocol.RegisterWorkerResponse;
import sparklite.protocol.SparkLiteProtocol;
import sparklite.protocol.SubmitTaskRequest;
import sparklite.protocol.TaskResultResponse;
import sparklite.scheduler.DAGScheduler;
import sparklite.scheduler.TaskScheduler;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Driver node process in sparklite, corresponding to Spark's Driver.
 *
 * <p>Houses the {@link DAGScheduler} and {@link TaskScheduler}. Receives worker
 * registrations and task results over the tickloom {@link com.tickloom.messaging.MessageBus}.
 */
public class SparkLiteDriver extends Process {
    private static final Logger logger = LoggerFactory.getLogger(SparkLiteDriver.class);

    private final TaskScheduler taskScheduler;
    private final DAGScheduler dagScheduler;

    public SparkLiteDriver(ProcessParams processParams) {
        super(processParams);
        this.taskScheduler = new TaskScheduler(this::sendSubmitTask);
        this.dagScheduler = new DAGScheduler(this.taskScheduler);
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public DAGScheduler getDagScheduler() {
        return dagScheduler;
    }

    public <T> com.tickloom.future.TickCompletableFuture<List<T>> runJob(sparklite.rdd.RDDLite<T> rdd) {
        return dagScheduler.submitJob(rdd);
    }

    private void sendSubmitTask(String workerId, SubmitTaskRequest request) {
        try {
            String correlationId = idGen.generateCorrelationId("task-" + request.taskId());
            Message message = createMessage(ProcessId.of(workerId), correlationId, request, SparkLiteProtocol.SUBMIT_TASK);
            messageBus.sendMessage(message);
        } catch (IOException e) {
            logger.error("Driver '{}' failed to send task {} to worker '{}'", id, request.taskId(), workerId, e);
        }
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        Map<MessageType, Handler> handlers = new HashMap<>();
        handlers.put(SparkLiteProtocol.REGISTER_WORKER, this::handleRegisterWorker);
        handlers.put(SparkLiteProtocol.TASK_RESULT, this::handleTaskResult);
        return handlers;
    }

    private void handleRegisterWorker(Message message) {
        RegisterWorkerRequest request = deserializePayload(message.payload(), RegisterWorkerRequest.class);
        logger.info("Driver '{}' received registration from worker '{}' on host '{}'",
                id, request.workerId(), request.hostNodeId());

        taskScheduler.handleWorkerRegistration(request);

        RegisterWorkerResponse response = new RegisterWorkerResponse(request.workerId(), true);
        Message responseMsg = createResponseMessage(message, response, SparkLiteProtocol.REGISTER_WORKER_RESPONSE);
        try {
            messageBus.sendMessage(responseMsg);
        } catch (IOException e) {
            logger.error("Driver failed to send registration response to worker '{}'", request.workerId(), e);
        }
    }

    private void handleTaskResult(Message message) {
        TaskResultResponse response = deserializePayload(message.payload(), TaskResultResponse.class);
        logger.info("Driver '{}' received result for task {} (stage {}, partition {}, success={})",
                id, response.taskId(), response.stageId(), response.partitionId(), response.success());
        taskScheduler.handleTaskResult(response);
    }

    @Override
    protected void onTick() {
        taskScheduler.tick();
    }
}
