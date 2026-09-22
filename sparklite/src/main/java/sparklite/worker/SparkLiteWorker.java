package sparklite.worker;

import com.tickloom.Process;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparklite.protocol.RegisterWorkerRequest;
import sparklite.protocol.RegisterWorkerResponse;
import sparklite.protocol.SparkLiteProtocol;
import sparklite.protocol.SubmitTaskRequest;
import sparklite.protocol.TaskResultResponse;
import sparklite.rdd.Partition;
import sparklite.rdd.RDDLite;
import sparklite.scheduler.RDDLiteTask;
import sparklite.scheduler.Task;
import sparklite.util.SerializerUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Worker node process in sparklite, corresponding to Spark's Executor / Worker.
 *
 * <p>Runs as a tickloom {@link Process} on the shared {@link com.tickloom.messaging.MessageBus}.
 * Upon boot, it registers itself with {@code SparkLiteDriver} via {@link SparkLiteProtocol#REGISTER_WORKER}.
 * It executes assigned {@link Task} units over partitions and replies with {@link SparkLiteProtocol#TASK_RESULT}.
 */
public class SparkLiteWorker extends Process {
    private static final Logger logger = LoggerFactory.getLogger(SparkLiteWorker.class);


    private final ProcessId driverId;
    private final String hostNodeId;
    private final int cores;

    private boolean isRegistered = false;

    /**
     * Clients this worker can hand to a task. Attached after construction, because
     * {@code Cluster.newClient} needs a cluster that is already built and the worker is created
     * inside {@code build()}'s factory.
     */
    private ClientRegistry clients = new ClientRegistry();

    public SparkLiteWorker(ProcessParams processParams,
                           ProcessId driverId,
                           String hostNodeId,
                           int cores) {
        super(processParams);
        this.driverId = driverId;
        this.hostNodeId = hostNodeId;
        this.cores = cores;
    }

    @Override
    protected TickCompletableFuture<?> onInit() {
        return TickCompletableFuture.completed(null);
    }

    /** Installs the clients tasks running here may use. See {@link ClientRegistry}. */
    public void attach(ClientRegistry clients) {
        this.clients = java.util.Objects.requireNonNull(clients, "clients");
    }

    /**
     * Gives every RDD in the lineage the client it declared.
     *
     * <p>Walks dependencies rather than touching only the task's RDD: the task holds the <i>top</i>
     * of the chain, so for {@code parquetRDD.filter(...).map(...)} the client belongs three levels
     * down and setting it on the {@code MapRDDLite} alone would silently do nothing.
     */
    private void injectClients(RDDLite<?> rdd) {
        Class<?> type = rdd.clientType();
        if (type != null) {
            rdd.setClient(clients.client(type));
        }
        for (RDDLite<?> parent : rdd.getDependencies()) {
            injectClients(parent);
        }
    }

    public String getHostNodeId() {
        return hostNodeId;
    }

    public int getCores() {
        return cores;
    }

    public boolean isRegistered() {
        return isRegistered;
    }

    @Override
    protected void onTick() {

    }

    @Override
    public TickCompletableFuture onStart() {
        sendRegistration(); //send registration when started.
        return TickCompletableFuture.completed(null);
    }

    public void sendRegistration() {
        try {
            RegisterWorkerRequest request = new RegisterWorkerRequest(id.name(), cores, hostNodeId);
            String correlationId = idGen.generateCorrelationId("reg");
            Message regMsg = createMessage(driverId, correlationId, request, SparkLiteProtocol.REGISTER_WORKER);
            messageBus.sendMessage(regMsg);
            logger.debug("Worker '{}' sent registration to driver '{}'", id, driverId);
        } catch (IOException e) {
            logger.error("Failed to send worker registration to driver", e);
        }
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        Map<MessageType, Handler> handlers = new HashMap<>();
        handlers.put(SparkLiteProtocol.SUBMIT_TASK, this::handleSubmitTask);
        handlers.put(SparkLiteProtocol.REGISTER_WORKER_RESPONSE, this::handleRegisterResponse);
        return handlers;
    }

    private void handleRegisterResponse(Message message) {
        RegisterWorkerResponse response = deserializePayload(message.payload(), RegisterWorkerResponse.class);
        if (response.success()) {
            this.isRegistered = true;
            logger.info("Worker '{}' successfully registered with driver '{}'", id, driverId);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleSubmitTask(Message message) {
        SubmitTaskRequest request = deserializePayload(message.payload(), SubmitTaskRequest.class);
        logger.info("Worker '{}' received task {} (stage {}, partition {}, locality {})",
                id, request.taskId(), request.stageId(), request.partitionId(), request.locality());

        Task<Object> task;
        Partition partition;
        try {
            task = (Task<Object>) SerializerUtil.deserialize(request.serializedTask());
            partition = SerializerUtil.deserialize(request.serializedPartition());
        } catch (Exception e) {
            logger.error("Failed to deserialize task or partition for task {}", request.taskId(), e);
            sendTaskResult(request, false, null, "Deserialization error: " + e.getMessage());
            return;
        }

        // Cast through Object: RDDLiteTask<T> extends Task<List<T>>, so javac rejects a direct
        // instanceof against the Task<Object> declared above as provably distinct.
        if (((Object) task) instanceof RDDLiteTask<?> rddTask) {
            try {
                injectClients(rddTask.getRdd());
            } catch (RuntimeException e) {
                logger.error("Failed to inject clients for task {} on worker '{}'", request.taskId(), id, e);
                sendTaskResult(request, false, null, e.getMessage());
                return;
            }
        }

        try {
            task.execute(partition).whenComplete((result, error) -> {
                try {
                    if (error != null) {
                        logger.error("Task {} execution failed on worker '{}'", request.taskId(), id, error);
                        sendTaskResult(request, false, null, error.getMessage());
                    } else {
                        logger.info("Task {} executed successfully on worker '{}'", request.taskId(), id);
                        byte[] serializedResult = SerializerUtil.serialize(result);
                        sendTaskResult(request, true, serializedResult, null);
                    }
                } catch (Exception e) {
                    logger.error("Failed to serialize or send task result for task {}", request.taskId(), e);
                    sendTaskResult(request, false, null, e.getMessage());
                }
            });
        } catch (Exception e) {
            logger.error("Synchronous failure executing task {}", request.taskId(), e);
            sendTaskResult(request, false, null, e.getMessage());
        }
    }

    private void sendTaskResult(SubmitTaskRequest request, boolean success, byte[] serializedResult, String error) {
        TaskResultResponse resultResponse = new TaskResultResponse(
                request.taskId(),
                request.stageId(),
                request.partitionId(),
                success,
                serializedResult,
                error
        );

        String correlationId = idGen.generateCorrelationId("res-" + request.taskId());
        Message resultMsg = createMessage(driverId, correlationId, resultResponse, SparkLiteProtocol.TASK_RESULT);
        try {
            messageBus.sendMessage(resultMsg);
        } catch (IOException e) {
            logger.error("Worker '{}' failed to send task result message to driver '{}'", id, driverId, e);
        }
    }
}
