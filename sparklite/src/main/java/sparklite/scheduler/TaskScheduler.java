package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.RequestWaitingList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sparklite.protocol.RegisterWorkerRequest;
import sparklite.protocol.SubmitTaskRequest;
import sparklite.protocol.TaskLocality;
import sparklite.protocol.TaskResultResponse;
import sparklite.rdd.Partition;
import sparklite.rdd.RDDLite;
import sparklite.util.SerializerUtil;

import java.util.*;

/**
 * Low-level task scheduler responsible for worker tracking, locality-aware
 * task placement, and task lifecycle management.
 *
 * <p>Matches tasks to workers by preferred location ({@link TaskLocality#NODE_LOCAL})
 * when possible, falling back to {@link TaskLocality#ANY} when no local worker has capacity.
 *
 * <p>Corresponds to {@code org.apache.spark.scheduler.TaskScheduler}.
 */
public class TaskScheduler {
    private static final Logger logger = LoggerFactory.getLogger(TaskScheduler.class);

    @FunctionalInterface
    public interface TaskSender {
        void send(String workerId, SubmitTaskRequest request);
    }

    public static class WorkerInfo {
        private final String workerId;
        private final int cores;
        private final String hostNodeId;
        private int tasksAssigned;

        public WorkerInfo(String workerId, int cores, String hostNodeId) {
            this.workerId = workerId;
            this.cores = cores;
            this.hostNodeId = hostNodeId;
            this.tasksAssigned = 0;
        }

        public String getWorkerId() {
            return workerId;
        }

        public int getCores() {
            return cores;
        }

        public String getHostNodeId() {
            return hostNodeId;
        }

        public int getTasksAssigned() {
            return tasksAssigned;
        }

        public void incrementTasks() {
            this.tasksAssigned++;
        }
    }

    private final TaskSender taskSender;
    private final Map<String, WorkerInfo> workers = new LinkedHashMap<>();
    private final Map<Integer, TaskLocality> taskLocalities = new LinkedHashMap<>();
    private final Map<Integer, String> taskWorkers = new LinkedHashMap<>();

    public TaskScheduler(TaskSender taskSender) {
        this.taskSender = Objects.requireNonNull(taskSender, "taskSender");
    }

    public synchronized void handleWorkerRegistration(RegisterWorkerRequest request) {
        WorkerInfo info = new WorkerInfo(request.workerId(), request.cores(), request.hostNodeId());
        workers.put(request.workerId(), info);
        logger.info("Registered worker '{}' on host '{}' with {} cores",
                request.workerId(), request.hostNodeId(), request.cores());
    }

    private final RequestWaitingList<Integer, TaskResultResponse> requestWaitingList = new RequestWaitingList<>(1000);

    public synchronized <T> TickCompletableFuture<List<T>> submitTasks(List<RDDLiteTask<T>> tasks, RDDLite<T> rdd) {
        if (workers.isEmpty()) {
            throw new IllegalStateException("No workers registered to execute tasks");
        }

        int numTasks = tasks.size();
        JobWaiter<T> waiter = new JobWaiter<>(numTasks);
        if (numTasks == 0) {
            return waiter.completionFuture();
        }

        TaskResultCallback<T> callback = new TaskResultCallback<>(waiter);

        Partition[] partitions = rdd.getPartitions();

        for (RDDLiteTask<T> task : tasks) {
            int partitionIndex = task.getPartitionId();
            Partition partition = (partitionIndex < partitions.length)
                    ? partitions[partitionIndex]
                    : null;

            List<String> preferred = (partition != null)
                    ? rdd.getPreferredLocations(partition)
                    : Collections.emptyList();

            // 1. Attempt NODE_LOCAL placement
            WorkerInfo chosenWorker = null;
            TaskLocality locality = TaskLocality.ANY;

            if (!preferred.isEmpty()) {
                for (WorkerInfo w : workers.values()) {
                    if (preferred.contains(w.getHostNodeId()) || preferred.contains(w.getWorkerId())) {
                        chosenWorker = w;
                        locality = TaskLocality.NODE_LOCAL;
                        break;
                    }
                }
            }

            // 2. Fallback to least loaded worker
            if (chosenWorker == null) {
                chosenWorker = workers.values().stream()
                        .min(Comparator.comparingInt(WorkerInfo::getTasksAssigned))
                        .orElseThrow();
                locality = preferred.isEmpty() ? TaskLocality.NO_PREF : TaskLocality.ANY;
            }

            chosenWorker.incrementTasks();
            taskLocalities.put(task.getTaskId(), locality);
            taskWorkers.put(task.getTaskId(), chosenWorker.getWorkerId());

            requestWaitingList.add(task.getTaskId(), callback);

            // Serialize task and partition for network transmission
            byte[] serializedTask = SerializerUtil.serialize(task);
            byte[] serializedPartition = SerializerUtil.serialize(partition);

            SubmitTaskRequest request = new SubmitTaskRequest(
                    task.getTaskId(),
                    task.getStageId(),
                    task.getPartitionId(),
                    locality,
                    serializedTask,
                    serializedPartition
            );

            logger.info("Dispatched task {} (partition {}) to worker '{}' with locality {}",
                    task.getTaskId(), partitionIndex, chosenWorker.getWorkerId(), locality);

            taskSender.send(chosenWorker.getWorkerId(), request);
        }

        return waiter.completionFuture();
    }

    public synchronized void handleTaskResult(TaskResultResponse response) {
        if (!response.success()) {
            requestWaitingList.handleError(response.taskId(),
                    new RuntimeException("Task " + response.taskId() + " failed: " + response.error()));
            return;
        }
        requestWaitingList.handleResponse(response.taskId(), response);
    }

    public synchronized void tick() {
        requestWaitingList.tick();
    }

    public synchronized int getNumWorkers() {
        return workers.size();
    }

    public synchronized TaskLocality getTaskLocality(int taskId) {
        return taskLocalities.get(taskId);
    }

    public synchronized String getAssignedWorker(int taskId) {
        return taskWorkers.get(taskId);
    }

    public synchronized Map<String, Integer> getWorkerTaskCounts() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (WorkerInfo w : workers.values()) {
            counts.put(w.getWorkerId(), w.getTasksAssigned());
        }
        return counts;
    }
}
