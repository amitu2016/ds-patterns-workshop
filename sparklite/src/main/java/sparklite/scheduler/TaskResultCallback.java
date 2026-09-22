package sparklite.scheduler;

import com.tickloom.ProcessId;
import com.tickloom.messaging.RequestCallback;
import sparklite.protocol.TaskResultResponse;
import sparklite.util.SerializerUtil;

import java.util.List;

/**
 * Turns worker replies arriving on the {@code RequestWaitingList} into {@link JobWaiter} calls.
 *
 * <p>Nothing but an adapter: the aggregation — slot per partition, complete on the last one, fail
 * fast — lives in {@code JobWaiter}, which is also what local mode uses. Running a job on a cluster
 * or in this process differs in how results arrive, not in how they are assembled.
 *
 * @param <T> Element type of the RDD being computed
 */
public class TaskResultCallback<T> implements RequestCallback<TaskResultResponse> {

    private final JobWaiter<T> waiter;

    public TaskResultCallback(JobWaiter<T> waiter) {
        this.waiter = waiter;
    }

    @Override
    public synchronized void onResponse(TaskResultResponse response, ProcessId fromNode) {
        if (!response.success()) {
            waiter.jobFailed(new RuntimeException(
                    "Task for partition " + response.partitionId() + " failed: " + response.error()));
            return;
        }
        try {
            List<T> partitionRecords = SerializerUtil.deserialize(response.serializedResult());
            waiter.taskSucceeded(response.partitionId(), partitionRecords);
        } catch (Exception e) {
            waiter.jobFailed(e);
        }
    }

    @Override
    public synchronized void onError(Exception error) {
        waiter.jobFailed(error);
    }
}
