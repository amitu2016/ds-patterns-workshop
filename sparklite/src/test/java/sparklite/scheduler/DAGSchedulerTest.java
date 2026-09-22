package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import org.junit.jupiter.api.Test;
import sparklite.protocol.RegisterWorkerRequest;
import sparklite.protocol.SubmitTaskRequest;
import sparklite.protocol.TaskResultResponse;
import sparklite.rdd.ParallelCollectionRDDLite;
import sparklite.rdd.RDDLite;
import sparklite.util.SerializerUtil;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DAGSchedulerTest {

    @Test
    void testDAGSchedulerSubmitJob() {
        List<SubmitTaskRequest> submitted = new ArrayList<>();
        TaskScheduler taskScheduler = new TaskScheduler((workerId, request) -> {
            submitted.add(request);
        });

        taskScheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-0", 2, "host-0"));

        DAGScheduler dagScheduler = new DAGScheduler(taskScheduler);

        RDDLite<Integer> rdd = new ParallelCollectionRDDLite<>(List.of(1, 2, 3, 4), 2);
        TickCompletableFuture<List<Integer>> jobFuture = dagScheduler.submitJob(rdd);

        assertEquals(2, submitted.size());

        // Simulate worker replies for both tasks
        for (SubmitTaskRequest req : submitted) {
            Task<?> task = SerializerUtil.deserialize(req.serializedTask());
            var partition = SerializerUtil.deserialize(req.serializedPartition());
            @SuppressWarnings("unchecked")
            var partFuture = ((Task<List<Integer>>) task).execute((sparklite.rdd.Partition) partition);
            assertTrue(partFuture.isCompleted());
            List<Integer> computed = partFuture.getResult();

            taskScheduler.handleTaskResult(new TaskResultResponse(
                    req.taskId(), req.stageId(), req.partitionId(),
                    true, SerializerUtil.serialize(computed), null));
        }

        assertTrue(jobFuture.isCompleted());
        List<Integer> result = jobFuture.getResult();
        assertEquals(List.of(1, 2, 3, 4), result);
    }
}

