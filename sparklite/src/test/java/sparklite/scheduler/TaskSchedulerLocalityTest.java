package sparklite.scheduler;

import com.tickloom.future.TickCompletableFuture;
import org.junit.jupiter.api.Test;
import sparklite.protocol.RegisterWorkerRequest;
import sparklite.protocol.SubmitTaskRequest;
import sparklite.protocol.TaskLocality;
import sparklite.rdd.Partition;
import sparklite.rdd.RDDLite;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TaskSchedulerLocalityTest {

    private record StubPartition(int index, List<String> preferred) implements Partition {
        @Override
        public int index() {
            return index;
        }
    }

    private static class StubRDD implements RDDLite<Integer> {
        private final StubPartition[] partitions;

        StubRDD(List<List<String>> preferredPerPartition) {
            this.partitions = new StubPartition[preferredPerPartition.size()];
            for (int i = 0; i < preferredPerPartition.size(); i++) {
                this.partitions[i] = new StubPartition(i, preferredPerPartition.get(i));
            }
        }

        @Override
        public Partition[] getPartitions() {
            return partitions;
        }

        @Override
        public TickCompletableFuture<Iterator<Integer>> compute(Partition split) {
            return TickCompletableFuture.completed(Collections.emptyIterator());
        }

        @Override
        public List<RDDLite<?>> getDependencies() {
            return Collections.emptyList();
        }

        @Override
        public List<String> getPreferredLocations(Partition split) {
            return ((StubPartition) split).preferred();
        }

        @Override
        public <R> RDDLite<R> map(sparklite.function.FunctionLite<Integer, R> f) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RDDLite<Integer> filter(sparklite.function.PredicateLite<Integer> f) {
            throw new UnsupportedOperationException();
        }
    }

    @Test
    void testLocalityPlacementAndFallback() {
        List<SubmitTaskRequest> sentRequests = new ArrayList<>();
        TaskScheduler scheduler = new TaskScheduler((workerId, request) -> {
            sentRequests.add(request);
        });

        // Register 3 workers located on storage nodes
        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-0", 2, "storage-node-0"));
        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-1", 2, "storage-node-1"));
        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-2", 2, "storage-node-2"));

        assertEquals(3, scheduler.getNumWorkers());

        // P0 prefers storage-node-1 -> should go to worker-1 (NODE_LOCAL)
        // P1 prefers storage-node-2 -> should go to worker-2 (NODE_LOCAL)
        // P2 prefers non-existent storage-node-99 -> should fallback to ANY
        StubRDD rdd = new StubRDD(List.of(
                List.of("storage-node-1"),
                List.of("storage-node-2"),
                List.of("storage-node-99")
        ));

        List<RDDLiteTask<Integer>> tasks = List.of(
                new RDDLiteTask<>(100, 1, 0, rdd),
                new RDDLiteTask<>(101, 1, 1, rdd),
                new RDDLiteTask<>(102, 1, 2, rdd)
        );

        scheduler.submitTasks(tasks, rdd);

        assertEquals(3, sentRequests.size());

        // Task 100: matched worker-1
        assertEquals(TaskLocality.NODE_LOCAL, scheduler.getTaskLocality(100));
        assertEquals("worker-1", scheduler.getAssignedWorker(100));

        // Task 101: matched worker-2
        assertEquals(TaskLocality.NODE_LOCAL, scheduler.getTaskLocality(101));
        assertEquals("worker-2", scheduler.getAssignedWorker(101));

        // Task 102: fallback to ANY (worker-0 was free)
        assertEquals(TaskLocality.ANY, scheduler.getTaskLocality(102));
        assertNotNull(scheduler.getAssignedWorker(102));
    }
}
