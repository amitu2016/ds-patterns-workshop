package sparklite.scheduler;

import com.tickloom.ProcessId;
import org.junit.jupiter.api.Test;
import sparklite.protocol.TaskResultResponse;
import sparklite.util.SerializerUtil;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class JobWaiterTest {

    @Test
    void resultsAreOrderedByPartitionIndexNotByArrival() {
        JobWaiter<String> waiter = new JobWaiter<>(3);

        waiter.taskSucceeded(2, List.of("c"));
        waiter.taskSucceeded(0, List.of("a"));
        assertFalse(waiter.completionFuture().isCompleted(), "a job is not done until every partition is");
        waiter.taskSucceeded(1, List.of("b"));

        assertTrue(waiter.completionFuture().isCompleted());
        assertEquals(List.of("a", "b", "c"), waiter.completionFuture().getResult());
    }

    @Test
    void aJobWithNoPartitionsIsAlreadyDone() {
        JobWaiter<String> waiter = new JobWaiter<>(0);
        assertTrue(waiter.completionFuture().isCompleted());
        assertEquals(List.of(), waiter.completionFuture().getResult());
    }

    @Test
    void theFirstFailureEndsTheJobAndLaterResultsAreIgnored() {
        JobWaiter<String> waiter = new JobWaiter<>(2);

        waiter.taskSucceeded(0, List.of("a"));
        waiter.jobFailed(new RuntimeException("worker lost"));
        waiter.taskSucceeded(1, List.of("b"));

        assertTrue(waiter.completionFuture().isFailed());
        assertEquals("worker lost", waiter.completionFuture().getException().getMessage());
    }

    @Test
    void aPartitionReportingTwiceIsAnError() {
        JobWaiter<String> waiter = new JobWaiter<>(2);

        waiter.taskSucceeded(0, List.of("a"));
        waiter.taskSucceeded(0, List.of("a again"));

        assertTrue(waiter.completionFuture().isFailed());
        assertTrue(waiter.completionFuture().getException().getMessage().contains("reported twice"));
    }

    /**
     * Defence in depth, not a fix for a live bug: {@code TaskScheduler.handleTaskResult} already
     * routes {@code success = false} to {@code RequestWaitingList.handleError}, so a failed response
     * never reaches {@code onResponse} by that path. The check matters because the callback should
     * be safe for any caller — counting a failed task as a finished partition would complete the
     * job with one partition's records missing, which looks like success and returns a wrong answer.
     */
    @Test
    void aFailedTaskFailsTheJobRatherThanShorteningTheAnswer() {
        JobWaiter<String> waiter = new JobWaiter<>(2);
        TaskResultCallback<String> callback = new TaskResultCallback<>(waiter);
        ProcessId worker = ProcessId.of("spark-worker-0");

        callback.onResponse(new TaskResultResponse(
                1, 0, 0, true, SerializerUtil.serialize(List.of("kept")), null), worker);
        callback.onResponse(new TaskResultResponse(
                2, 0, 1, false, null, "No ObjectStoreClient set on ParquetRDDLite"), worker);

        assertTrue(waiter.completionFuture().isFailed(), "a failed task must fail the job");
        assertTrue(waiter.completionFuture().getException().getMessage()
                .contains("No ObjectStoreClient set"),
                waiter.completionFuture().getException().getMessage());
    }
}
