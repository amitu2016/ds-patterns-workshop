package sparklite.context;

import com.tickloom.ProcessId;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.testkit.Cluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sparklite.rdd.RDDLite;
import sparklite.worker.SparkLiteWorker;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SparkLiteClusterTest {

    private Cluster cluster;
    private SparkLiteDriver driver;
    private SparkLiteContext sc;

    @BeforeEach
    void setUp() throws Exception {
        ProcessId driverId = ProcessId.of("driver");
        ProcessId worker1Id = ProcessId.of("worker-1");
        ProcessId worker2Id = ProcessId.of("worker-2");

        List<ProcessId> allIds = List.of(driverId, worker1Id, worker2Id);

        cluster = new Cluster()
                .withProcessIds(allIds)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    if (params.id().equals(driverId)) {
                        return new SparkLiteDriver(params);
                    } else {
                        return new SparkLiteWorker(params, driverId, "host-" + params.id().name(), 2);
                    }
                })
                .start();

        driver = cluster.getNode(driverId);
        sc = new SparkLiteContext(driver);

        // Wait until both workers have registered with the driver via tick progression
        cluster.tickUntil(() -> driver.getTaskScheduler().getNumWorkers() >= 2);
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void testEndToEndClusterExecution() {
        assertEquals(2, driver.getTaskScheduler().getNumWorkers());

        RDDLite<Integer> rdd = sc.parallelize(List.of(1, 2, 3, 4, 5, 6, 7, 8), 2)
                .map(x -> x * 2)
                .filter(x -> x > 4);

        TickCompletableFuture<List<Integer>> jobFuture = sc.runJob(rdd);

        // Advance simulation ticks until job completes
        cluster.tickUntil(jobFuture::isCompleted);

        assertTrue(jobFuture.isCompleted());
        List<Integer> results = jobFuture.getResult();

        assertEquals(List.of(6, 8, 10, 12, 14, 16), results);
    }
}
