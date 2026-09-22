package sparklite.rdd;

import com.tickloom.future.TickCompletableFuture;
import org.junit.jupiter.api.Test;
import sparklite.context.SparkLiteContext;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RDDLiteTest {

    @Test
    void testParallelCollectionPartitioning() {
        List<Integer> data = List.of(10, 20, 30, 40, 50);
        ParallelCollectionRDDLite<Integer> rdd = new ParallelCollectionRDDLite<>(data, 3);

        Partition[] partitions = rdd.getPartitions();
        assertEquals(3, partitions.length);

        assertEquals(0, partitions[0].index());
        assertEquals(1, partitions[1].index());
        assertEquals(2, partitions[2].index());
    }

    @Test
    void mapFilterAndRunJobInLocalMode() {
        List<Integer> data = List.of(1, 2, 3, 4, 5, 6);
        RDDLite<Integer> rdd = new ParallelCollectionRDDLite<>(data, 2)
                .map(x -> x * 10)
                .filter(x -> x > 30)
                .map(x -> x + 1);

        // No driver: SparkLiteContext falls back to local mode, exactly as a SparkContext created
        // with master = "local[*]" does.
        TickCompletableFuture<List<Integer>> future = new SparkLiteContext().runJob(rdd);
        assertTrue(future.isCompleted());
        List<Integer> result = future.getResult();

        assertEquals(List.of(41, 51, 61), result);
    }
}
