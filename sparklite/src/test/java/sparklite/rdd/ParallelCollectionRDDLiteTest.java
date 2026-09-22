package sparklite.rdd;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ParallelCollectionRDDLiteTest {
    @Test
    public void testParallelCollectionPartitioning() {
        ParallelCollectionRDDLite<Integer> rdd = new ParallelCollectionRDDLite<>(List.of(1,2,3,4), 2);
        Partition[] partitions = rdd.getPartitions();
        assertEquals(2, partitions.length);
        assertEquals(List.of(1, 2), ((ParallelCollectionPartition)partitions[0]).getData());
        assertEquals(List.of(3, 4), ((ParallelCollectionPartition)partitions[1]).getData());
    }

}