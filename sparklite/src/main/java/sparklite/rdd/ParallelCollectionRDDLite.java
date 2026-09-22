package sparklite.rdd;

import com.tickloom.future.TickCompletableFuture;
import sparklite.function.FunctionLite;
import sparklite.function.PredicateLite;

import java.util.*;

/**
 * An RDD that represents data already in memory, partitioned evenly.
 * Corresponds to {@code org.apache.spark.rdd.ParallelCollectionRDD}.
 */
public class ParallelCollectionRDDLite<T> implements RDDLite<T> {
    private final List<T> data;
    private final int numPartitions;

    public ParallelCollectionRDDLite(List<T> data, int numPartitions) {
        this.data = new ArrayList<>(data);
        this.numPartitions = numPartitions;
    }

    @Override
    public Partition[] getPartitions() {
        Partition[] result = new Partition[numPartitions];
        int itemsPerPartition = (int) Math.ceil((double) data.size() / numPartitions);

        for (int i = 0; i < numPartitions; i++) {
            int start = i * itemsPerPartition;
            int end = Math.min(start + itemsPerPartition, data.size());
            List<T> partitionData = (start < data.size()) ? data.subList(start, end) : Collections.emptyList();
            result[i] = new ParallelCollectionPartition<>(i, partitionData);
        }

        return result;
    }

    @Override
    public TickCompletableFuture<Iterator<T>> compute(Partition split) {
        TickCompletableFuture<Iterator<T>> future = new TickCompletableFuture<>();
        if (!(split instanceof ParallelCollectionPartition)) {
            future.fail(new IllegalArgumentException(
                    "Invalid partition type: " + split.getClass().getName() + ", expected: ParallelCollectionPartition"));
            return future;
        }

        @SuppressWarnings("unchecked")
        ParallelCollectionPartition<T> partition = (ParallelCollectionPartition<T>) split;
        future.complete(partition.iterator());
        return future;
    }

    @Override
    public List<RDDLite<?>> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return Collections.emptyList();
    }

    @Override
    public <R> RDDLite<R> map(FunctionLite<T, R> f) {
        return new MapRDDLite<>(this, f);
    }

    @Override
    public RDDLite<T> filter(PredicateLite<T> f) {
        return new FilterRDDLite<>(this, f);
    }
}
