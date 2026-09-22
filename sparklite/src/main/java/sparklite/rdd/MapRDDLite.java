package sparklite.rdd;

import com.tickloom.future.TickCompletableFuture;
import sparklite.function.FunctionLite;
import sparklite.function.PredicateLite;

import java.util.*;

/**
 * An RDD that applies a map function to each element of the parent RDD.
 * Corresponds to {@code org.apache.spark.rdd.MapPartitionsRDD}.
 *
 * @param <T> Input element type
 * @param <R> Output element type
 */
public class MapRDDLite<T, R> implements RDDLite<R> {
    private final RDDLite<T> parent;
    private final FunctionLite<T, R> mapFunction;
    private final Partition[] partitions;

    public MapRDDLite(RDDLite<T> parent, FunctionLite<T, R> f) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.mapFunction = Objects.requireNonNull(f, "mapFunction");
        this.partitions = parent.getPartitions();
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    @Override
    public TickCompletableFuture<Iterator<R>> compute(Partition split) {
        return parent.compute(split).thenApply(parentIter -> new Iterator<R>() {
            @Override
            public boolean hasNext() {
                return parentIter.hasNext();
            }

            @Override
            public R next() {
                return mapFunction.apply(parentIter.next());
            }
        });
    }

    @Override
    public List<RDDLite<?>> getDependencies() {
        return Collections.singletonList(parent);
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        return parent.getPreferredLocations(split);
    }

    @Override
    public <U> RDDLite<U> map(FunctionLite<R, U> f) {
        return new MapRDDLite<>(this, f);
    }

    @Override
    public RDDLite<R> filter(PredicateLite<R> f) {
        return new FilterRDDLite<>(this, f);
    }
}

