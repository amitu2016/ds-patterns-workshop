package sparklite.rdd;

import com.tickloom.future.TickCompletableFuture;
import sparklite.function.FunctionLite;
import sparklite.function.PredicateLite;

import java.util.*;

/**
 * An RDD that filters elements of the parent RDD using a predicate.
 * Corresponds to {@code org.apache.spark.rdd.MapPartitionsRDD} with filter.
 *
 * @param <T> Element type
 */
public class FilterRDDLite<T> implements RDDLite<T> {
    private final RDDLite<T> parent;
    private final PredicateLite<T> predicate;
    private final Partition[] partitions;

    public FilterRDDLite(RDDLite<T> parent, PredicateLite<T> f) {
        this.parent = Objects.requireNonNull(parent, "parent");
        this.predicate = Objects.requireNonNull(f, "predicate");
        this.partitions = parent.getPartitions();
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    @Override
    public TickCompletableFuture<Iterator<T>> compute(Partition split) {
        return parent.compute(split).thenApply(parentIter -> new Iterator<T>() {
            private T nextElement = null;
            private boolean hasNext = false;

            private void findNext() {
                while (!hasNext && parentIter.hasNext()) {
                    T element = parentIter.next();
                    if (predicate.test(element)) {
                        nextElement = element;
                        hasNext = true;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                if (!hasNext) {
                    findNext();
                }
                return hasNext;
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                hasNext = false;
                return nextElement;
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
    public <R> RDDLite<R> map(FunctionLite<T, R> f) {
        return new MapRDDLite<>(this, f);
    }

    @Override
    public RDDLite<T> filter(PredicateLite<T> f) {
        return new FilterRDDLite<>(this, f);
    }
}

