package sparklite.rdd;

import com.tickloom.future.TickCompletableFuture;
import sparklite.function.FunctionLite;
import sparklite.function.PredicateLite;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * RDDLite is an interface representing a Resilient Distributed Dataset,
 * corresponding to {@code org.apache.spark.rdd.RDD}.
 *
 * @param <T> The type of elements in this RDD
 */
public interface RDDLite<T> extends Serializable {
    int DEFAULT_NUM_PARTITIONS = 2;

    // Core RDD properties from Spark
    Partition[] getPartitions();
    TickCompletableFuture<Iterator<T>> compute(Partition split);
    List<RDDLite<?>> getDependencies();

    // Locality / preferred locations
    List<String> getPreferredLocations(Partition split);

    // Storage client, injected on the worker after deserialization.
    //
    // A real Spark RDD declares neither of these: its compute() names the client pool at compile
    // time (KafkaDataConsumer.acquire, FileSystem.get) and pulls from it, constructing on a miss.
    // tickloom clients can only be created by Cluster, so sparklite's worker pushes instead and has
    // to be told what to push. See docs/SPARKLITE-CLIENT-INJECTION.md §6.

    /** The client type this RDD needs on a worker, or null if it reads no external storage. */
    default Class<?> clientType() {
        return null;
    }

    /**
     * Supplies the client named by {@link #clientType()}. Called by the worker after it deserializes
     * the task, since the field holding a client must be transient.
     */
    default void setClient(Object client) {
        // RDDs with no clientType() need nothing.
    }

    // Transformations (lazy)
    <R> RDDLite<R> map(FunctionLite<T, R> f);
    RDDLite<T> filter(PredicateLite<T> f);

    // No collect() here. Real Spark's RDD.collect() calls sc.runJob, and so does this model:
    // SparkLiteContext.runJob submits to the driver, or falls back to LocalScheduler when there is
    // none. An RDD describes how to compute a partition; running a job is the scheduler's business.
}

