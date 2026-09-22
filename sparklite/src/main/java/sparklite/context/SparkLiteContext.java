package sparklite.context;

import com.tickloom.future.TickCompletableFuture;
import parquetlite.file.ObjectStoreParquetReader;
import sparklite.parquet.ParquetRDDLite;
import sparklite.rdd.ParallelCollectionRDDLite;
import sparklite.rdd.RDDLite;
import sparklite.scheduler.LocalScheduler;

import java.util.List;

/**
 * Main entry point for SparkLite programming model, corresponding to {@code org.apache.spark.SparkContext}.
 */
public class SparkLiteContext {
    private final SparkLiteDriver driver;
    private final int defaultParallelism;

    public SparkLiteContext(SparkLiteDriver driver, int defaultParallelism) {
        this.driver = driver;
        this.defaultParallelism = defaultParallelism;
    }

    public SparkLiteContext(SparkLiteDriver driver) {
        this(driver, 2);
    }

    public SparkLiteContext() {
        this(null, 2);
    }

    public SparkLiteDriver getDriver() {
        return driver;
    }

    public int getDefaultParallelism() {
        return defaultParallelism;
    }

    public <T> RDDLite<T> parallelize(List<T> data) {
        return parallelize(data, defaultParallelism);
    }

    public <T> RDDLite<T> parallelize(List<T> data, int numPartitions) {
        return new ParallelCollectionRDDLite<>(data, numPartitions);
    }

    /**
     * Plans a Parquet read from a footer the caller already fetched.
     *
     * <p>The footer read stays with the caller deliberately: it is the {@code getSplits} step, and
     * blocking on a {@code TickCompletableFuture} needs {@code Cluster}, which is test-scope only
     * (root {@code build.gradle}). Nothing in {@code src/main} can block.
     */
    public ParquetRDDLite parquetFile(String objectKey,
                                      long fileSize,
                                      ObjectStoreParquetReader.FooterResult footer,
                                      List<List<String>> preferredLocations) {
        return new ParquetRDDLite(objectKey, fileSize, footer, preferredLocations);
    }

    public ParquetRDDLite parquetFile(String objectKey,
                                      long fileSize,
                                      ObjectStoreParquetReader.FooterResult footer) {
        return new ParquetRDDLite(objectKey, fileSize, footer, List.of());
    }

    /**
     * Executes an action on an RDD across the cluster (via {@link SparkLiteDriver}) or locally if driver is not set.
     */
    public <T> TickCompletableFuture<List<T>> runJob(RDDLite<T> rdd) {
        if (driver != null) {
            return driver.getDagScheduler().submitJob(rdd);
        }
        return LocalScheduler.runJob(rdd);
    }
}
