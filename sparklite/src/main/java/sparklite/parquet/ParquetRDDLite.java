package sparklite.parquet;

import com.tickloom.future.TickCompletableFuture;
import objectstorelite.client.ObjectStoreClient;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.io.PrefetchedInputFile;
import parquetlite.schema.TableRecord;
import sparklite.rdd.FilterRDDLite;
import sparklite.rdd.MapRDDLite;
import sparklite.rdd.Partition;
import sparklite.rdd.RDDLite;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An {@link RDDLite} backed by an Apache Parquet file on object storage.
 *
 * <p>Implements the core architectural mapping:
 * <b>One Parquet Row Group → One RDDLite Partition</b>.
 *
 * <p>A row group is {@code BlockMetaData} in parquet-mr — the class is named for the intent that a
 * row group <i>be</i> one HDFS block ({@code parquet.block.size}, default 128MB), so one task reads
 * one block from one DataNode. This mapping inherits that shape, though over object storage there
 * are no blocks to be local to. See parquetlite/MAPPING.md.
 *
 * <p>During initialization, it reads the Parquet footer from the end of the file
 * via {@link ObjectStoreParquetReader#fetchFooterWithBytes} using range reads. Each row group
 * (block) described in the footer becomes a {@link ParquetPartition}.
 */
public class ParquetRDDLite implements RDDLite<TableRecord> {

    private final String objectKey;
    private final long fileSize;
    private final byte[] cachedFooterBytes;
    private final long cachedFooterStart;
    private final ParquetPartition[] partitions;

    /**
     * Injected by the worker after this RDD is deserialized from a task. Transient because a client
     * cannot cross the wire — mirrors Spark's {@code @transient} RDD fields. There is no driver-side
     * equivalent: reading a partition needs a worker's client, so {@link #collect()} on the driver
     * fails by design.
     */
    private transient ObjectStoreClient client;

    /**
     * Builds the RDD from a footer the caller already read. Performs no I/O — this is the
     * {@code getSplits} boundary: planning happens in the driver-side caller, which is also the only
     * place able to block on a {@code TickCompletableFuture}, and the RDD is left holding nothing but
     * data plus {@link #compute}.
     */
    public ParquetRDDLite(String objectKey,
                          long fileSize,
                          ObjectStoreParquetReader.FooterResult footer,
                          List<List<String>> preferredLocationsPerRowGroup) {
        this.objectKey = Objects.requireNonNull(objectKey, "objectKey");
        this.fileSize = fileSize;
        Objects.requireNonNull(footer, "footer");
        this.cachedFooterBytes = footer.footerBytes();
        this.cachedFooterStart = footer.footerStart();

        List<BlockMetaData> blocks = footer.metadata().getBlocks();
        this.partitions = new ParquetPartition[blocks.size()];

        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData block = blocks.get(i);
            List<String> preferred = (preferredLocationsPerRowGroup != null && i < preferredLocationsPerRowGroup.size())
                    ? preferredLocationsPerRowGroup.get(i)
                    : Collections.emptyList();

            partitions[i] = new ParquetPartition(
                    i,
                    objectKey,
                    i,
                    block.getRowCount(),
                    block.getStartingPos(),
                    block.getCompressedSize(),   // on-disk bytes; getTotalByteSize is uncompressed
                    preferred
            );
        }
    }

    @Override
    public Class<?> clientType() {
        return ObjectStoreClient.class;
    }

    @Override
    public void setClient(Object client) {
        this.client = (ObjectStoreClient) client;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public long getFileSize() {
        return fileSize;
    }

    @Override
    public Partition[] getPartitions() {
        return partitions;
    }

    /**
     * Reads this partition's row group with the injected {@link ObjectStoreClient} and returns the
     * client's future directly — the scheduler's tick loop drives it. Nothing here blocks, which is
     * what lets {@code compute} run inside a tick without pumping the cluster from within one.
     *
     * <p>Exactly one network call: the footer travelled with the task, so only the row group's bytes
     * are fetched. {@code parquet-mr} then decodes from memory via {@link PrefetchedInputFile}.
     */
    @Override
    public TickCompletableFuture<Iterator<TableRecord>> compute(Partition split) {
        if (!(split instanceof ParquetPartition parquetPartition)) {
            return failed(new IllegalArgumentException(
                    "Invalid partition type: " + split.getClass().getName() + ", expected ParquetPartition"));
        }
        if (client == null) {
            // Mirrors Spark's RDD.sc, which throws "This RDD lacks a SparkContext" on an executor:
            // a transient field that was not re-supplied must fail loudly, never fall back.
            return failed(new IllegalStateException(
                    "No ObjectStoreClient set on ParquetRDDLite for partition " + split.index()
                            + " — a worker injects it after deserializing the task. Computing this"
                            + " RDD on the driver (collect() outside a cluster) is not supported."));
        }

        return client.getObjectRange(objectKey, parquetPartition.getStartOffset(),
                        (int) parquetPartition.getLength())
                .thenApply(response -> {
                    if (!response.success()) {
                        throw new RuntimeException("Failed to read row group "
                                + parquetPartition.getRowGroupIndex() + " of " + objectKey
                                + ": " + response.error());
                    }
                    try {
                        PrefetchedInputFile inputFile = PrefetchedInputFile.of(fileSize)
                                .with(cachedFooterStart, cachedFooterBytes)
                                .with(parquetPartition.getStartOffset(), response.data());
                        return ObjectStoreParquetReader
                                .readRowGroups(inputFile, List.of(parquetPartition.getRowGroupIndex()))
                                .iterator();
                    } catch (Exception e) {
                        throw new RuntimeException(
                                "Failed to decode Parquet row group for partition " + split.index(), e);
                    }
                });
    }

    private static TickCompletableFuture<Iterator<TableRecord>> failed(Exception cause) {
        TickCompletableFuture<Iterator<TableRecord>> future = new TickCompletableFuture<>();
        future.fail(cause);
        return future;
    }

    @Override
    public List<RDDLite<?>> getDependencies() {
        return Collections.emptyList();
    }

    @Override
    public List<String> getPreferredLocations(Partition split) {
        if (split instanceof ParquetPartition pp) {
            return pp.getPreferredLocations();
        }
        return Collections.emptyList();
    }

    @Override
    public <R> RDDLite<R> map(sparklite.function.FunctionLite<TableRecord, R> f) {
        return new MapRDDLite<>(this, f);
    }

    @Override
    public RDDLite<TableRecord> filter(sparklite.function.PredicateLite<TableRecord> f) {
        return new FilterRDDLite<>(this, f);
    }
}
