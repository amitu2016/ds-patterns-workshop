package parquetlite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import objectstorelite.client.ObjectStoreClient;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.server.StorageNode;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.filter.RowGroupFilter;
import parquetlite.io.ByteRange;
import parquetlite.io.PrefetchedInputFile;
import parquetlite.io.RangeMetrics;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slide 3.5: Parquet + Distributed Processing (Over Object Store with Predicate Pushdown).
 *
 * <p>Demonstrates:
 * 1. Storing Parquet on an erasure-coded object store ({@code objectstorelite}).
 * 2. Explicit Two-Phase Range Reads:
 *    - Phase 1: Fetch only the footer range at the EOF.
 *    - Phase 2: Evaluate query predicate against row group min/max statistics.
 *    - Phase 3: Fetch ONLY matching row groups over the wire.
 * 3. Exact byte comparison: Naive full file scan vs. Predicate Pushdown.
 *
 * <p>Run via: {@code make demo-3.5}
 */
@DisplayName("Demo 3.5: Parquet + Distributed Processing")
public class Demo_3_5_ParquetDistributedProcessing {

    // TRY IT: change MIN_AGE_FILTER to 65 (skips 6 of 7 groups) or 20 (reads all groups)
    static final int MIN_AGE_FILTER = 60;

    private static final int DATA_SHARDS = 4;
    private static final int PARITY_SHARDS = 2;
    private static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    @TempDir
    Path clusterStorageRoot;

    private Cluster cluster;
    private ObjectStoreClient client;
    private List<ProcessId> nodeIds;
    /** Learned from the store, never from a local copy — a reader arriving at an object has none. */
    private long objectSize;

    /**
     * Round trip 0: {@code FileSystem.getFileStatus} upstream, a {@code HEAD Object} on S3. Costs a
     * round trip and zero body bytes, which is why it shows in the request count but not the byte
     * total — and why newer readers replace it with a suffix range.
     */
    private long fetchObjectSize(RangeMetrics metrics) {
        if (metrics != null) {
            metrics.record(S3_KEY, 0, 0, "object size (HEAD — a round trip, zero bytes)");
        }
        return cluster.tickUntilComplete(client.getObjectSize(S3_KEY)).size();
    }
    private static final String S3_KEY = "data/customers.parquet";

    /** One explicit range GET against the erasure-coded store, recorded so it can be counted. */
    private byte[] fetch(ByteRange range, RangeMetrics metrics, String why) {
        if (metrics != null) {
            metrics.record(S3_KEY, range.offset(), range.length(), why);
        }
        return cluster.tickUntilComplete(
                client.getObjectRange(S3_KEY, range.offset(), range.length())).data();
    }

    @BeforeEach
    void setUp() throws Exception {
        nodeIds = new ArrayList<>();
        List<Path> nodePaths = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            ProcessId id = ProcessId.of("storage-node-" + i);
            nodeIds.add(id);
            nodePaths.add(clusterStorageRoot.resolve("disk-node-" + i));
        }

        ErasureSet erasureSet = new ErasureSet(0, nodeIds);
        ErasureSetMapper mapper = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.CRCMOD, null, List.of(erasureSet));

        cluster = new Cluster()
                .withProcessIds(nodeIds)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    int index = nodeIds.indexOf(params.id());
                    return new StorageNode(peerIds, params, nodePaths.get(index), mapper, DATA_SHARDS, PARITY_SHARDS);
                })
                .start();

        client = cluster.newClient(
                ProcessId.of("spark-worker-client"),
                (peers, params) -> new ObjectStoreClient(peers, params, nodeIds.get(0))
        );


        // Create sample Parquet dataset (45 customers across 7 row groups)
        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();
        // Write a real Parquet file and upload its bytes. From here on the object is read the way
        // a client reads one it did not write: the local file's length never enters the read path.
        Path localFile = clusterStorageRoot.resolve("customers.parquet");
        ParquetWriterHelper.writeParquetFile(localFile, schema, customers, 512);

        // Upload to object store
        var putFuture = client.putObject(S3_KEY, java.nio.file.Files.readAllBytes(localFile));
        cluster.tickUntil(putFuture::isCompleted);
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void compareNaiveScanVsPredicatePushdown() throws Exception {
        System.out.println("================================================================================");
        System.out.println("  DEMO 3.5: Predicate Pushdown over Object Storage (Exact Byte Reduction)       ");
        System.out.println("================================================================================");
        // The size is deliberately not printed here: a reader does not know it yet. Learning it is
        // the first round trip of the protocol, and it is reported below where it is fetched.
        System.out.printf("Object: s3://default/%s (erasure coded 4+2 across %d nodes)%n",
                S3_KEY, TOTAL_SHARDS);
        System.out.printf("Query:  SELECT * FROM customers WHERE age >= %d%n%n", MIN_AGE_FILTER);

        // -------------------------------------------------------------
        // RUN 1: NAIVE FULL SCAN (Read all row groups over object store)
        // -------------------------------------------------------------
        RangeMetrics naiveMetrics = new RangeMetrics();
        objectSize = fetchObjectSize(naiveMetrics);
        System.out.printf("   HEAD says the object is %d bytes.%n", objectSize);

        System.out.println("── [1] Naive Full File Scan (No Pushdown) ───────────────────────────────────");
        System.out.println("   A reader with no footer statistics has nothing to skip on, so it takes");
        System.out.println("   the whole object in one GET and filters afterwards.");
        byte[] wholeObject = fetch(new ByteRange(0, (int) objectSize), naiveMetrics, "whole object");
        List<TableRecord> allRecords = ObjectStoreParquetReader.readAllRecords(
                PrefetchedInputFile.wholeObject(wholeObject));
        long matchingInNaive = allRecords.stream().filter(r -> r.getInteger("age") >= MIN_AGE_FILTER).count();
        System.out.printf("   Total records read:     %d%n", allRecords.size());
        System.out.printf("   Matching records:       %d%n", matchingInNaive);
        System.out.printf("   Total bytes transferred: %d bytes (Full file)%n%n", naiveMetrics.totalBytesTransferred());

        // -------------------------------------------------------------
        // RUN 2: EXPLICIT TWO-PHASE RANGE READ WITH PREDICATE PUSHDOWN
        // -------------------------------------------------------------
        RangeMetrics selectiveMetrics = new RangeMetrics();
        System.out.println("── [2] Explicit Two-Phase Range Read (Predicate Pushdown) ───────────────────");

        // Phase 0: the size, which a reader must learn before it can find the tail.
        fetchObjectSize(selectiveMetrics);
        // Phase 1: two more round trips — the tail says how long the footer is, then the footer.
        System.out.println("   Step 1: Reading footer range from object tail...");
        ByteRange hintRange = ObjectStoreParquetReader.footerLengthHintRange(objectSize);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                objectSize, fetch(hintRange, selectiveMetrics, "footer length hint (last 8 bytes)"));
        var footerResult = ObjectStoreParquetReader.readFooter(
                objectSize, footerRange, fetch(footerRange, selectiveMetrics, "footer metadata"));
        ParquetMetadata metadata = footerResult.metadata();

        List<BlockMetaData> allBlocks = metadata.getBlocks();
        System.out.printf("   Found %d row groups in footer metadata.%n%n", allBlocks.size());

        // Phase 2: Filter row groups using min/max statistics
        System.out.println("   Step 2: Evaluating predicate against row group statistics:");
        List<Integer> candidateIndices = new ArrayList<>();
        for (int i = 0; i < allBlocks.size(); i++) {
            BlockMetaData block = allBlocks.get(i);
            boolean skip = RowGroupFilter.canSkipMinInteger(block, "age", MIN_AGE_FILTER);
            var col = RowGroupFilter.findColumnChunk(block, "age");
            String maxAge = col != null && col.getStatistics() != null ?
                    col.getStatistics().maxAsString() : "?";

            if (skip) {
                System.out.printf("      ⏭️  Row Group [%d] (max age: %-2s): SKIPPED (0 bytes transferred!)%n", i, maxAge);
            } else {
                System.out.printf("      ✅ Row Group [%d] (max age: %-2s): MATCHED (marked for range read)%n", i, maxAge);
                candidateIndices.add(i);
            }
        }
        System.out.printf("   Result: %d of %d row groups selected for fetch.%n%n",
                candidateIndices.size(), allBlocks.size());

        // Phase 3: Fetch ONLY matching row groups over the object store
        System.out.println("   Step 3: Fetching byte ranges for matching row groups only...");
        // The footer is already in hand; only the surviving row groups cross the network. Decoding
        // then happens entirely in memory — the same shape a sparklite task uses.
        PrefetchedInputFile selectiveInput = PrefetchedInputFile.of(objectSize)
                .with(footerResult.footerStart(), footerResult.footerBytes());
        for (int index : candidateIndices) {
            ByteRange range = ObjectStoreParquetReader.rowGroupRange(allBlocks.get(index));
            selectiveInput = selectiveInput.with(
                    range.offset(), fetch(range, selectiveMetrics, "row group " + index));
        }

        List<TableRecord> selectiveRecords = ObjectStoreParquetReader.readRowGroups(selectiveInput, candidateIndices);
        System.out.printf("   Records retrieved from selected groups: %d%n%n", selectiveRecords.size());

        selectiveMetrics.printTimeline();

        // -------------------------------------------------------------
        // I/O REDUCTION SUMMARY
        // -------------------------------------------------------------
        long naiveBytes = naiveMetrics.totalBytesTransferred();
        long selectiveBytes = selectiveMetrics.totalBytesTransferred();
        long bytesSaved = naiveBytes - selectiveBytes;
        double reductionPct = ((double) bytesSaved / naiveBytes) * 100.0;

        System.out.println("\n📊 PERFORMANCE SUMMARY:");
        System.out.printf("   Naive Scan Transferred:     %6d bytes%n", naiveBytes);
        System.out.printf("   Pushdown Scan Transferred:  %6d bytes (footer + target chunks)%n", selectiveBytes);
        System.out.printf("   Data Transfer Saved:        %6d bytes%n", bytesSaved);
        System.out.printf("   Network I/O Reduction:      %6.1f%%%n", reductionPct);
        System.out.println("================================================================================\n");

        assertTrue(selectiveBytes < naiveBytes, "Pushdown scan must transfer fewer bytes than naive scan");
    }
}
