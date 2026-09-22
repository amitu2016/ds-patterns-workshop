package parquetlite.file;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import objectstorelite.client.ObjectStoreClient;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.protocol.PutObjectResponse;
import objectstorelite.server.StorageNode;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.filter.RowGroupFilter;
import parquetlite.io.ByteRange;
import parquetlite.io.PrefetchedInputFile;
import parquetlite.io.RangeMetrics;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStoreParquetReaderTest {

    private static final int DATA_SHARDS = 4;
    private static final int PARITY_SHARDS = 2;
    private static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    @TempDir
    Path clusterStorageRoot;

    private Cluster cluster;
    private ObjectStoreClient client;
    private List<ProcessId> nodeIds;

    /**
     * The object's size, learned from the store — never from a local copy of the bytes. A reader
     * arriving at an existing object has no local copy, and {@code ParquetFileReader} needs the
     * length before it can locate the footer at the tail.
     */
    private long objectSize;
    private int recordCount;
    private static final String OBJECT_KEY = "analytics/customers.parquet";

    @BeforeEach
    void setUp() throws Exception {
        nodeIds = new ArrayList<>();
        List<Path> nodePaths = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            ProcessId id = ProcessId.of("node-" + i);
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
                ProcessId.of("client-test"),
                (peers, params) -> new ObjectStoreClient(peers, params, nodeIds.get(0))
        );

        // Write a real Parquet file, then upload its bytes. Everything after this point reads the
        // object the way a client would -- the local file is write-side only, and its length is
        // deliberately not carried into the read path.
        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();
        recordCount = customers.size();

        Path localFile = clusterStorageRoot.resolve("customers.parquet");
        ParquetWriterHelper.writeParquetFile(localFile, schema, customers, 512);

        var putFuture = client.putObject(OBJECT_KEY, Files.readAllBytes(localFile));
        cluster.tickUntilComplete(putFuture);
        PutObjectResponse resp = putFuture.getResult();
        assertNotNull(resp);
        assertEquals(OBJECT_KEY, resp.key());
    }

    /**
     * Round trip 0: how big is the object?
     *
     * <p>Upstream this is {@code FileSystem.getFileStatus(path)} — a {@code HEAD Object} on S3,
     * which {@code HadoopInputFile.fromPath} issues before {@code ParquetFileReader} ever reads a
     * byte. It costs a round trip and no body bytes at all, which is why newer readers skip it with
     * a suffix range ({@code Range: bytes=-8}), whose {@code Content-Range} header carries the total.
     */
    private long fetchObjectSize(RangeMetrics metrics) {
        if (metrics != null) {
            metrics.record(OBJECT_KEY, 0, 0, "object size (HEAD — a round trip, zero bytes)");
        }
        var response = cluster.tickUntilComplete(client.getObjectSize(OBJECT_KEY));
        assertTrue(response.success(), "size lookup failed: " + response.error());
        return response.size();
    }

    /** One explicit range GET against the erasure-coded store, recorded if metrics are supplied. */
    private byte[] fetch(ByteRange range, RangeMetrics metrics, String why) {
        if (metrics != null) {
            metrics.record(OBJECT_KEY, range.offset(), range.length(), why);
        }
        return cluster.tickUntilComplete(
                client.getObjectRange(OBJECT_KEY, range.offset(), range.length())).data();
    }

    /** Size, then footer length, then the footer itself — three round trips before any data. */
    private ObjectStoreParquetReader.FooterResult readFooter(RangeMetrics metrics) throws Exception {
        objectSize = fetchObjectSize(metrics);
        long size = objectSize;
        ByteRange hint = ObjectStoreParquetReader.footerLengthHintRange(size);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                size, fetch(hint, metrics, "footer length hint (last 8 bytes)"));
        return ObjectStoreParquetReader.readFooter(
                size, footerRange, fetch(footerRange, metrics, "footer metadata"));
    }

    /** An in-memory view of just the ranges named, as a task would assemble it. */
    private PrefetchedInputFile view(ObjectStoreParquetReader.FooterResult footer,
                                     List<Integer> rowGroups,
                                     RangeMetrics metrics) {
        PrefetchedInputFile file = PrefetchedInputFile.of(objectSize)
                .with(footer.footerStart(), footer.footerBytes());
        for (int index : rowGroups) {
            ByteRange range = ObjectStoreParquetReader.rowGroupRange(footer.metadata().getBlocks().get(index));
            file = file.with(range.offset(), fetch(range, metrics, "row group " + index));
        }
        return file;
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void fetchFooterOnlyReadsObjectTail() throws Exception {
        RangeMetrics metrics = new RangeMetrics();
        ParquetMetadata metadata = readFooter(metrics).metadata();

        assertNotNull(metadata);
        assertTrue(metadata.getBlocks().size() >= 3);
        assertEquals(3, metrics.totalRequests(),
                "size, footer length, footer — three round trips before a single data byte");
        assertTrue(metrics.totalBytesTransferred() < objectSize,
                "and still far less than the object");
    }

    @Test
    void readRowGroupsOverObjectStoreWithPushdown() throws Exception {
        RangeMetrics metrics = new RangeMetrics();

        // 1. Fetch footer metadata via targeted range read
        var footer = readFooter(metrics);

        // 2. Evaluate predicate: age >= 65 (skips 6 of 7 row groups)
        List<Integer> matchingGroups = RowGroupFilter.filterForMinInteger(
                footer.metadata().getBlocks(), "age", 65);
        assertEquals(List.of(5), matchingGroups, "Only Row Group 5 should match age >= 65");

        // 3. Fetch only the matching row group's bytes, then decode from memory
        List<TableRecord> seniorRecords = ObjectStoreParquetReader.readRowGroups(
                view(footer, matchingGroups, metrics), matchingGroups);
        assertEquals(7, seniorRecords.size());

        for (TableRecord rec : seniorRecords) {
            assertTrue(rec.getInteger("age") >= 55,
                    "Record " + rec.primaryKey() + " age should be in senior range, was " + rec.getInteger("age"));
        }

        // Metrics should reflect that row groups 0 and 1 were skipped!
        assertTrue(metrics.totalBytesTransferred() < objectSize);
    }

    @Test
    void fullFileReadFetchesAllRecords() throws Exception {
        var footer = readFooter(null);
        List<Integer> everyRowGroup = new ArrayList<>();
        for (int i = 0; i < footer.metadata().getBlocks().size(); i++) {
            everyRowGroup.add(i);
        }

        List<TableRecord> allRecords = ObjectStoreParquetReader.readRowGroups(
                view(footer, everyRowGroup, null), everyRowGroup);
        assertEquals(recordCount, allRecords.size());
    }
}
