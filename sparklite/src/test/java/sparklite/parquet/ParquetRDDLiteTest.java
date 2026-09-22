package sparklite.parquet;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.io.ByteRange;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;
import sparklite.rdd.Partition;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers the planning half of {@link ParquetRDDLite}: footer metadata in, partitions out.
 *
 * <p>The decode half is covered by {@code parquetlite}'s {@code PrefetchedInputFileTest}, and the
 * end-to-end read through an injected {@code ObjectStoreClient} by demo 4.2. This test deliberately
 * does not run {@code compute}: a task reads through a client supplied by a worker, and there is no
 * worker here.
 */
class ParquetRDDLiteTest {

    private static final String KEY = "analytics/customers.parquet";

    private byte[] parquetBytes;
    private ObjectStoreParquetReader.FooterResult footer;

    private byte[] fetch(ByteRange range) {
        return java.util.Arrays.copyOfRange(parquetBytes, (int) range.offset(), (int) range.endExclusive());
    }

    @BeforeEach
    void setUp() throws Exception {
        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> sampleCustomers = ParquetWriterHelper.createSampleCustomers();
        // 512 bytes per row group creates multiple row groups
        parquetBytes = ParquetWriterHelper.writeToBytes(schema, sampleCustomers, 512);

        ByteRange hintRange = ObjectStoreParquetReader.footerLengthHintRange(parquetBytes.length);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                parquetBytes.length, fetch(hintRange));
        footer = ObjectStoreParquetReader.readFooter(
                parquetBytes.length, footerRange, fetch(footerRange));
    }

    @Test
    void oneRowGroupBecomesOnePartition() {
        ParquetRDDLite rdd = new ParquetRDDLite(KEY, parquetBytes.length, footer, List.of());

        List<BlockMetaData> blocks = footer.metadata().getBlocks();
        assertTrue(blocks.size() >= 2, "sample data should span multiple row groups");
        assertEquals(blocks.size(), rdd.getPartitions().length);

        for (int i = 0; i < blocks.size(); i++) {
            ParquetPartition p = (ParquetPartition) rdd.getPartitions()[i];
            assertEquals(i, p.index());
            assertEquals(i, p.getRowGroupIndex());
            assertEquals(blocks.get(i).getStartingPos(), p.getStartOffset());
            assertEquals(blocks.get(i).getCompressedSize(), p.getLength(),
                    "a partition's length is the row group's on-disk size, not its uncompressed size");
        }
    }

    @Test
    void preferredLocationsAreCarriedPerRowGroup() {
        List<List<String>> locations = List.of(List.of("storage-node-0"), List.of("storage-node-1"));
        ParquetRDDLite rdd = new ParquetRDDLite(KEY, parquetBytes.length, footer, locations);

        Partition[] partitions = rdd.getPartitions();
        assertEquals(List.of("storage-node-0"), rdd.getPreferredLocations(partitions[0]));
        assertEquals(List.of("storage-node-1"), rdd.getPreferredLocations(partitions[1]));
        // Row groups beyond the supplied list get no preference rather than a wrong one.
        assertTrue(rdd.getPreferredLocations(partitions[partitions.length - 1]).isEmpty()
                || partitions.length <= 2);
    }

    @Test
    void declaresTheClientTypeAWorkerMustInject() {
        ParquetRDDLite rdd = new ParquetRDDLite(KEY, parquetBytes.length, footer, List.of());
        assertEquals(objectstorelite.client.ObjectStoreClient.class, rdd.clientType());
    }

    /**
     * Without an injected client the read must fail loudly rather than reaching for some other
     * connection — the whole point of the transient field. Mirrors Spark's {@code RDD.sc}, which
     * throws "This RDD lacks a SparkContext" when touched on an executor.
     */
    @Test
    void computingWithoutAnInjectedClientFailsLoudly() {
        ParquetRDDLite rdd = new ParquetRDDLite(KEY, parquetBytes.length, footer, List.of());

        var future = rdd.compute(rdd.getPartitions()[0]);

        assertTrue(future.isFailed(), "compute must fail, not hang or return empty");
        assertInstanceOf(IllegalStateException.class, future.getException());
        assertTrue(future.getException().getMessage().contains("No ObjectStoreClient set"),
                future.getException().getMessage());
    }
}
