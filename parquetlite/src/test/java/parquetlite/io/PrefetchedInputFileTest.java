package parquetlite.io;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link PrefetchedInputFile} is the read path a sparklite worker uses: fetch the ranges, then
 * decode from memory. The round-trip test is the one that matters — it proves footer bytes plus a
 * single row group's bytes are everything {@code ParquetFileReader} needs, which is what lets
 * {@code compute} make exactly one network call per partition.
 */
class PrefetchedInputFileTest {

    private byte[] parquetData;

    /** Stands in for an object store: every read is an explicit byte range. */
    private byte[] fetch(ByteRange range) {
        return Arrays.copyOfRange(parquetData, (int) range.offset(), (int) range.endExclusive());
    }

    private ObjectStoreParquetReader.FooterResult readFooter() throws Exception {
        long size = parquetData.length;
        ByteRange hint = ObjectStoreParquetReader.footerLengthHintRange(size);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(size, fetch(hint));
        return ObjectStoreParquetReader.readFooter(size, footerRange, fetch(footerRange));
    }

    @BeforeEach
    void setUp() throws Exception {
        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();
        parquetData = ParquetWriterHelper.writeToBytes(schema, customers, 512);
    }

    @Test
    void resolvesReadsToTheSliceContainingThePosition() throws Exception {
        PrefetchedInputFile file = PrefetchedInputFile.of(100)
                .with(0, new byte[]{1, 2, 3, 4})
                .with(50, new byte[]{9, 8, 7});

        try (var in = file.newStream()) {
            in.seek(1);
            byte[] head = new byte[3];
            in.readFully(head);
            assertArrayEquals(new byte[]{2, 3, 4}, head);

            in.seek(51);
            byte[] tail = new byte[2];
            in.readFully(tail);
            assertArrayEquals(new byte[]{8, 7}, tail);
        }
        assertEquals(100, file.getLength());
    }

    @Test
    void readingAnUnprefetchedOffsetFailsLoudly() throws Exception {
        PrefetchedInputFile file = PrefetchedInputFile.of(100)
                .with(0, new byte[]{1, 2, 3, 4});

        try (var in = file.newStream()) {
            in.seek(20);                                  // seeking anywhere is allowed
            IOException e = assertThrows(IOException.class, () -> in.readFully(new byte[4]));
            assertTrue(e.getMessage().contains("was not prefetched"), e.getMessage());
            assertTrue(e.getMessage().contains("[0, 4)"), e.getMessage());
        }
    }

    @Test
    void rejectsASliceThatDoesNotFitTheObject() {
        assertThrows(IllegalArgumentException.class,
                () -> PrefetchedInputFile.of(10).with(8, new byte[]{1, 2, 3, 4}));
    }

    /**
     * The invariant sparklite's {@code compute} depends on: one footer slice (shipped with the task)
     * plus one row group slice (fetched on the worker) is enough to decode that row group. Nothing
     * else in the file is touched.
     */
    @Test
    void decodesOneRowGroupFromFooterPlusThatRowGroupOnly() throws Exception {
        var footer = readFooter();

        List<BlockMetaData> blocks = footer.metadata().getBlocks();
        assertTrue(blocks.size() >= 2, "need multiple row groups to prove only one is read");

        for (int i = 0; i < blocks.size(); i++) {
            BlockMetaData block = blocks.get(i);
            byte[] rowGroupBytes = fetch(ObjectStoreParquetReader.rowGroupRange(block));

            PrefetchedInputFile in = PrefetchedInputFile.of(parquetData.length)
                    .with(footer.footerStart(), footer.footerBytes())
                    .with(block.getStartingPos(), rowGroupBytes);

            List<TableRecord> records = ObjectStoreParquetReader.readRowGroups(in, List.of(i));

            assertEquals(block.getRowCount(), records.size(),
                    "row group " + i + " should decode exactly its own rows");
            assertFalse(records.isEmpty());
        }
    }

    /** Reading beyond what was prefetched must fail rather than silently return the wrong rows. */
    @Test
    void decodingARowGroupThatWasNotFetchedFails() throws Exception {
        var footer = readFooter();
        BlockMetaData first = footer.metadata().getBlocks().get(0);

        PrefetchedInputFile onlyFirst = PrefetchedInputFile.of(parquetData.length)
                .with(footer.footerStart(), footer.footerBytes())
                .with(first.getStartingPos(), fetch(ObjectStoreParquetReader.rowGroupRange(first)));

        assertThrows(Exception.class,
                () -> ObjectStoreParquetReader.readRowGroups(onlyFirst, List.of(1)));
    }
}
