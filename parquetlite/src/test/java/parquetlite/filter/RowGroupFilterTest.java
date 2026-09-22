package parquetlite.filter;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.file.ParquetFooterReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RowGroupFilterTest {

    @TempDir
    Path tempDir;

    private ParquetMetadata metadata;

    @BeforeEach
    void setUp() throws IOException {
        Path parquetFile = tempDir.resolve("customers.parquet");
        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();

        // Write with small row group size to force 3 row groups (5 customers each)
        ParquetWriterHelper.writeParquetFile(parquetFile, schema, customers, 512);
        metadata = ParquetFooterReader.readFooter(parquetFile);
    }

    @Test
    void verifiesMultiRowGroupCreation() {
        System.out.println(ParquetFooterReader.formatFooterDump(metadata));
        assertTrue(metadata.getBlocks().size() >= 3,
                "Expected at least 3 row groups, got: " + metadata.getBlocks().size());
    }

    @Test
    void filterForMinInteger() {
        List<BlockMetaData> blocks = metadata.getBlocks();

        // Query age >= 65: only Row Group 5 has max 68 >= 65. All other 6 row groups skipped!
        List<Integer> selectedSenior = RowGroupFilter.filterForMinInteger(blocks, "age", 65);
        assertEquals(1, selectedSenior.size(), "Only Row Group 5 should match age >= 65");
        assertEquals(5, selectedSenior.get(0));

        // Query age >= 60: Row Group 4 (max 62) and Row Group 5 (max 68) match.
        List<Integer> selected60 = RowGroupFilter.filterForMinInteger(blocks, "age", 60);
        assertEquals(2, selected60.size());
        assertTrue(selected60.contains(4));
        assertTrue(selected60.contains(5));
    }

    @Test
    void filterForPointLookup() {
        List<BlockMetaData> blocks = metadata.getBlocks();

        // C002 is in Row Group 0 (C001-C007)
        List<Integer> c002Groups = RowGroupFilter.filterForPointLookup(blocks, "id", "C002");
        assertEquals(1, c002Groups.size());
        assertEquals(0, c002Groups.get(0));

        // C038 is in Row Group 5 (C036-C042)
        List<Integer> c038Groups = RowGroupFilter.filterForPointLookup(blocks, "id", "C038");
        assertEquals(1, c038Groups.size());
        assertEquals(5, c038Groups.get(0));
    }

    /** Opens the file the way a local reader does — parquet-mr handling the file, not a byte array. */
    private List<TableRecord> readRowGroups(List<Integer> rowGroups) throws IOException {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(tempDir.resolve("customers.parquet").toUri()),
                new Configuration());
        return ObjectStoreParquetReader.readRowGroups(inputFile, rowGroups);
    }

    /**
     * Statistics say which row groups <i>could</i> match; only reading them says whether they do.
     *
     * <p>Asserting the selected index alone would pass just as happily if the filter returned the
     * wrong group, or if min/max statistics disagreed with the rows behind them. This reads the
     * bytes parquet-mr decodes and checks the rows themselves — and checks the skipped groups
     * really held nothing that qualified, which is the half a false negative would hide.
     */
    @Test
    void theSelectedRowGroupActuallyHoldsTheMatchingRows() throws IOException {
        List<BlockMetaData> blocks = metadata.getBlocks();
        int threshold = 65;

        List<Integer> selected = RowGroupFilter.filterForMinInteger(blocks, "age", threshold);
        assertEquals(List.of(5), selected);

        List<TableRecord> fromSelected = readRowGroups(selected);
        assertFalse(fromSelected.isEmpty(), "the selected row group must contain rows");
        assertTrue(fromSelected.stream().anyMatch(r -> r.getInteger("age") >= threshold),
                "the selected row group must actually contain a qualifying row, not merely permit one");
        assertEquals(blocks.get(5).getRowCount(), fromSelected.size(),
                "reading one row group must return exactly that row group's rows");

        // The other side of the claim: nothing was wrongly skipped.
        List<Integer> skipped = new ArrayList<>();
        for (int i = 0; i < blocks.size(); i++) {
            if (!selected.contains(i)) {
                skipped.add(i);
            }
        }
        List<TableRecord> fromSkipped = readRowGroups(skipped);
        assertTrue(fromSkipped.stream().allMatch(r -> r.getInteger("age") < threshold),
                "a skipped row group must hold no row that would have qualified");

        assertEquals(ParquetWriterHelper.createSampleCustomers().size(),
                fromSelected.size() + fromSkipped.size(),
                "selected plus skipped must account for every row exactly once");
    }

    /** A point lookup must land on the row group that really holds the key. */
    @Test
    void thePointLookupRowGroupActuallyHoldsTheKey() throws IOException {
        List<Integer> groups = RowGroupFilter.filterForPointLookup(metadata.getBlocks(), "id", "C038");
        assertEquals(List.of(5), groups);

        List<TableRecord> rows = readRowGroups(groups);
        assertTrue(rows.stream().anyMatch(r -> "C038".equals(r.getString("id"))),
                "the row group chosen by min/max must contain the key it was chosen for");
    }

    @Test
    void cleanStringDecodesHexProperly() {
        assertEquals("Alice", RowGroupFilter.cleanString("Alice"));
        assertNull(RowGroupFilter.cleanString(null));
        // "0x416c696365" is ASCII "Alice"
        assertEquals("Alice", RowGroupFilter.cleanString("0x416c696365"));
    }
}
