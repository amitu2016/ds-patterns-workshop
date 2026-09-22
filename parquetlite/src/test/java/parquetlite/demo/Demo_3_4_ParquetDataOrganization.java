package parquetlite.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import parquetlite.filter.RowGroupFilter;
import parquetlite.io.ByteRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.file.ParquetFooterReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slide 3.4: Parquet Data Organization: Row Groups & Column Chunks.
 *
 * <pre>
 *                                  PARQUET FILE
 *  +==========================================================================+
 *  | 4-byte Magic: "PAR1"                                                     |
 *  +==========================================================================+
 *  | ROW GROUP 0  (Horizontal Cut: Rows 0..6)                                 |
 *  |   +------------------------------------+                                 |
 *  |   | Column Chunk: id   [C001..C007]    | ---+                            |
 *  |   +------------------------------------+    |                            |
 *  |   | Column Chunk: name                 |    | Vertical Cut               |
 *  |   +------------------------------------+    | (Column Projection)        |
 *  |   | Column Chunk: email                |    | Readers fetch only the     |
 *  |   +------------------------------------+    | columns needed by query    |
 *  |   | Column Chunk: age                  |    |                            |
 *  |   +------------------------------------+    |                            |
 *  |   | Column Chunk: city                 | ---+                            |
 *  |   +------------------------------------+                                 |
 *  +--------------------------------------------------------------------------+
 *  | ROW GROUP 1  (Horizontal Cut: Rows 7..13)                                |
 *  |   +------------------------------------+                                 |
 *  |   | Column Chunk: id   [C008..C014]    |                                 |
 *  |   +------------------------------------+                                 |
 *  |   | Column Chunk: name                 |                                 |
 *  |   +------------------------------------+                                 |
 *  |   | ... (chunks for all columns)       |                                 |
 *  +--------------------------------------------------------------------------+
 *  | ... (additional row groups)                                              |
 *  +==========================================================================+
 *  | FOOTER METADATA (Written last at EOF)                                    |
 *  |   - File Schema: leaf column types & repetition                          |
 *  |   - Row group offsets, sizes, row counts                                 |
 *  |   - Per-chunk min/max statistics & encodings                             |
 *  +--------------------------------------------------------------------------+
 *  | 4-byte Footer Length (little-endian)                                     |
 *  +--------------------------------------------------------------------------+
 *  | 4-byte Magic: "PAR1"                                                     |
 *  +==========================================================================+
 *
 *  QUERY FLOW: Point Lookup (e.g., id = 'C019', select 'name')
 *
 *    1. Read File Tail           2. Evaluate Statistics        3. Fetch Column Chunks
 *   +-------------------+       +-----------------------+     +-----------------------+
 *   | Read last 8 bytes |  ==>  | Check id statistics   | ==> | In surviving group 2, |
 *   | for length hint,  |       | for each row group:   |     | read ONLY id & name   |
 *   | then fetch footer |       |   RG 0: [C001..C007]  |     | chunks. Other columns |
 *   | metadata.         |       |   RG 1: [C008..C014]  |     | are never fetched.    |
 *   +-------------------+       |   RG 2: [C015..C021]  |     +-----------------------+
 *                               |         -> MATCH      |
 *                               +-----------------------+
 * </pre>
 *
 * <p>Key Takeaways:
 * <ul>
 *   <li><b>Row Groups (Horizontal Cut):</b> Sized in bytes (default 128MB). Unit of distributed work (1 row group = 1 Spark partition).</li>
 *   <li><b>Column Chunks (Vertical Cut):</b> Every row group contains chunks for every column. Queries fetch only requested column chunks.</li>
 *   <li><b>Footer at End:</b> Streaming writers cannot compute offsets or min/max statistics until data is written, so metadata is stored at the tail.</li>
 * </ul>
 *
 * <p>Run via: {@code make demo-3.4}
 */
@DisplayName("Demo 3.4: Parquet Data Organization")
public class Demo_3_4_ParquetDataOrganization {

    // TRY IT: change ROW_GROUP_SIZE to 1024 or 2048 and watch the row-group count fall.
    //         Note it is a target size in BYTES, not a row count — Parquet closes a row group
    //         once the buffered data reaches it, so the count depends on how well the data
    //         compresses. Large enough, and the whole file becomes a single row group, which
    //         is why a badly chosen size destroys predicate pushdown: one group means nothing
    //         can be skipped.
    static final int ROW_GROUP_SIZE = 512;

    /** TRY IT: any id from C001 to C045 — watch which row group the statistics pick. */
    static final String WANTED_CUSTOMER = "C019";

    /** The column the lookup searches on, and the one field it actually wants. */
    private static final String KEY_COLUMN = "id";
    private static final String WANTED_COLUMN = "name";

    /** Everything else stays on disk. This list is the projection, in both step 4b and 4c. */
    private static final List<String> PROJECTED_COLUMNS = List.of(KEY_COLUMN, WANTED_COLUMN);

    /** The column step 3 follows through the file. Any column would show the same shape. */
    private static final String TRACKED_COLUMN = "age";

    @Test
    void inspectParquetFooterAndRowGroups(@TempDir Path tempDir) throws Exception {
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();
        printTitle(customers.size());

        Path parquetFile = writeParquetFile(tempDir, customers);
        FileLayout layout = FileLayout.readFrom(parquetFile);

        printFooterDump(layout);
        printEveryRowGroupHoldsEveryColumn(layout);
        printWhereOneColumnLives(layout, TRACKED_COLUMN);
        printPerColumnTotals(layout);

        PointLookup lookup = readOneCustomersName(layout, WANTED_CUSTOMER);

        printKeyTakeaways();

        assertLookupFoundTheRightCustomer(lookup);
        assertEveryRowGroupCarriesEveryColumn(layout);
    }

    // ===============================================================================
    // The file under inspection
    // ===============================================================================

    private static void printTitle(int recordCount) {
        System.out.println("=".repeat(80));
        System.out.println("  DEMO 3.4: Parquet Data Organization (Row Groups, Columns & Statistics)        ");
        System.out.println("=".repeat(80));
        System.out.printf("Creating Parquet file on disk with %d records (target row group size: %d bytes)...%n%n",
                recordCount, ROW_GROUP_SIZE);
    }

    private static Path writeParquetFile(Path directory, List<TableRecord> customers) throws IOException {
        Path parquetFile = directory.resolve("customers.parquet");
        ParquetWriterHelper.writeParquetFile(
                parquetFile, TableSchema.createCustomerSchema(), customers, ROW_GROUP_SIZE);
        return parquetFile;
    }

    // ===============================================================================
    // Step 1: the footer
    // ===============================================================================

    private static void printFooterDump(FileLayout layout) {
        System.out.println("Step 1: Reading footer from end of file...");
        System.out.println(ParquetFooterReader.formatFooterDump(layout.metadata()));
    }

    // ===============================================================================
    // Step 2: the invariant the dump shows but does not state
    // ===============================================================================

    private static void printEveryRowGroupHoldsEveryColumn(FileLayout layout) {
        System.out.println("Step 2: Every row group carries a chunk for EVERY column");
        System.out.printf("   %d row group%s × %d columns = %d column chunks in this file.%n",
                layout.rowGroupCount(), layout.isSingleRowGroup() ? "" : "s",
                layout.columnCount(), layout.rowGroupCount() * layout.columnCount());
        System.out.println("   No row group holds a subset of the schema. Columns are not spread across");
        System.out.println("   row groups — what is spread is each column's data.\n");
    }

    // ===============================================================================
    // Step 3: follow one column through the file, so the scatter is visible
    // ===============================================================================

    private static void printWhereOneColumnLives(FileLayout layout, String column) {
        System.out.printf("Step 3: Where column '%s' actually lives%n", column);
        for (int i = 0; i < layout.rowGroupCount(); i++) {
            ColumnChunkMetaData chunk = layout.chunk(i, column);
            System.out.printf("   row group %d: offset %-6d size %-5d%n",
                    i, chunk.getStartingPos(), chunk.getTotalSize());
        }
        if (layout.isSingleRowGroup()) {
            printWhyOnePieceIsATradeOff(layout, column);
        } else {
            printWhyManyPiecesCost(layout, column);
        }
    }

    private static void printWhyOnePieceIsATradeOff(FileLayout layout, String column) {
        System.out.printf("   → %d bytes of '%s' data, in one piece: at this ROW_GROUP_SIZE the whole%n",
                layout.totalBytesOf(column), column);
        System.out.println("     file is a single row group. Reads are maximally sequential — and nothing");
        System.out.println("     can ever be skipped, because skipping is per row group. That is the");
        System.out.println("     trade-off the size knob controls.\n");
    }

    private static void printWhyManyPiecesCost(FileLayout layout, String column) {
        System.out.printf("   → %d bytes of '%s' data, in %d pieces at %d different offsets.%n",
                layout.totalBytesOf(column), column, layout.rowGroupCount(), layout.rowGroupCount());
        System.out.printf("   Reading this column end to end is %d reads, not one. Bigger row groups%n",
                layout.rowGroupCount());
        System.out.println("   mean fewer and more sequential reads — the argument for a 128MB target.\n");
    }

    private static void printPerColumnTotals(FileLayout layout) {
        System.out.println("   Per-column totals across the whole file:");
        for (String column : layout.columnNames()) {
            System.out.printf("      %-6s %-8s %d chunks, %d bytes%n",
                    column, layout.repetitionOf(column), layout.rowGroupCount(), layout.totalBytesOf(column));
        }
        System.out.println();
    }

    // ===============================================================================
    // Step 4: the read path a Spark task takes — footer, row group, column chunk, value
    // ===============================================================================

    private static PointLookup readOneCustomersName(FileLayout layout, String customerId) throws Exception {
        System.out.printf("Step 4: Reading one value — %s's %s — without reading the file%n%n",
                customerId, WANTED_COLUMN);

        List<Integer> candidates = narrowToRowGroupsThatCanContain(layout, customerId);
        int rowGroupToRead = candidates.get(0);
        long projectedBytes = announceTheChunksWorthFetching(layout, rowGroupToRead);
        TableRecord row = decodeOnlyTheProjectedColumns(layout, rowGroupToRead, customerId);

        reportWhatEachLayerCost(layout, rowGroupToRead, projectedBytes);
        explainHowSparkRunsThis();

        return new PointLookup(customerId, candidates, rowGroupToRead, row);
    }

    /**
     * (a) Statistics prove a row group <i>cannot</i> contain the key, never that it does. So this
     * narrows the search and does not end it — the survivor is still scanned row by row in (c).
     */
    private static List<Integer> narrowToRowGroupsThatCanContain(FileLayout layout, String key) {
        System.out.println("   (a) Which row groups could hold it? Ask the footer statistics.");
        List<Integer> candidates = RowGroupFilter.filterForPointLookup(
                layout.rowGroups(), KEY_COLUMN, key);

        for (int i = 0; i < layout.rowGroupCount(); i++) {
            System.out.printf("       row group %d: %s in [%s]  %s%n",
                    i, KEY_COLUMN, layout.statisticsRangeOf(i, KEY_COLUMN),
                    candidates.contains(i) ? "← could contain it" : "ruled out, unread");
        }
        printHowMuchWasRuledOut(layout, candidates);
        return candidates;
    }

    private static void printHowMuchWasRuledOut(FileLayout layout, List<Integer> candidates) {
        if (candidates.size() < layout.rowGroupCount()) {
            System.out.printf("       → %d of %d row groups survive. Everything else is skipped unread.%n%n",
                    candidates.size(), layout.rowGroupCount());
            return;
        }
        System.out.printf("       → all %d row group%s survive: with this ROW_GROUP_SIZE the statistics%n",
                layout.rowGroupCount(), layout.isSingleRowGroup() ? "" : "s");
        System.out.println("         rule nothing out, so the whole file must be decoded. Coarser groups");
        System.out.println("         mean wider min/max brackets and less to skip.\n");
    }

    /** (b) Inside the surviving row group, only the projected columns are worth a request. */
    private static long announceTheChunksWorthFetching(FileLayout layout, int rowGroupIndex) {
        System.out.println("   (b) Which bytes inside that row group? Ask for two chunks, not the group.");
        long projectedBytes = 0;
        for (String column : PROJECTED_COLUMNS) {
            ByteRange range = layout.chunkRange(rowGroupIndex, column);
            projectedBytes += range.length();
            System.out.printf("       %-4s chunk: bytes %d..%d (%d bytes)%n",
                    column, range.offset(), range.offset() + range.length() - 1, range.length());
        }
        System.out.printf("       the other %d columns of row group %d are never touched.%n%n",
                layout.columnCount() - PROJECTED_COLUMNS.size(), rowGroupIndex);
        return projectedBytes;
    }

    /** (c) Decode the projected chunks, then find the row the old-fashioned way. */
    private static TableRecord decodeOnlyTheProjectedColumns(FileLayout layout, int rowGroupIndex, String key)
            throws IOException {
        System.out.println("   (c) Decode those chunks and find the row.");
        MessageType projection = projectionOf(layout.schema(), PROJECTED_COLUMNS);
        List<TableRecord> rows = readColumnsFromRowGroup(layout.file(), rowGroupIndex, projection);
        TableRecord found = rowWithKey(rows, KEY_COLUMN, key);

        System.out.printf("       row group %d decoded to %d rows of %s.%n",
                rowGroupIndex, rows.size(), PROJECTED_COLUMNS);
        System.out.printf("       → %s is %s%n%n", key, found.values().get(WANTED_COLUMN));
        return found;
    }

    private static void reportWhatEachLayerCost(FileLayout layout, int rowGroupIndex, long projectedBytes)
            throws IOException {
        System.out.println("   What each layer cost:");
        System.out.printf("       whole file          %6d bytes%n", layout.sizeOnDisk());
        System.out.printf("       one row group       %6d bytes   ← what a Spark task is handed%n",
                layout.rowGroups().get(rowGroupIndex).getCompressedSize());
        System.out.printf("       %d column chunks     %6d bytes   ← what the task actually decodes%n",
                PROJECTED_COLUMNS.size(), projectedBytes);
        System.out.println("   Row groups narrow which rows; column chunks narrow which fields. Two");
        System.out.println("   independent cuts, and a query uses both.\n");
    }

    private static void explainHowSparkRunsThis() {
        System.out.println("   In Spark this is one task: FileScanRDD hands a partition one row group,");
        System.out.println("   and the projection pushed down from the query decides which chunks inside");
        System.out.println("   it get decoded. Nothing about the two cuts is Spark-specific — they are");
        System.out.println("   properties of the file, which is why any engine can parallelise over it.\n");
    }

    private static void printKeyTakeaways() {
        System.out.println("Key Takeaways for Distributed Processing:");
        System.out.println("   1. The footer is at the END because offsets and statistics are not known");
        System.out.println("      until the data is written — which is why reading costs two round trips.");
        System.out.println("   2. Row groups cut horizontally and are sized in BYTES, not rows. One row");
        System.out.println("      group is a contiguous byte range, hence one range read, hence one task.");
        System.out.println("   3. Column chunks cut vertically. Every row group holds one chunk per column,");
        System.out.println("      which is what makes encoding, projection and min/max statistics possible.");
        System.out.println("   4. So projecting k of N columns costs k range requests PER ROW GROUP.");
        System.out.println("   5. Query engines read the FOOTER FIRST to decide which row groups to skip.");
        System.out.println("=".repeat(80) + "\n");
    }

    // ===============================================================================
    // What must be true however the knobs are set
    // ===============================================================================

    private static void assertLookupFoundTheRightCustomer(PointLookup lookup) {
        assertEquals("Customer_" + lookup.customerId(), lookup.foundName(),
                "the projected read must return the same name as a full read");
    }

    /**
     * Deliberately not asserting a row group count: ROW_GROUP_SIZE is a byte target, so the number
     * depends on compression, and pinning it to a magic number would break the TRY IT above. What
     * does hold at every size is that no row group ever carries a subset of the schema.
     */
    private static void assertEveryRowGroupCarriesEveryColumn(FileLayout layout) {
        assertTrue(layout.rowGroupCount() >= 1, "expected at least one row group");
        for (int i = 0; i < layout.rowGroupCount(); i++) {
            assertEquals(layout.schema().getFieldCount(), layout.rowGroups().get(i).getColumns().size(),
                    "row group " + i + " must carry one chunk for every column in the schema");
        }
    }

    // ===============================================================================
    // Reading Parquet: the small pieces the steps above are written in terms of
    // ===============================================================================

    /** What the footer says about the file, in the shape the steps above ask questions in. */
    private record FileLayout(Path file, ParquetMetadata metadata, MessageType schema,
                              List<BlockMetaData> rowGroups, List<String> columnNames) {

        static FileLayout readFrom(Path file) throws IOException {
            ParquetMetadata metadata = ParquetFooterReader.readFooter(file);
            List<String> columnNames = metadata.getBlocks().get(0).getColumns().stream()
                    .map(chunk -> chunk.getPath().toDotString())
                    .toList();
            return new FileLayout(file, metadata, metadata.getFileMetaData().getSchema(),
                    metadata.getBlocks(), columnNames);
        }

        int rowGroupCount() {
            return rowGroups.size();
        }

        int columnCount() {
            return columnNames.size();
        }

        boolean isSingleRowGroup() {
            return rowGroups.size() == 1;
        }

        ColumnChunkMetaData chunk(int rowGroupIndex, String column) {
            return rowGroups.get(rowGroupIndex).getColumns().stream()
                    .filter(c -> c.getPath().toDotString().equals(column))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no chunk for column " + column));
        }

        /** The bytes one column occupies inside one row group — the smallest thing worth fetching. */
        ByteRange chunkRange(int rowGroupIndex, String column) {
            ColumnChunkMetaData chunk = chunk(rowGroupIndex, column);
            return new ByteRange(chunk.getStartingPos(), (int) chunk.getTotalSize());
        }

        /** One column's data is not in one place: this is the sum of its chunks, group by group. */
        long totalBytesOf(String column) {
            long total = 0;
            for (int i = 0; i < rowGroupCount(); i++) {
                total += chunk(i, column).getTotalSize();
            }
            return total;
        }

        String repetitionOf(String column) {
            return schema.getType(column).getRepetition().name().toLowerCase();
        }

        String statisticsRangeOf(int rowGroupIndex, String column) {
            Statistics<?> statistics = chunk(rowGroupIndex, column).getStatistics();
            return RowGroupFilter.cleanString(statistics.minAsString())
                    + ".." + RowGroupFilter.cleanString(statistics.maxAsString());
        }

        long sizeOnDisk() throws IOException {
            return Files.size(file);
        }
    }

    /** The outcome of step 4, so the assertions can check it without re-reading anything. */
    private record PointLookup(String customerId, List<Integer> candidateRowGroups,
                               int rowGroupRead, TableRecord row) {

        String foundName() {
            return String.valueOf(row.values().get(WANTED_COLUMN));
        }
    }

    /** The schema a reader asks for, which is how column projection is expressed to parquet-mr. */
    private static MessageType projectionOf(MessageType fullSchema, List<String> columns) {
        return new MessageType(fullSchema.getName(),
                columns.stream().map(fullSchema::getType).toList());
    }

    /**
     * Decodes one row group, reading only the columns in {@code projection}. The chunks for every
     * other column stay on disk: {@code setRequestedSchema} is what makes that true, and it is the
     * same call Spark makes when a query selects a subset of columns.
     */
    private static List<TableRecord> readColumnsFromRowGroup(Path file, int rowGroupIndex, MessageType projection)
            throws IOException {
        HadoopInputFile inputFile = HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(file.toUri()), new Configuration());

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            reader.setRequestedSchema(projection);
            return decodeRows(reader.readRowGroup(rowGroupIndex), projection);
        }
    }

    private static List<TableRecord> decodeRows(PageReadStore pages, MessageType projection) {
        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projection);
        RecordReader<Group> recordReader =
                columnIO.getRecordReader(pages, new GroupRecordConverter(projection));

        List<TableRecord> rows = new ArrayList<>();
        for (long i = 0; i < pages.getRowCount(); i++) {
            rows.add(toTableRecord(recordReader.read(), projection));
        }
        return rows;
    }

    private static TableRecord toTableRecord(Group group, MessageType projection) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int field = 0; field < projection.getFieldCount(); field++) {
            values.put(projection.getFieldName(field), readField(group, projection, field));
        }
        return new TableRecord(String.valueOf(values.get(KEY_COLUMN)), values);
    }

    /** One decoded field, typed by what the schema says it is. */
    private static Object readField(Group group, MessageType schema, int fieldIndex) {
        return switch (schema.getType(fieldIndex).asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> group.getInteger(fieldIndex, 0);
            case INT64 -> group.getLong(fieldIndex, 0);
            case DOUBLE -> group.getDouble(fieldIndex, 0);
            case BOOLEAN -> group.getBoolean(fieldIndex, 0);
            default -> group.getString(fieldIndex, 0);
        };
    }

    /** The linear scan inside the surviving row group — statistics narrowed it, they did not find it. */
    private static TableRecord rowWithKey(List<TableRecord> rows, String keyColumn, String key) {
        return rows.stream()
                .filter(row -> key.equals(row.values().get(keyColumn)))
                .findFirst()
                .orElseThrow(() -> new AssertionError(
                        "row group survived the statistics filter but does not contain " + key));
    }
}
