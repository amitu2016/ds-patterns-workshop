package parquetlite.file;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import parquetlite.io.PrefetchedInputFile;
import parquetlite.io.ByteRange;
import parquetlite.schema.TableRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * High-level reader implementing the Explicit Two-Phase Range Read workflow over object storage.
 */
public class ObjectStoreParquetReader {

    /**
     * Reads all records from specified row groups of an {@link InputFile}.
     */
    public static List<TableRecord> readRowGroups(InputFile inputFile, List<Integer> rowGroupIndices) throws IOException {
        List<TableRecord> records = new ArrayList<>();

        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

            for (int idx : rowGroupIndices) {
                PageReadStore pageStore = reader.readRowGroup(idx);
                if (pageStore == null) {
                    continue;
                }

                RecordReader<Group> recordReader = columnIO.getRecordReader(pageStore, new GroupRecordConverter(schema));
                long rowCount = reader.getFooter().getBlocks().get(idx).getRowCount();

                for (long r = 0; r < rowCount; r++) {
                    Group group = recordReader.read();
                    if (group != null) {
                        Map<String, Object> values = new LinkedHashMap<>();
                        for (int f = 0; f < schema.getFieldCount(); f++) {
                            String fName = schema.getFieldName(f);
                            if (group.getFieldRepetitionCount(f) > 0) {
                                values.put(fName, extractFieldValue(group, f));
                            }
                        }
                        String pk = values.containsKey("id") ? values.get("id").toString() : "row-" + records.size();
                        records.add(new TableRecord(pk, values));
                    }
                }
            }
        }

        return records;
    }

    /**
     * Reads all records from all row groups in the file.
     */
    public static List<TableRecord> readAllRecords(InputFile inputFile) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            int blockCount = reader.getFooter().getBlocks().size();
            List<Integer> allIndices = new ArrayList<>();
            for (int i = 0; i < blockCount; i++) {
                allIndices.add(i);
            }
            return readRowGroups(inputFile, allIndices);
        }
    }

    public record FooterResult(ParquetMetadata metadata, byte[] footerBytes, long footerStart) {}

    /**
     * The trailing bytes every Parquet reader starts from: a 4-byte little-endian footer length
     * followed by the {@code PAR1} magic.
     */
    public static final int FOOTER_LENGTH_HINT_BYTES = 8;

    // ---------------------------------------------------------------------------------------
    // Explicit range reads.
    //
    // Reading a Parquet footer takes two round trips -- the tail tells you how long the footer is,
    // and only then can you ask for it -- so the protocol is exposed as ranges the caller fetches
    // rather than hidden behind a fetch callback. Nothing below performs I/O, which is what lets a
    // sparklite worker (which cannot block on a TickCompletableFuture) share this code with a
    // driver that can.
    // ---------------------------------------------------------------------------------------

    /** Round trip 1: the object's last {@value #FOOTER_LENGTH_HINT_BYTES} bytes. */
    public static ByteRange footerLengthHintRange(long fileSize) {
        if (fileSize < FOOTER_LENGTH_HINT_BYTES) {
            throw new IllegalArgumentException(
                    "object of " + fileSize + " bytes is too small to be a Parquet file");
        }
        return new ByteRange(fileSize - FOOTER_LENGTH_HINT_BYTES, FOOTER_LENGTH_HINT_BYTES);
    }

    /**
     * Round trip 2: the exact footer bytes, decoded from what round trip 1 returned. The range
     * covers the Thrift metadata plus the length field and magic, so it is self-describing.
     */
    public static ByteRange footerRange(long fileSize, byte[] lengthHintBytes) {
        if (lengthHintBytes == null || lengthHintBytes.length < FOOTER_LENGTH_HINT_BYTES) {
            throw new IllegalArgumentException(
                    "expected " + FOOTER_LENGTH_HINT_BYTES + " bytes from footerLengthHintRange");
        }
        int footerLen = ByteBuffer.wrap(lengthHintBytes, 0, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
        int totalFooterBytes = footerLen + FOOTER_LENGTH_HINT_BYTES;
        return new ByteRange(fileSize - totalFooterBytes, totalFooterBytes);
    }

    /**
     * Parses the metadata from the bytes of {@link #footerRange}. Performs no I/O.
     *
     * <p>A footer-only view is a prefetched range like any other: exactly one part of the object is
     * present. If {@code ParquetFileReader} ever reached outside it, that would mean
     * {@link #footerRange} computed the wrong bounds, and {@link PrefetchedInputFile} says so
     * rather than failing somewhere less obvious.
     */
    public static FooterResult readFooter(long fileSize, ByteRange footerRange, byte[] footerBytes)
            throws IOException {
        PrefetchedInputFile footerInput = PrefetchedInputFile.of(fileSize)
                .with(footerRange.offset(), footerBytes);
        return new FooterResult(
                ParquetFooterReader.readFooter(footerInput), footerBytes, footerRange.offset());
    }

    /**
     * The bytes one row group occupies on disk — what a task fetches to compute its partition.
     *
     * <p><b>Compressed, not total.</b> {@code getTotalByteSize()} is parquet-mr's name for the
     * <i>uncompressed</i> size of the column chunks; {@code getCompressedSize()} is what is
     * actually stored. Using the former asks for more bytes than the row group occupies, and with
     * a well-compressing column it asks for bytes past the end of the file.
     */
    public static ByteRange rowGroupRange(BlockMetaData block) {
        return new ByteRange(block.getStartingPos(), (int) block.getCompressedSize());
    }

    private static Object extractFieldValue(Group group, int fieldIndex) {
        Type fieldType = group.getType().getType(fieldIndex);
        if (fieldType.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName typeName = fieldType.asPrimitiveType().getPrimitiveTypeName();
            return switch (typeName) {
                case INT32 -> group.getInteger(fieldIndex, 0);
                case INT64 -> group.getLong(fieldIndex, 0);
                case DOUBLE -> group.getDouble(fieldIndex, 0);
                case FLOAT -> group.getFloat(fieldIndex, 0);
                case BOOLEAN -> group.getBoolean(fieldIndex, 0);
                case BINARY -> group.getString(fieldIndex, 0);
                default -> group.getValueToString(fieldIndex, 0);
            };
        }
        return group.getValueToString(fieldIndex, 0);
    }
}
