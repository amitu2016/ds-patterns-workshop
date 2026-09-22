package parquetlite.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility for writing Parquet files with controllable row group sizes.
 */
public class ParquetWriterHelper {

    public static final int DEFAULT_ROW_GROUP_SIZE = 512; // Small size forces multiple row groups

    /**
     * Writes records to a local Parquet file.
     */
    public static void writeParquetFile(java.nio.file.Path outputPath,
                                        TableSchema schema,
                                        List<TableRecord> records,
                                        int rowGroupSize) throws IOException {
        Configuration conf = new Configuration();
        MessageType parquetSchema = schema.toParquetSchema("record");
        Path hadoopPath = new Path(outputPath.toUri());

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(hadoopPath)
                .withType(parquetSchema)
                .withConf(conf)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(rowGroupSize)
                .withPageSize(256)
                .withDictionaryPageSize(256)
                .withMinRowCountForPageSizeCheck(5)
                .withMaxRowCountForPageSizeCheck(10)
                .build()) {

            for (TableRecord rec : records) {
                SimpleGroup group = new SimpleGroup(parquetSchema);
                for (String colName : schema.getColumnNames()) {
                    Object val = rec.get(colName);
                    if (val != null) {
                        TableSchema.ColumnType type = schema.getColumn(colName).getType();
                        switch (type) {
                            case STRING -> group.add(colName, val.toString());
                            case INTEGER -> group.add(colName, ((Number) val).intValue());
                            case LONG -> group.add(colName, ((Number) val).longValue());
                            case DOUBLE -> group.add(colName, ((Number) val).doubleValue());
                            case BOOLEAN -> group.add(colName, (Boolean) val);
                        }
                    }
                }
                writer.write(group);
            }
        }
    }

    /**
     * Writes records to an in-memory byte array using a temporary file.
     */
    public static byte[] writeToBytes(TableSchema schema,
                                      List<TableRecord> records,
                                      int rowGroupSize) throws IOException {
        File temp = File.createTempFile("parquet-write-", ".parquet");
        temp.delete();
        try {
            writeParquetFile(temp.toPath(), schema, records, rowGroupSize);
            return Files.readAllBytes(temp.toPath());
        } finally {
            temp.delete();
        }
    }

    /**
     * Generates a sample customer dataset partitioned into 3 logical age/key tiers:
     * - Tier 1: Young customers (C001 - C015, age 20-29)
     * - Tier 2: Mid-age customers (C016 - C030, age 35-45)
     * - Tier 3: Senior customers (C031 - C045, age 55-68)
     */
    public static List<TableRecord> createSampleCustomers() {
        List<TableRecord> list = new ArrayList<>();
        String[] cities = {"New York", "San Francisco", "Austin", "Portland", "Chicago", "Boston", "Seattle", "Miami"};

        // Tier 1: Young customers (ages 20-29) -> Row Group 0
        for (int i = 1; i <= 15; i++) {
            String id = String.format("C%03d", i);
            int age = 20 + (i % 10);
            list.add(new TableRecord(id, Map.of(
                    "id", id,
                    "name", "Customer_" + id,
                    "email", id.toLowerCase() + "@example.com",
                    "age", age,
                    "city", cities[i % cities.length]
            )));
        }

        // Tier 2: Mid-age customers (ages 35-45) -> Row Group 1
        for (int i = 16; i <= 30; i++) {
            String id = String.format("C%03d", i);
            int age = 35 + (i % 11);
            list.add(new TableRecord(id, Map.of(
                    "id", id,
                    "name", "Customer_" + id,
                    "email", id.toLowerCase() + "@example.com",
                    "age", age,
                    "city", cities[i % cities.length]
            )));
        }

        // Tier 3: Senior customers (ages 55-68) -> Row Group 2
        for (int i = 31; i <= 45; i++) {
            String id = String.format("C%03d", i);
            int age = 55 + (i % 14);
            list.add(new TableRecord(id, Map.of(
                    "id", id,
                    "name", "Customer_" + id,
                    "email", id.toLowerCase() + "@example.com",
                    "age", age,
                    "city", cities[i % cities.length]
            )));
        }

        return list;
    }
}
