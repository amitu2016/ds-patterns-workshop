package deltalite.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for reading and writing Parquet files.
 * Used primarily for checkpoint operations in Delta Lake.
 */
public class ParquetUtil {

    /**
     * Writes a list of records to a Parquet file.
     *
     * @param records the records to write
     * @param filePath the path to write to
     * @return the size of the file in bytes
     * @throws IOException if an I/O error occurs
     */
    public static long writeRecords(List<Map<String, String>> records, java.nio.file.Path filePath) throws IOException {
        if (records == null || records.isEmpty()) {
            return 0;
        }
        
        // Ensure the parent directory exists
        java.nio.file.Files.createDirectories(filePath.getParent());
        
        // Get all column names
        List<String> columnNames = extractColumnNames(records);
        
        // Create an Avro schema for the records
        Schema schema = createRecordsSchema(columnNames);
        
        // Create a Hadoop Path from the Java Path
        Path hadoopPath = new Path(filePath.toString());
        
        // Configure Hadoop to overwrite existing files
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");
        conf.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        
        // Delete the file if it exists to avoid FileAlreadyExistsException
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(hadoopPath)) {
            fs.delete(hadoopPath, true);
        }
        
        // Initialize the Parquet writer
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(hadoopPath)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(conf)
                .build()) {
            
            // Convert each record to an Avro record and write it
            for (Map<String, String> record : records) {
                GenericRecord avroRecord = convertMapToRecord(record, schema);
                writer.write(avroRecord);
            }
        }
        
        // Return the file size
        return java.nio.file.Files.size(filePath);
    }
    
    /**
     * Reads records from a Parquet file with optional filtering.
     *
     * @param filePath the path to read from
     * @param filterPredicate optional filter predicate to apply to the records
     * @return a list of records, where each record is a map of column names to values
     * @throws IOException if an I/O error occurs
     */
    public static List<Map<String, String>> readRecords(java.nio.file.Path filePath, FilterPredicate filterPredicate) throws IOException {
        if (!java.nio.file.Files.exists(filePath)) {
            return new ArrayList<>();
        }
        
        List<Map<String, String>> records = new ArrayList<>();
        
        // Create a Hadoop Path from the Java Path
        Path hadoopPath = new Path(filePath.toString());
        
        // FilterCompat.get(null) throws, so an unfiltered read must not call withFilter at
        // all. Predicate pushdown is opt-in: with no predicate the reader scans every row
        // group, which is exactly the "before" half of demo 3.5.
        var builder = AvroParquetReader
                .<GenericRecord>builder(hadoopPath)
                .withConf(new Configuration());
        if (filterPredicate != null) {
            builder = builder.withFilter(FilterCompat.get(filterPredicate));
        }

        try (ParquetReader<GenericRecord> reader = builder.build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Map<String, String> map = convertRecordToMap(record);
                records.add(map);
            }
        }
        
        return records;
    }
    
    /**
     * Reads records from a Parquet file without filtering.
     *
     * @param filePath the path to read from
     * @return a list of records, where each record is a map of column names to values
     * @throws IOException if an I/O error occurs
     */
    public static List<Map<String, String>> readRecords(java.nio.file.Path filePath) throws IOException {
        return readRecords(filePath, null);
    }
    
    /**
     * Creates an Avro schema for records based on column names.
     *
     * @param columnNames the list of column names
     * @return the schema
     */
    private static Schema createRecordsSchema(List<String> columnNames) {
        StringBuilder fieldsJson = new StringBuilder();
        
        for (int i = 0; i < columnNames.size(); i++) {
            fieldsJson.append("{\"name\": \"")
                      .append(columnNames.get(i))
                      .append("\", \"type\": [\"null\", \"string\"], \"default\": null}");
            
            if (i < columnNames.size() - 1) {
                fieldsJson.append(",\n");
            }
        }
        
        String schemaJson = 
            "{\"namespace\": \"com.example.deltajava\",\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"RecordData\",\n" +
            " \"fields\": [\n" +
            fieldsJson.toString() +
            " ]\n" +
            "}";
        
        return new Schema.Parser().parse(schemaJson);
    }
    
    /**
     * Converts a Map to an Avro GenericRecord.
     *
     * @param map the map to convert
     * @param schema the Avro schema to use
     * @return the record
     */
    private static GenericRecord convertMapToRecord(Map<String, String> map, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);
        
        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            String value = map.get(fieldName);
            if (value != null) {
                record.put(fieldName, value);
            } else {
                record.put(fieldName, null);
            }
        }
        
        return record;
    }
    
    /**
     * Converts an Avro GenericRecord to a Map.
     *
     * @param record the record to convert
     * @return the map
     */
    private static Map<String, String> convertRecordToMap(GenericRecord record) {
        Map<String, String> map = new HashMap<>();
        
        for (Schema.Field field : record.getSchema().getFields()) {
            String fieldName = field.name();
            Object value = record.get(fieldName);
            if (value != null) {
                map.put(fieldName, value.toString());
            }
        }
        
        return map;
    }
    
    /**
     * Extracts all column names from the records.
     *
     * @param records the records to extract column names from
     * @return a list of column names
     */
    private static List<String> extractColumnNames(List<Map<String, String>> records) {
        return records.stream()
                .flatMap(record -> record.keySet().stream())
                .distinct()
                .collect(Collectors.toList());
    }
} 