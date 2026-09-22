package sparklite.demo;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.io.PrefetchedInputFile;
import parquetlite.io.ByteRange;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;
import sparklite.parquet.ParquetPartition;
import sparklite.parquet.ParquetRDDLite;
import sparklite.rdd.Partition;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slide 4.1: Parquet to RDD Mapping.
 *
 * <p>Demonstrates:
 * 1. How Apache Spark maps columnar storage to distributed partitions.
 * 2. <b>One Parquet Row Group → One RDDLite Partition</b>.
 * 3. Inspecting the file footer metadata to extract row counts and byte boundaries
 *    without reading full row data.
 *
 * <p>Run via: {@code make demo-4.1}
 */
@DisplayName("Demo 4.1: Parquet to RDD Mapping")
public class Demo_4_1_ParquetToRddMapping {

    // TRY IT: change ROW_GROUP_SIZE to 1024 or 2048 to see the RDD partition count change!
    static final int ROW_GROUP_SIZE = 512;

    @Test
    void parquetRowGroupsMapToOneToOneWithRDDPartitions(@TempDir Path tempDir) throws Exception {
        System.out.println("================================================================================");
        System.out.println("  DEMO 4.1: Parquet to RDD Mapping (One Row Group → One RDD Partition)          ");
        System.out.println("================================================================================");

        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();

        System.out.printf("Creating Parquet dataset: %d customer records (target row group: %d bytes)...%n",
                customers.size(), ROW_GROUP_SIZE);

        byte[] parquetBytes = ParquetWriterHelper.writeToBytes(schema, customers, ROW_GROUP_SIZE);
        System.out.printf("Generated Parquet file: %d bytes total%n%n", parquetBytes.length);

        // Stands in for an object store: every read below is an explicit byte range.
        java.util.function.Function<ByteRange, byte[]> fetch = r ->
                java.util.Arrays.copyOfRange(parquetBytes, (int) r.offset(), (int) r.endExclusive());

        // Step 1: Read footer metadata
        System.out.println("🔍 Step 1: Querying Parquet Footer from end of file...");
        // Two round trips: the tail says how long the footer is, then the footer itself.
        ByteRange hintRange = ObjectStoreParquetReader.footerLengthHintRange(parquetBytes.length);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                parquetBytes.length, fetch.apply(hintRange));
        var footerResult = ObjectStoreParquetReader.readFooter(
                parquetBytes.length, footerRange, fetch.apply(footerRange));
        ParquetMetadata metadata = footerResult.metadata();
        List<BlockMetaData> blocks = metadata.getBlocks();

        System.out.printf("Parquet footer reports %d Row Groups across %d total rows.%n%n",
                blocks.size(), metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum());

        // Step 2: Construct ParquetRDDLite
        System.out.println("🚀 Step 2: Instantiating ParquetRDDLite...");
        ParquetRDDLite rdd = new ParquetRDDLite(
                "customers.parquet", parquetBytes.length, footerResult, List.of());

        Partition[] partitions = rdd.getPartitions();
        System.out.printf("ParquetRDDLite generated %d partitions.%n%n", partitions.length);

        // Step 3: Print mapping table
        System.out.println("📋 Step 3: Row Group to RDD Partition Mapping Table:");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("%-15s | %-15s | %-12s | %-20s | %-10s%n",
                "RDD Partition", "Parquet Block", "Row Count", "Byte Range", "Length");
        System.out.println("--------------------------------------------------------------------------------");

        long totalPartitionRows = 0;
        for (int i = 0; i < partitions.length; i++) {
            ParquetPartition pp = (ParquetPartition) partitions[i];
            BlockMetaData block = blocks.get(i);
            totalPartitionRows += pp.getRowCount();

            System.out.printf("Partition %-5d | Row Group %-5d | %-12d | [%6d .. %6d] | %6d bytes%n",
                    pp.index(),
                    pp.getRowGroupIndex(),
                    pp.getRowCount(),
                    pp.getStartOffset(),
                    pp.getStartOffset() + pp.getLength(),
                    pp.getLength()
            );

            assertEquals(i, pp.getRowGroupIndex());
            assertEquals(block.getRowCount(), pp.getRowCount());
            assertEquals(block.getStartingPos(), pp.getStartOffset());
        }
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Total RDD Records across partitions: %d rows%n%n", totalPartitionRows);

        // Step 4: read one partition the way a task does — fetch exactly its byte range, then
        // decode from memory. On a worker the fetch is ObjectStoreClient.getObjectRange(); here it
        // is the local fetcher, but the ranges and the decode are identical.
        System.out.println("⚡ Step 4: Reading one partition the way a task reads it...");
        ParquetPartition target = (ParquetPartition) partitions[0];

        ByteRange rowGroupRange = new ByteRange(target.getStartOffset(), (int) target.getLength());
        byte[] rowGroupBytes = fetch.apply(rowGroupRange);

        PrefetchedInputFile taskView = PrefetchedInputFile.of(parquetBytes.length)
                .with(footerResult.footerStart(), footerResult.footerBytes())   // travels with the task
                .with(target.getStartOffset(), rowGroupBytes);                  // fetched by the task

        List<TableRecord> partitionRecords = ObjectStoreParquetReader.readRowGroups(
                taskView, List.of(target.getRowGroupIndex()));

        long bytesRead = footerResult.footerBytes().length + rowGroupBytes.length;
        System.out.printf("Partition %d read %d of %d bytes (%.1f%% of the file) and decoded %d rows.%n",
                target.index(), bytesRead, parquetBytes.length,
                100.0 * bytesRead / parquetBytes.length, partitionRecords.size());

        assertEquals(target.getRowCount(), partitionRecords.size(),
                "a partition decodes exactly its own row group");
        assertTrue(bytesRead < parquetBytes.length,
                "a task must never need the whole file to read one partition");

        System.out.println("\n💡 Key Architectural Takeaways:");
        System.out.println("   1. Parquet's physical chunking (Row Groups) directly defines Spark's parallelism.");
        System.out.println("   2. Each task reads ONLY its assigned byte range in object storage.");
        System.out.println("   3. No task coordinates or waits on another task's byte range.");
        System.out.println("================================================================================\n");

        assertEquals(blocks.size(), partitions.length, "Each row group must map to exactly one partition");
        assertEquals(customers.size(), totalPartitionRows, "Partitions must cover every record exactly once");
    }
}
