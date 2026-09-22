package sparklite.demo;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.io.ByteRange;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;
import sparklite.parquet.ParquetPartition;
import sparklite.parquet.ParquetRDDLite;
import sparklite.protocol.RegisterWorkerRequest;
import sparklite.protocol.SubmitTaskRequest;
import sparklite.protocol.TaskLocality;
import sparklite.rdd.Partition;
import sparklite.scheduler.RDDLiteTask;
import sparklite.scheduler.TaskScheduler;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slide 4.3: Spark's Parquet Integration — Data Locality.
 *
 * <p>Demonstrates:
 * 1. <b>The without/with pattern</b>:
 *    - <b>Without Locality (ANY)</b>: Tasks scheduled on arbitrary or remote workers, incurring
 *      cross-network data transfer costs for every row group.
 *    - <b>With Locality (NODE_LOCAL)</b>: Tasks scheduled directly on workers co-located with
 *      the storage nodes holding the target row group / shards.
 * 2. How {@link TaskScheduler} inspects {@link Partition#index()} and preferred locations
 *    to schedule tasks onto local workers.
 *
 * <p>Run via: {@code make demo-4.3}
 */
@DisplayName("Demo 4.3: Spark Data Locality")
public class Demo_4_3_DataLocality {

    // TRY IT: change NUM_STORAGE_NODES or ROW_GROUP_SIZE to see how task placement shifts!
    static final int NUM_STORAGE_NODES = 3;
    static final int ROW_GROUP_SIZE = 512;

    /**
     * Where each row group's data sits, as the scheduler is told it.
     *
     * <p>Round-robin across the storage nodes, and a stand-in: real Spark <i>derives</i> this from
     * the filesystem. On HDFS a block's hosts come from the NameNode, and
     * {@code FilePartition.preferredLocations()} ranks them by how many of the partition's bytes
     * each holds, returning the top few. The list is plural because HDFS replicates -- three
     * DataNodes hold the same block, so any of them is local.
     *
     * <p>objectstorelite cannot supply it honestly: {@code ErasureSetMapper} puts one object on one
     * erasure set and stripes it across every node in that set, so all row groups of one file share
     * the same nodes and no partition's placement differs from another's. Real S3 is further still
     * -- it exposes no placement at all, so Spark on S3 gets empty preferred locations and runs
     * ANY. This models HDFS-style block placement: the scheduler logic it exercises is real, the
     * placement it is fed is not.
     */
    private static List<List<String>> placeRowGroups(int rowGroupCount) {
        List<List<String>> locations = new ArrayList<>(rowGroupCount);
        for (int i = 0; i < rowGroupCount; i++) {
            locations.add(List.of("storage-node-" + (i % NUM_STORAGE_NODES)));
        }
        return locations;
    }

    @Test
    void withoutDataLocality_tasksIncurRemoteNetworkTransfer() throws Exception {
        System.out.println("================================================================================");
        System.out.println("  DEMO 4.3 [PART 1]: Without Data Locality (Random / Remote Placement)          ");
        System.out.println("================================================================================");

        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();
        byte[] parquetBytes = ParquetWriterHelper.writeToBytes(schema, customers, ROW_GROUP_SIZE);

        java.util.function.Function<ByteRange, byte[]> fetch = r ->
                java.util.Arrays.copyOfRange(parquetBytes, (int) r.offset(), (int) r.endExclusive());

        // Scenario: Workers are located in a compute-only rack (remote-rack-A), separate from storage
        List<SubmitTaskRequest> dispatched = new ArrayList<>();
        TaskScheduler scheduler = new TaskScheduler((wId, req) -> dispatched.add(req));

        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-A", 4, "compute-rack-A"));
        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-B", 4, "compute-rack-A"));

        // Planning: read the footer here, on the driver side, where blocking is legitimate.
        ByteRange hintRange = ObjectStoreParquetReader.footerLengthHintRange(parquetBytes.length);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                parquetBytes.length, fetch.apply(hintRange));
        var footer = ObjectStoreParquetReader.readFooter(
                parquetBytes.length, footerRange, fetch.apply(footerRange));

        // One entry per row group, derived from the footer rather than written out by hand.
        // Seven hardcoded entries silently desynchronised from the data: raising ROW_GROUP_SIZE
        // collapsed this to a single task that still "passed", and lowering it broke the run --
        // from turning the TRY IT knob above, which is the one change it invites.
        List<List<String>> preferredLocations = placeRowGroups(footer.metadata().getBlocks().size());
        ParquetRDDLite rdd = new ParquetRDDLite("analytics/customers.parquet", parquetBytes.length,
                footer, preferredLocations);

        List<RDDLiteTask<TableRecord>> tasks = new ArrayList<>();
        for (int i = 0; i < rdd.getPartitions().length; i++) {
            tasks.add(new RDDLiteTask<>(100 + i, 1, i, rdd));
        }

        scheduler.submitTasks(tasks, rdd);

        System.out.println("📋 Scheduling Decision Table (WITHOUT Locality):");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("%-12s | %-16s | %-16s | %-12s | %-15s%n",
                "Task ID", "Preferred Node", "Assigned Worker", "Locality", "Network Cost");
        System.out.println("--------------------------------------------------------------------------------");

        int remoteTransfers = 0;
        long totalRemoteBytes = 0;
        for (RDDLiteTask<TableRecord> t : tasks) {
            ParquetPartition pp = (ParquetPartition) rdd.getPartitions()[t.getPartitionId()];
            TaskLocality loc = scheduler.getTaskLocality(t.getTaskId());
            String assigned = scheduler.getAssignedWorker(t.getTaskId());

            System.out.printf("Task %-7d | %-16s | %-16s | %-12s | %6d bytes (REMOTE)%n",
                    t.getTaskId(), pp.getPreferredLocations().get(0), assigned, loc, pp.getLength());

            assertEquals(TaskLocality.ANY, loc, "Without matching worker, locality must fall back to ANY");
            remoteTransfers++;
            totalRemoteBytes += pp.getLength();
        }
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("❌ Result: %d / %d tasks fell back to ANY.%n", remoteTransfers, tasks.size());
        System.out.printf("   Total cross-network data transferred: %d bytes%n%n", totalRemoteBytes);
    }

    @Test
    void withDataLocality_tasksScheduledOnStorageNodeWorkers() throws Exception {
        System.out.println("================================================================================");
        System.out.println("  DEMO 4.3 [PART 2]: With Data Locality (Co-located Node-Local Placement)       ");
        System.out.println("================================================================================");

        TableSchema schema = TableSchema.createCustomerSchema();
        List<TableRecord> customers = ParquetWriterHelper.createSampleCustomers();
        byte[] parquetBytes = ParquetWriterHelper.writeToBytes(schema, customers, ROW_GROUP_SIZE);

        java.util.function.Function<ByteRange, byte[]> fetch = r ->
                java.util.Arrays.copyOfRange(parquetBytes, (int) r.offset(), (int) r.endExclusive());

        // Scenario: Workers are co-located directly on the storage nodes!
        List<SubmitTaskRequest> dispatched = new ArrayList<>();
        TaskScheduler scheduler = new TaskScheduler((wId, req) -> dispatched.add(req));

        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-0", 4, "storage-node-0"));
        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-1", 4, "storage-node-1"));
        scheduler.handleWorkerRegistration(new RegisterWorkerRequest("worker-2", 4, "storage-node-2"));

        // Planning: read the footer here, on the driver side, where blocking is legitimate.
        ByteRange hintRange = ObjectStoreParquetReader.footerLengthHintRange(parquetBytes.length);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                parquetBytes.length, fetch.apply(hintRange));
        var footer = ObjectStoreParquetReader.readFooter(
                parquetBytes.length, footerRange, fetch.apply(footerRange));

        // One entry per row group, derived from the footer (see placeRowGroups).
        List<List<String>> preferredLocations = placeRowGroups(footer.metadata().getBlocks().size());
        ParquetRDDLite rdd = new ParquetRDDLite("analytics/customers.parquet", parquetBytes.length,
                footer, preferredLocations);

        List<RDDLiteTask<TableRecord>> tasks = new ArrayList<>();
        for (int i = 0; i < rdd.getPartitions().length; i++) {
            tasks.add(new RDDLiteTask<>(200 + i, 1, i, rdd));
        }

        scheduler.submitTasks(tasks, rdd);

        System.out.println("📋 Scheduling Decision Table (WITH Locality):");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("%-12s | %-16s | %-16s | %-12s | %-15s%n",
                "Task ID", "Preferred Node", "Assigned Worker", "Locality", "Network Cost");
        System.out.println("--------------------------------------------------------------------------------");

        int localTasks = 0;
        for (RDDLiteTask<TableRecord> t : tasks) {
            ParquetPartition pp = (ParquetPartition) rdd.getPartitions()[t.getPartitionId()];
            TaskLocality loc = scheduler.getTaskLocality(t.getTaskId());
            String assigned = scheduler.getAssignedWorker(t.getTaskId());

            System.out.printf("Task %-7d | %-16s | %-16s | %-12s | %s%n",
                    t.getTaskId(), pp.getPreferredLocations().get(0), assigned, loc, "0 bytes (LOCAL SHARD)");

            assertEquals(TaskLocality.NODE_LOCAL, loc, "Task must be placed NODE_LOCAL");
            localTasks++;
        }
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("✅ Result: %d / %d tasks achieved NODE_LOCAL locality!%n", localTasks, tasks.size());
        System.out.println("   Total cross-network data transferred: 0 bytes (Pure local disk/memory read!)");

        System.out.println("\n💡 Key Architectural Takeaways:");
        System.out.println("   1. Data Locality brings compute to the data, not data to the compute.");
        System.out.println("   2. NODE_LOCAL scheduling avoids saturating top-of-rack switches and network cards.");
        System.out.println("   3. In large-scale Parquet scans, locality reduces end-to-end query latency dramatically.");
        System.out.println("================================================================================\n");

        assertEquals(tasks.size(), localTasks);
    }
}
