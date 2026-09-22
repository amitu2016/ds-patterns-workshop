package sparklite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import objectstorelite.client.ObjectStoreClient;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.server.StorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import parquetlite.file.ParquetWriterHelper;
import parquetlite.file.ObjectStoreParquetReader;
import parquetlite.io.ByteRange;
import parquetlite.schema.TableRecord;
import parquetlite.schema.TableSchema;
import sparklite.context.SparkLiteContext;
import sparklite.context.SparkLiteDriver;
import sparklite.parquet.ParquetRDDLite;
import sparklite.rdd.RDDLite;
import sparklite.worker.SparkLiteWorker;
import sparklite.worker.ClientRegistry;

import com.tickloom.future.TickCompletableFuture;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slide 4.2: End-to-End Spark + Parquet Execution.
 *
 * <p>Demonstrates:
 * 1. Distributed DAG execution: Driver builds {@code ResultStage} from RDD lineage.
 * 2. TaskScheduler dispatches tasks across multiple workers over tickloom's {@link com.tickloom.messaging.MessageBus}.
 * 3. Workers execute narrow transformations (Filter & Map) on Parquet row groups and return results.
 * 4. Driver collects results from all partitions.
 *
 * <p>Run via: {@code make demo-4.2}
 */
@DisplayName("Demo 4.2: End-to-End Spark + Parquet Execution")
public class Demo_4_2_EndToEndExecution {

    // TRY IT: change MIN_AGE to 50 or 25, or change NUM_WORKERS to 2 or 5!
    static final int MIN_AGE = 40;
    static final int NUM_WORKERS = 3;

    private static final int DATA_SHARDS = 4;
    private static final int PARITY_SHARDS = 2;
    private static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;
    private static final String S3_KEY = "analytics/customers.parquet";

    @TempDir
    Path clusterStorageRoot;

    private Cluster cluster;
    private ObjectStoreClient storageClient;
    private SparkLiteDriver driver;
    private SparkLiteContext sc;
    private byte[] parquetBytes;
    /**
     * The driver's own read path. Blocking here is legitimate — this runs on the test thread, not
     * inside a tick. A worker cannot do this, which is why a task reads through an injected client
     * and returns its future instead.
     */
    private byte[] fetch(ByteRange range) throws IOException {
        var response = cluster.tickUntilComplete(
                storageClient.getObjectRange(S3_KEY, range.offset(), range.length()));
        if (!response.success()) {
            throw new IOException("Failed to fetch " + range + " from objectstorelite: " + response.error());
        }
        return response.data();
    }
    private final Map<ProcessId, SparkLiteWorker> workers = new HashMap<>();
    private List<TableRecord> sampleCustomers;

    @BeforeEach
    void setUp() throws Exception {
        // 1. Prepare Parquet dataset (45 records across multiple row groups)
        TableSchema schema = TableSchema.createCustomerSchema();
        sampleCustomers = ParquetWriterHelper.createSampleCustomers();
        parquetBytes = ParquetWriterHelper.writeToBytes(schema, sampleCustomers, 512);

        // 2. Storage nodes for objectstorelite
        List<ProcessId> storageNodeIds = new ArrayList<>();
        List<Path> storagePaths = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            ProcessId id = ProcessId.of("storage-node-" + i);
            storageNodeIds.add(id);
            storagePaths.add(clusterStorageRoot.resolve("disk-node-" + i));
        }

        ErasureSet erasureSet = new ErasureSet(0, storageNodeIds);
        ErasureSetMapper mapper = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.CRCMOD, null, List.of(erasureSet));

        // 3. Spark processes
        ProcessId driverId = ProcessId.of("spark-driver");
        List<ProcessId> workerIds = new ArrayList<>();
        for (int i = 0; i < NUM_WORKERS; i++) {
            workerIds.add(ProcessId.of("spark-worker-" + i));
        }

        List<ProcessId> allProcesses = new ArrayList<>();
        allProcesses.addAll(storageNodeIds);
        allProcesses.add(driverId);
        allProcesses.addAll(workerIds);


        // 4. Boot unified tickloom cluster
        cluster = new Cluster()
                .withProcessIds(allProcesses)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    String name = params.id().name();
                    if (name.startsWith("storage-node-")) {
                        int idx = storageNodeIds.indexOf(params.id());
                        return new StorageNode(peerIds, params, storagePaths.get(idx), mapper, DATA_SHARDS, PARITY_SHARDS);
                    } else if (name.equals("spark-driver")) {
                        return new SparkLiteDriver(params);
                    } else {
                        int workerIdx = workerIds.indexOf(params.id());
                        String host = "host-" + (workerIdx % TOTAL_SHARDS);
                        SparkLiteWorker worker = new SparkLiteWorker(params, driverId, host, 2);
                        workers.put(params.id(), worker);
                        return worker;
                    }
                })
                .start();

        // 5. Store file in objectstorelite
        storageClient = cluster.newClient(
                ProcessId.of("storage-admin"),
                (peers, params) -> new ObjectStoreClient(peers, params, storageNodeIds.get(0))
        );

        // 5a. Each worker gets its own object-store client — this is what a task reads through.
        //     It must happen after build().start(), because newClient needs the built cluster.
        for (ProcessId workerId : workerIds) {
            ObjectStoreClient workerClient = cluster.newClientConnectedTo(
                    ProcessId.of(workerId.name() + "-s3"),
                    storageNodeIds.get(0),
                    (peers, params) -> new ObjectStoreClient(peers, params, storageNodeIds.get(0)));
            workers.get(workerId).attach(new ClientRegistry().with(ObjectStoreClient.class, workerClient));
        }

        var putFuture = storageClient.putObject(S3_KEY, parquetBytes);
        cluster.tickUntil(putFuture::isCompleted);

        // 6. Connect driver and wait for worker registrations
        driver = cluster.getNode(driverId);
        sc = new SparkLiteContext(driver);

        cluster.tickUntil(() -> driver.getTaskScheduler().getNumWorkers() >= NUM_WORKERS);
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void executeEndToEndQueryAcrossWorkers() throws Exception {
        System.out.println("================================================================================");
        System.out.println("  DEMO 4.2: End-to-End Spark + Parquet Execution (DAG → Stages → Tasks)         ");
        System.out.println("================================================================================");

        System.out.printf("Cluster initialized: 1 SparkLiteDriver + %d SparkLiteWorkers + %d Storage Nodes%n",
                NUM_WORKERS, TOTAL_SHARDS);
        System.out.printf("Dataset: s3://default/%s (%d bytes, %d records)%n",
                S3_KEY, parquetBytes.length, sampleCustomers.size());
        System.out.printf("Query:   parquetRDD.filter(age >= %d).map(formatSummary)%n%n", MIN_AGE);

        // Step 1: Create ParquetRDDLite
        System.out.println("📦 Step 1: Creating ParquetRDDLite from object storage...");
        // Planning, on the driver: two round trips to learn the row group layout.
        ByteRange hintRange = ObjectStoreParquetReader.footerLengthHintRange(parquetBytes.length);
        ByteRange footerRange = ObjectStoreParquetReader.footerRange(
                parquetBytes.length, fetch(hintRange));
        var footer = ObjectStoreParquetReader.readFooter(
                parquetBytes.length, footerRange, fetch(footerRange));
        ParquetRDDLite parquetRDD = sc.parquetFile(S3_KEY, parquetBytes.length, footer);
        System.out.printf("   Created %d partitions (1 partition per row group)%n%n",
                parquetRDD.getPartitions().length);

        // Step 2: Define Transformation Lineage (Lazy)
        System.out.println("🔗 Step 2: Defining transformation lineage (Filter -> Map)...");
        RDDLite<String> formattedRDD = parquetRDD
                .filter(record -> ((Integer) record.values().get("age")) >= MIN_AGE)
                .map(record -> String.format("%s | Age: %2d | %s | %s",
                        record.values().get("id"),
                        record.values().get("age"),
                        record.values().get("name"),
                        record.values().get("city")));

        // Step 3: Trigger Job Submission (Action)
        System.out.println("🚀 Step 3: Submitting job to DAGScheduler...");
        TickCompletableFuture<List<String>> jobFuture = sc.runJob(formattedRDD);

        // Advance simulation ticks until execution finishes across workers
        cluster.tickUntil(jobFuture::isCompleted);

        assertTrue(jobFuture.isCompleted());
        List<String> results = jobFuture.getResult();

        // Step 4: Display Task Distribution and Results
        System.out.println("\n📊 Step 4: Worker Task Distribution & Execution Breakdown:");
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("%-20s | %-15s%n", "Worker Process", "Tasks Executed");
        System.out.println("--------------------------------------------------------------------------------");

        Map<String, Integer> taskCounts = driver.getTaskScheduler().getWorkerTaskCounts();
        int totalTasksAssigned = 0;
        for (var entry : taskCounts.entrySet()) {
            System.out.printf("%-20s | %-15d%n", entry.getKey(), entry.getValue());
            totalTasksAssigned += entry.getValue();
        }
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Total Stage Tasks: %d across %d partitions%n%n", totalTasksAssigned, parquetRDD.getPartitions().length);

        System.out.println("📄 Step 5: Collected Results (First 10 sample records):");
        System.out.println("--------------------------------------------------------------------------------");
        results.stream().limit(10).forEach(r -> System.out.println("   " + r));
        if (results.size() > 10) {
            System.out.printf("   ... and %d more matching records%n", results.size() - 10);
        }
        System.out.println("--------------------------------------------------------------------------------");
        System.out.printf("Total matching records: %d / %d total%n%n", results.size(), sampleCustomers.size());

        System.out.println("💡 Key Architectural Takeaways:");
        System.out.println("   1. DAGScheduler decomposed the RDD pipeline into a single ResultStage.");
        System.out.println("   2. Tasks were distributed concurrently across tickloom worker processes via MessageBus.");
        System.out.println("   3. Each worker read its assigned Parquet Row Group directly via range reads.");
        System.out.println("================================================================================\n");

        long expectedMatching = sampleCustomers.stream()
                .filter(c -> ((Integer) c.values().get("age")) >= MIN_AGE)
                .count();

        assertEquals(expectedMatching, results.size());
        assertEquals(parquetRDD.getPartitions().length, totalTasksAssigned);
    }
}
