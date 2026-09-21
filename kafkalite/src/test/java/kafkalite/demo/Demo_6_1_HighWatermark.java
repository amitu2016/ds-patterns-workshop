package kafkalite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kafkalite.admin.CreateTopicCommand;
import kafkalite.api.FetchIsolation;
import kafkalite.api.Message;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.server.BrokerServer;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SLIDE: Writes above the high-watermark   (demo 6.1)
 * SLIDE: Assignment: Replication & High-Watermark
 *
 * <p>Demonstrates why replication alone is not enough for consistency without the High Watermark:
 * <ol>
 *   <li>Leader accepts write and appends to local disk log.</li>
 *   <li>Followers fetch asynchronously over the network.</li>
 *   <li><b>The danger:</b> If a consumer reads uncommitted data before followers replicate it,
 *       and the leader crashes, the new leader has not seen that data. The record disappears from
 *       history — a dirty read / phantom read anomaly!</li>
 *   <li><b>The solution:</b> High Watermark (HW). The leader tracks the log end offset (LEO) of all
 *       in-sync replicas. HW advances to the minimum offset replicated across all ISRs.
 *       Consumers reading with {@link FetchIsolation#HIGH_WATERMARK} are strictly confined to offsets &le; HW.</li>
 * </ol>
 *
 * <p><b>The without/with pair:</b>
 * <ul>
 *   <li>{@link #withoutHighWatermark_uncommittedDataDisappears()}: consumer reads at log end without HW;
 *       leader crashes before replication; record is permanently lost from the surviving broker.</li>
 *   <li>{@link #withHighWatermark_readBlockedUntilReplicated()}: consumer read with HW returns empty until
 *       replication completes; after HW advances, data is durable across leader failure.</li>
 * </ul>
 */
class Demo_6_1_HighWatermark extends ZookeeperTestHarness {

    private static final ProcessId BROKER_1 = ProcessId.of("broker-1");
    private static final ProcessId BROKER_2 = ProcessId.of("broker-2");
    private static final TopicAndPartition ORDERS_0 = new TopicAndPartition("orders", 0);

    @Test
    @DisplayName("6.1 (without) · reading uncommitted log end causes dirty reads; record vanishes when leader crashes")
    void withoutHighWatermark_uncommittedDataDisappears() throws Exception {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 6.1 (WITHOUT): UNCOMMITTED DATA DISAPPEARS ON LEADER FAILURE");
        System.out.println("=".repeat(72));

        Map<ProcessId, Config> configs = configsFor(BROKER_1, BROKER_2);

        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    return new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                })
                .start()) {

            cluster.tickUntil(cluster::areAllNodesInitialized);

            BrokerServer broker1 = cluster.getNode(BROKER_1);
            BrokerServer broker2 = cluster.getNode(BROKER_2);

            // Step 1: Create topic 'orders' with 1 partition and RF=2
            System.out.println("\n--- Step 1: Topic 'orders' created (RF=2, replicas: [1, 2]) ---");
            CreateTopicCommand createTopicCommand = new CreateTopicCommand(
                    broker1.zookeeper(),
                    new kafkalite.common.ReplicaAssigner(new java.util.Random() {
                        @Override
                        public int nextInt(int bound) {
                            return 0;
                        }
                    })
            );
            createTopicCommand.createTopic("orders", 1, 2);

            cluster.tickUntil(() -> broker1.isLeaderFor(ORDERS_0)
                    && broker2.followerPartitions().contains(ORDERS_0));

            System.out.printf("  Leader: Broker %d (HW=%d, LEO=%d)\n",
                    broker1.config().getBrokerId(), broker1.highWatermarkFor(ORDERS_0), broker1.lastOffsetFor(ORDERS_0));
            System.out.printf("  Follower: Broker %d (HW=%d, LEO=%d)\n",
                    broker2.config().getBrokerId(), broker2.highWatermarkFor(ORDERS_0), broker2.lastOffsetFor(ORDERS_0));

            // Step 2: Producer sends a message to Leader (Broker 1)
            System.out.println("\n--- Step 2: Producer appends 'order-100' to Leader (Broker 1) ---");
            Message orderMessage = new Message("order-100", "Alice: $500 - Stock Purchase");
            long offset = broker1.append(ORDERS_0, orderMessage);
            assertEquals(1, offset);

            System.out.printf("  Broker 1 appended offset %d (LEO=%d, HW=%d)\n",
                    offset, broker1.lastOffsetFor(ORDERS_0), broker1.highWatermarkFor(ORDERS_0));
            System.out.printf("  Broker 2 has NOT replicated yet (LEO=%d, HW=%d)\n",
                    broker2.lastOffsetFor(ORDERS_0), broker2.highWatermarkFor(ORDERS_0));

            // Step 3: WITHOUT High Watermark: Consumer reads with LOG_END
            System.out.println("\n--- Step 3: Consumer reads with LOG_END (WITHOUT High Watermark) ---");
            List<Message> dirtyReadRecords = broker1.read(ORDERS_0, 1, FetchIsolation.LOG_END);
            assertFalse(dirtyReadRecords.isEmpty(), "Consumer read uncommitted record directly from log end!");
            System.out.printf("  ⚠️ DIRTY READ: Consumer received uncommitted record: %s -> %s\n",
                    dirtyReadRecords.get(0).keyAsString(), dirtyReadRecords.get(0).valueAsString());

            // Step 4: Leader (Broker 1) crashes before follower replicates
            System.out.println("\n--- Step 4: Broker 1 crashes before Broker 2 replicates ---");
            broker1.close();
            System.out.println("  💥 Broker 1 halted unexpectedly (power loss / network isolation)!");

            // Step 5: Consumer checks surviving Broker 2
            System.out.println("\n--- Step 5: Consumer tries to read from surviving Broker 2 ---");
            List<Message> survivingRecords = broker2.read(ORDERS_0, 1, FetchIsolation.LOG_END);
            System.out.printf("  Surviving Broker 2 log end offset: %d\n", broker2.lastOffsetFor(ORDERS_0));
            System.out.printf("  Records found at offset 1: %s\n", survivingRecords);

            assertTrue(survivingRecords.isEmpty(),
                    "Broker 2 never replicated the record! The data consumer saw has vanished!");

            System.out.println("""

                    ── what failed ──
                    The consumer read record 'order-100' using LOG_END isolation before replicas acknowledged it.
                    When Broker 1 crashed, Broker 2 had LEO=0.
                    The order processed by the consumer was completely lost from the cluster!
                    This is why Kafka enforces the HIGH_WATERMARK isolation boundary.
                    """);
        }
    }

    @Test
    @DisplayName("6.1 (with) · High Watermark blocks consumer reads until replicated; ensures zero data loss")
    void withHighWatermark_readBlockedUntilReplicated() throws Exception {
        System.out.println("\n" + "=".repeat(72));
        System.out.println(" DEMO 6.1 (WITH): HIGH WATERMARK PROTECTS AGAINST UNCOMMITTED READS");
        System.out.println("=".repeat(72));

        Map<ProcessId, Config> configs = configsFor(BROKER_1, BROKER_2);

        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    BrokerServer brokerServer = new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                    return brokerServer;
                })
                .start()) {

            cluster.tickUntil(cluster::areAllNodesInitialized);

            BrokerServer broker1 = cluster.getNode(BROKER_1);
            BrokerServer broker2 = cluster.getNode(BROKER_2);

            // Step 1: Create topic 'orders'
            System.out.println("\n--- Step 1: Topic 'orders' created (RF=2, replicas: [1, 2]) ---");
            CreateTopicCommand createTopicCommand = new CreateTopicCommand(
                    broker1.zookeeper(),
                    new kafkalite.common.ReplicaAssigner(new java.util.Random() {
                        @Override
                        public int nextInt(int bound) {
                            return 0;
                        }
                    })
            );
            createTopicCommand.createTopic("orders", 1, 2);

            cluster.tickUntil(() -> broker1.isLeaderFor(ORDERS_0)
                    && broker2.followerPartitions().contains(ORDERS_0));

            // Step 2: Producer appends to Leader
            System.out.println("\n--- Step 2: Producer appends 'order-100' to Leader (Broker 1) ---");
            Message orderMessage = new Message("order-100", "Alice: $500 - Stock Purchase");
            long offset = broker1.append(ORDERS_0, orderMessage);
            assertEquals(1, offset);

            System.out.printf("  Broker 1 appended offset %d (LEO=%d, HW=%d)\n",
                    offset, broker1.lastOffsetFor(ORDERS_0), broker1.highWatermarkFor(ORDERS_0));

            // Step 3: WITH High Watermark: Consumer attempts to read
            System.out.println("\n--- Step 3: Consumer reads with HIGH_WATERMARK isolation ---");
            List<Message> uncommitted = broker1.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);
            assertTrue(uncommitted.isEmpty(),
                    "Consumer read must be blocked/empty until the record is committed past High Watermark!");
            System.out.printf("  🛡️ SAFE: Read returned 0 records because offset 1 > High Watermark (%d)\n",
                    broker1.highWatermarkFor(ORDERS_0));

            // Step 4: Followers fetch over MessageBus across ticks
            System.out.println("\n--- Step 4: Replicas fetch over MessageBus (SimulatedNetwork) ---");
            cluster.tickUntil(() -> {
                return broker2.lastOffsetFor(ORDERS_0) == 1
                        && broker1.highWatermarkFor(ORDERS_0) == 1
                        && broker2.highWatermarkFor(ORDERS_0) == 1;
            });

            System.out.printf("  Replication complete over MessageBus!\n");
            System.out.printf("  Leader Broker 1:   LEO=%d, HW=%d\n",
                    broker1.lastOffsetFor(ORDERS_0), broker1.highWatermarkFor(ORDERS_0));
            System.out.printf("  Follower Broker 2: LEO=%d, HW=%d\n",
                    broker2.lastOffsetFor(ORDERS_0), broker2.highWatermarkFor(ORDERS_0));

            // Step 5: Consumer reads again with HIGH_WATERMARK
            System.out.println("\n--- Step 5: Consumer reads committed data past High Watermark ---");
            List<Message> committed = broker1.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);
            assertEquals(1, committed.size());
            assertEquals("order-100", committed.get(0).keyAsString());
            System.out.printf("  ✅ COMMITTED READ: Consumer received safely replicated record: %s -> %s\n",
                    committed.get(0).keyAsString(), committed.get(0).valueAsString());

            // Step 6: Verify fault tolerance on leader crash
            System.out.println("\n--- Step 6: Broker 1 crashes; surviving Broker 2 retains committed data ---");
            broker1.close();
            List<Message> replicaRecords = broker2.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);
            assertEquals(1, replicaRecords.size());
            assertEquals("order-100", replicaRecords.get(0).keyAsString());
            System.out.printf("  ✅ SURVIVING REPLICA: Broker 2 has committed record: %s -> %s\n",
                    replicaRecords.get(0).keyAsString(), replicaRecords.get(0).valueAsString());

            System.out.println("""

                    ── what succeeded ──
                    With HIGH_WATERMARK isolation:
                      1. The uncommitted record was invisible to consumers until replicated by Broker 2.
                      2. Follower fetched over MessageBus and acknowledged to Leader.
                      3. Leader advanced High Watermark to 1.
                      4. When Broker 1 crashed, Broker 2 already had the committed data on disk. Zero data loss!
                    """);
        }
    }
}
