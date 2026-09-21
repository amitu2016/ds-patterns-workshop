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
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLIDE: Consistency - HighWaterMark   (demo 6.2)
 * SLIDE: Replica Consistency
 *
 * <p>Demonstrates the multi-node replication pipeline in action across ticks:
 * <ol>
 *   <li>Three brokers start and elect one controller.</li>
 *   <li>Topic {@code "orders"} is created with RF=3 (Broker 1 is Leader; Brokers 2 &amp; 3 are Followers).</li>
 *   <li>Producer writes a stream of records (offsets 1 through 5) to the Leader.</li>
 *   <li>Leader advances local LEO (Log End Offset), but {@code highWatermark} stays at minimum
 *       offset replicated across ALL in-sync replicas (ISRs).</li>
 *   <li>Followers independently fetch over {@link com.tickloom.messaging.MessageBus}.</li>
 *   <li>As followers catch up, Leader advances {@code highWatermark}.</li>
 *   <li>Followers receive updated {@code highWatermark} in subsequent {@code FetchResponse} frames.</li>
 *   <li>Failover verification: Leader crashes; surviving replicas have identical committed logs.</li>
 * </ol>
 */
class Demo_6_2_ConsistencyHighWatermark extends ZookeeperTestHarness {

    private static final ProcessId BROKER_1 = ProcessId.of("broker-1");
    private static final ProcessId BROKER_2 = ProcessId.of("broker-2");
    private static final ProcessId BROKER_3 = ProcessId.of("broker-3");
    private static final TopicAndPartition ORDERS_0 = new TopicAndPartition("orders", 0);

    @Test
    @DisplayName("6.2 · multi-replica replication pipeline advances High Watermark monotonically across ticks")
    void multiReplicaReplicationPipelineAndFailover() throws Exception {
        System.out.println("\n" + "=".repeat(78));
        System.out.println(" DEMO 6.2: MULTI-REPLICA HIGH WATERMARK REPLICATION PIPELINE");
        System.out.println("=".repeat(78));

        Map<ProcessId, Config> configs = configsFor(BROKER_1, BROKER_2, BROKER_3);

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
            BrokerServer broker3 = cluster.getNode(BROKER_3);

            // Step 1: Create topic 'orders' with RF=3
            System.out.println("\n--- Step 1: Topic 'orders' created (RF=3 across Brokers [1, 2, 3]) ---");
            CreateTopicCommand createTopicCommand = new CreateTopicCommand(
                    broker1.zookeeper(),
                    new kafkalite.common.ReplicaAssigner(new Random() {
                        @Override
                        public int nextInt(int bound) {
                            return 0;
                        }
                    })
            );
            createTopicCommand.createTopic("orders", 1, 3);

            cluster.tickUntil(() -> broker1.isLeaderFor(ORDERS_0)
                    && broker2.followerPartitions().contains(ORDERS_0)
                    && broker3.followerPartitions().contains(ORDERS_0));

            printStatus("Initial", broker1, broker2, broker3);

            // Step 2: Producer appends a batch of 5 messages to Leader (Broker 1)
            System.out.println("\n--- Step 2: Producer sends 5 messages to Leader (Broker 1) ---");
            for (int i = 1; i <= 5; i++) {
                long offset = broker1.append(ORDERS_0, new Message("order-" + i, "item-" + i + ": $" + (i * 20)));
                assertEquals(i, offset);
            }
            System.out.println("  Producer wrote offsets 1..5 to Leader");
            printStatus("After Appends", broker1, broker2, broker3);

            // High Watermark must still be 0 because followers have not replicated yet
            assertEquals(0, broker1.highWatermarkFor(ORDERS_0));
            assertEquals(5, broker1.lastOffsetFor(ORDERS_0));

            // Consumers reading with HIGH_WATERMARK see nothing yet
            List<Message> uncommitted = broker1.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);
            assertTrue(uncommitted.isEmpty(), "Uncommitted records must not be readable past HW");

            // Step 3: Run cluster ticks to propagate replication across MessageBus
            System.out.println("\n--- Step 3: Followers fetch over MessageBus across ticks ---");
            cluster.tickUntil(() -> {
                return broker1.highWatermarkFor(ORDERS_0) == 5
                        && broker2.lastOffsetFor(ORDERS_0) == 5
                        && broker3.lastOffsetFor(ORDERS_0) == 5
                        && broker2.highWatermarkFor(ORDERS_0) == 5
                        && broker3.highWatermarkFor(ORDERS_0) == 5;
            });

            printStatus("Fully Replicated", broker1, broker2, broker3);

            // Step 4: Consumers can now read all 5 records
            System.out.println("\n--- Step 4: Consumer reads all 5 records safely with HIGH_WATERMARK ---");
            List<Message> committed = broker1.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);
            assertEquals(5, committed.size());
            for (int i = 1; i <= 5; i++) {
                assertEquals("order-" + i, committed.get(i - 1).keyAsString());
                System.out.printf("  Offset %d: %s -> %s\n",
                        i, committed.get(i - 1).keyAsString(), committed.get(i - 1).valueAsString());
            }

            // Step 5: Leader Crash & Failover Consistency
            System.out.println("\n--- Step 5: Leader Failover: Broker 1 crashes ---");
            broker1.close();
            System.out.println("  💥 Broker 1 (Leader) stopped!");

            // Verify that both surviving followers have identical logs and can serve reads
            List<Message> broker2Records = broker2.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);
            List<Message> broker3Records = broker3.read(ORDERS_0, 1, FetchIsolation.HIGH_WATERMARK);

            assertEquals(5, broker2Records.size());
            assertEquals(5, broker3Records.size());

            for (int i = 0; i < 5; i++) {
                assertEquals(broker2Records.get(i).keyAsString(), broker3Records.get(i).keyAsString());
                assertEquals(broker2Records.get(i).valueAsString(), broker3Records.get(i).valueAsString());
            }
            System.out.println("  ✅ Both surviving replicas (Brokers 2 & 3) have identical commit logs on disk!");
            System.out.println("  ✅ Any replica can be safely elected new leader without data loss!");

            System.out.println("""

                    ── what succeeded ──
                    The replication pipeline advanced High Watermark to 5:
                      1. Leader accepted 5 records, advancing LEO to 5 while keeping HW at 0.
                      2. Follower 2 and Follower 3 independently fetched over MessageBus.
                      3. Leader advanced HW to min(allReplicas) = 5 only after all replicas acknowledged.
                      4. When Leader crashed, both surviving replicas had full logs. Zero data lost!
                    ==============================================================================
                    """);
        }
    }

    private static void printStatus(String stage, BrokerServer b1, BrokerServer b2, BrokerServer b3) {
        System.out.println("\n  [" + stage + "]");
        System.out.printf("  +-----------+-------------------+-------------------+-------------------+-------------------+\n");
        System.out.printf("  | Node      | Broker 1 (Leader) | Broker 2 (Replica)| Broker 3 (Replica)| High Watermark    |\n");
        System.out.printf("  +-----------+-------------------+-------------------+-------------------+-------------------+\n");
        System.out.printf("  | LEO / HW  | LEO=%-2d  HW=%-2d      | LEO=%-2d  HW=%-2d      | LEO=%-2d  HW=%-2d      | HW = %-13d|\n",
                b1.lastOffsetFor(ORDERS_0), b1.highWatermarkFor(ORDERS_0),
                b2.lastOffsetFor(ORDERS_0), b2.highWatermarkFor(ORDERS_0),
                b3.lastOffsetFor(ORDERS_0), b3.highWatermarkFor(ORDERS_0),
                b1.highWatermarkFor(ORDERS_0));
        System.out.printf("  +-----------+-------------------+-------------------+-------------------+-------------------+\n");
    }
}
