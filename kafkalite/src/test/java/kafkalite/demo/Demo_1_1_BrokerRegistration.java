package kafkalite.demo;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import kafkalite.common.Config;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.server.BrokerServer;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SLIDE: Assignment 1: Node registration with Zookeeper   (demo 1.1)
 * SLIDE: Consistent Core Interface & Cluster Primitives
 *
 * <p>Group membership with no membership protocol. Each broker writes one EPHEMERAL znode at
 * {@code /brokers/ids/<id>} as part of its own startup. ZooKeeper's session lifetime is the
 * failure detector.
 *
 * <p><b>Note who is observing.</b> The brokers are not watching each other — in Kafka only the
 * controller watches {@code /brokers/ids}, and everyone else is told via {@code UpdateMetadata}.
 * Here we read the core directly. Who <i>reacts</i> to a membership change is Topic 2's subject.
 */
class Demo_1_1_BrokerRegistration extends ZookeeperTestHarness {

    // TRY IT: add ProcessId.of("broker-4") to this list. It registers itself and nothing else
    //         in the demo changes — registration is self-service, and the assertions below are
    //         derived from the list rather than written out.
    private static final List<ProcessId> BROKERS = List.of(
            ProcessId.of("broker-1"), ProcessId.of("broker-2"), ProcessId.of("broker-3"), ProcessId.of("broker-4"));

    /** The broker whose ZooKeeper session we end, to watch its znode vanish with it. */
    private static final ProcessId LOSES_SESSION = BROKERS.get(BROKERS.size() - 1);

    /** brokerIdOf is inherited from ZookeeperTestHarness. */
    private static Set<Integer> allBrokerIds() {
        return BROKERS.stream().map(ZookeeperTestHarness::brokerIdOf).collect(Collectors.toSet());
    }

    @Test
    @DisplayName("1.1 · brokers register as ephemeral znodes; the session is the lease")
    void brokersRegisterThemselvesAndVanishWithTheirSession() throws Exception {
        // One config, one ZooKeeper session, one port per broker — as in a real deployment.
        Map<ProcessId, Config> configs = configsFor(BROKERS.toArray(new ProcessId[0]));

        try (Cluster cluster = new Cluster()
                .withProcessIds(List.copyOf(configs.keySet()))
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    Config config = configs.get(params.id());
                    return new BrokerServer(peerIds, params, config, new ZookeeperClient(config));
                })
                .start()) {

            System.out.println("\n--- ZooKeeper at " + zkConnect + " ---");
            System.out.println("/brokers/ids before any broker ticks: " + registry(cluster) + "\n");

            // ---- 1. registration is part of each broker's own startup. Nothing coordinates it.
            cluster.tickUntil(cluster::areAllNodesInitialized);

            printRegistry(cluster, "after startup");
            assertEquals(allBrokerIds(), registry(cluster));

            // ---- 2. a broker dies. No goodbye, no shutdown hook.
            System.out.println("\n--- ending " + LOSES_SESSION.name() + "'s ZooKeeper session (no graceful leave) ---");
            cluster.<BrokerServer>getNode(LOSES_SESSION).zookeeper().close();

            // ---- 3. the znode goes with the session
            Set<Integer> survivors = new java.util.HashSet<>(allBrokerIds());
            survivors.remove(brokerIdOf(LOSES_SESSION));
            cluster.tickUntil(() -> registry(cluster).equals(survivors));
            printRegistry(cluster, "after " + LOSES_SESSION.name() + "'s session ended");

            System.out.println("""

                    ── what just happened ──
                    No heartbeat protocol. No timeout logic in any broker.
                    The znode was EPHEMERAL: it existed exactly as long as the session,
                    so session death *is* failure detection. That is the Consistent Core
                    handing out a lease.
                    """);
        }
    }

    /** Any observer's view of the core — read through the first broker's ZooKeeper connection. */
    private static Set<Integer> registry(Cluster cluster) {
        return cluster.<BrokerServer>getNode(BROKERS.get(0)).zookeeper().getAllBrokerIds();
    }

    private static void printRegistry(Cluster cluster, String label) {
        ZookeeperClient zk = cluster.<BrokerServer>getNode(BROKERS.get(0)).zookeeper();
        System.out.println("\n--- /brokers/ids " + label + " ---");
        zk.getAllBrokers().stream()
                .sorted(java.util.Comparator.comparingInt(b -> b.id()))
                .forEach(b -> System.out.println("  /brokers/ids/" + b.id() + "  ->  " + b));
    }
}
