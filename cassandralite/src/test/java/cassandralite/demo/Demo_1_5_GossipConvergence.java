package cassandralite.demo;

import cassandralite.cluster.CassandraCluster;
import cassandralite.gossip.ApplicationState;
import cassandralite.gossip.EndpointState;
import cassandralite.gossip.Gossiper;
import cassandralite.gossip.VersionedValue;
import com.tickloom.ProcessId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Slide 1.5: What's common? (gossip).
 *
 * <p>Two different questions get asked of gossip, and they have different answers:
 *
 * <ol>
 *   <li><b>Bootstrap.</b> A node starts knowing only the seed list. How long until everyone knows
 *       everyone? <b>Constant</b> — and that is the point of seeds, not a flaw.</li>
 *   <li><b>Dissemination.</b> In a cluster that already agrees, one node changes its state. How
 *       long until every node has it? <b>O(log N)</b> — the epidemic property that makes gossip
 *       usable at thousands of nodes.</li>
 * </ol>
 *
 * <p>Conflating the two is easy and hides both lessons. A constant bootstrap number under a
 * "logarithmic" headline says nothing; the logarithm only appears once information has to travel
 * more than one hop.
 *
 * <p>Run via: {@code make demo-1.5}
 */
// SLIDE: What’s common?
@DisplayName("Demo 1.5: Gossip Convergence")
public class Demo_1_5_GossipConvergence {

    // TRY IT: add 512 and 1024. Bootstrap stays flat; dissemination grows by about one round each
    //         time the cluster doubles. That gap is the whole lesson.
    static final List<Integer> CLUSTER_SIZES = List.of(4, 8, 16, 32, 64, 128, 200);

    /** How many nodes every other node is told about at startup. Cassandra's seed list. */
    static final int SEED_NODES = 1;
    static final int MAX_ROUNDS = 100;

    @Test
    void bootstrapIsConstantWhileDisseminationIsLogarithmic() throws Exception {
        System.out.println("======================================================================");
        System.out.println("  Demo 1.5 · Decentralized Peer-to-Peer Gossip");
        System.out.println("  (Apache Cassandra GMS Protocol on tickloom discrete tick loop)");
        System.out.println("======================================================================");

        Map<Integer, Integer> bootstrapRounds = new LinkedHashMap<>();
        Map<Integer, Integer> disseminationRounds = new LinkedHashMap<>();

        for (int size : CLUSTER_SIZES) {
            try (CassandraCluster cluster = CassandraCluster.create(size, SEED_NODES)) {
                bootstrapRounds.put(size, roundsToLearnEveryone(cluster, size));
                disseminationRounds.put(size, roundsToSpreadOneUpdate(cluster));
            }
        }

        printBootstrap(bootstrapRounds);
        printDissemination(disseminationRounds);

        // --- Bootstrap: flat. Every node starts knowing only the seed, so round 1 is N-1 SYNs to
        //     one node, which answers each with the membership it just accumulated.
        int first = bootstrapRounds.get(CLUSTER_SIZES.get(0));
        for (int size : CLUSTER_SIZES) {
            assertEquals(first, bootstrapRounds.get(size),
                    "bootstrap through a seed is one handshake, so it must not grow with N");
        }

        // --- Dissemination: grows, but logarithmically. Over a 50x range of cluster sizes the
        //     round count must grow, yet stay far below anything linear in N.
        int smallest = disseminationRounds.get(CLUSTER_SIZES.get(0));
        int largest = disseminationRounds.get(CLUSTER_SIZES.get(CLUSTER_SIZES.size() - 1));
        assertTrue(largest > smallest,
                "dissemination must cost more in a bigger cluster, or it is not epidemic at all");
        assertTrue(largest <= 2 * ceilLog2(CLUSTER_SIZES.get(CLUSTER_SIZES.size() - 1)),
                "dissemination must stay within a small multiple of log2(N); was " + largest);

        // Monotonic in the sense that matters: never fewer rounds for a bigger cluster.
        int previous = 0;
        for (int size : CLUSTER_SIZES) {
            assertTrue(disseminationRounds.get(size) >= previous,
                    "a larger cluster must not disseminate faster");
            previous = disseminationRounds.get(size);
        }
    }

    /** Rounds until every node holds an {@link EndpointState} for every other node. */
    private int roundsToLearnEveryone(CassandraCluster cluster, int size) {
        for (int round = 1; round <= MAX_ROUNDS; round++) {
            cluster.tick();
            if (cluster.isConverged()) {
                for (Gossiper node : cluster.getAllNodes()) {
                    for (ProcessId target : cluster.nodeIds()) {
                        assertTrue(node.getEndpointStates().containsKey(target),
                                node.id() + " is missing state for " + target);
                    }
                }
                return round;
            }
        }
        throw new AssertionError("cluster of " + size + " did not converge in " + MAX_ROUNDS + " rounds");
    }

    /**
     * Rounds for a state change at one node to reach every node, starting from a converged cluster.
     *
     * <p>This is the measurement the O(log N) claim is about. Nothing is one hop from everyone here:
     * a seed is contacted with probability {@code seeds/live}, so the update has to spread from peer
     * to random peer, doubling its reach each round.
     */
    private int roundsToSpreadOneUpdate(CassandraCluster cluster) {
        List<Gossiper> nodes = cluster.getAllNodes();
        Gossiper origin = nodes.get(nodes.size() - 1);          // deliberately not the seed
        ProcessId key = origin.id();

        origin.updateApplicationState(ApplicationState.STATUS, new VersionedValue("DECOMMISSIONING", 999));

        for (int round = 1; round <= MAX_ROUNDS; round++) {
            cluster.tick();
            if (nodes.stream().allMatch(node -> hasUpdate(node, key))) {
                return round;
            }
        }
        throw new AssertionError("update did not reach every node in " + MAX_ROUNDS + " rounds");
    }

    private boolean hasUpdate(Gossiper node, ProcessId origin) {
        EndpointState state = node.getEndpointStates().get(origin);
        if (state == null) {
            return false;
        }
        VersionedValue status = state.applicationStates().get(ApplicationState.STATUS);
        return status != null && "DECOMMISSIONING".equals(status.value());
    }

    private static int ceilLog2(int n) {
        return 32 - Integer.numberOfLeadingZeros(n - 1);
    }

    private void printBootstrap(Map<Integer, Integer> rounds) {
        System.out.println("\n── Part 1 · Bootstrap: every node starts knowing only the seed ───────");
        System.out.printf("%-14s | %-11s | %-20s | %s%n",
                "Cluster Size", "Seed Nodes", "Rounds to Converge", "Agreement");
        System.out.println("---------------+-------------+----------------------+---------------");
        rounds.forEach((size, r) -> System.out.printf("%-14s | %-11s | %-20s | %s%n",
                size + " nodes", SEED_NODES + " seed(s)", r + " rounds", "100% (all nodes)"));
        System.out.println("""
                
                   Flat, and correctly so. Round 1 is N-1 SYNs to the one node everybody was
                   told about; it answers each with the membership it just learned. A seed is a
                   rendezvous, so bootstrap costs one handshake no matter how large N is.""");
    }

    private void printDissemination(Map<Integer, Integer> rounds) {
        System.out.println("\n── Part 2 · Dissemination: one node changes state, cluster already agrees ─");
        System.out.printf("%-14s | %-22s | %s%n", "Cluster Size", "Rounds to Reach All", "log2(N)");
        System.out.println("---------------+------------------------+---------------");
        rounds.forEach((size, r) -> System.out.printf("%-14s | %-22s | %d%n",
                size + " nodes", r + " rounds", ceilLog2(size)));

        List<Integer> sizes = new ArrayList<>(rounds.keySet());
        int first = rounds.get(sizes.get(0));
        int last = rounds.get(sizes.get(sizes.size() - 1));
        System.out.printf("""
                
                   %dx more nodes costs %d extra round(s), not %dx more. Each round roughly
                   doubles the number of nodes holding the update, so reaching all of them takes
                   about log2(N) rounds. That is why gossip works at thousands of nodes -- and it
                   only shows up once no single node is one hop from everyone.%n""",
                sizes.get(sizes.size() - 1) / sizes.get(0), last - first, sizes.get(sizes.size() - 1) / sizes.get(0));
        System.out.println("======================================================================\n");
    }
}
