package cassandralite.cluster;

import cassandralite.gossip.Gossiper;
import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;

import java.util.*;

/**
 * Harness for creating and managing a cluster of {@link Gossiper} nodes running on
 * tickloom's simulated network and deterministic tick loop.
 */
public class CassandraCluster implements AutoCloseable {

    private final Cluster cluster;
    private final List<ProcessId> nodeIds;
    private final List<ProcessId> seedIds;

    public static CassandraCluster create(int numNodes) throws Exception {
        return create(numNodes, 1);
    }

    public static CassandraCluster create(int numNodes, int numSeeds) throws Exception {
        List<ProcessId> allIds = new ArrayList<>();
        for (int i = 1; i <= numNodes; i++) {
            allIds.add(ProcessId.of("node-" + i));
        }
        List<ProcessId> seeds = allIds.subList(0, Math.min(numSeeds, numNodes));

        Cluster cluster = new Cluster()
                .withProcessIds(allIds)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> new Gossiper(peerIds, params, seeds, 1, new Random(42 + params.id().hashCode())))
                .start();

        return new CassandraCluster(cluster, allIds, seeds);
    }

    public CassandraCluster(Cluster cluster, List<ProcessId> nodeIds, List<ProcessId> seedIds) {
        this.cluster = cluster;
        this.nodeIds = List.copyOf(nodeIds);
        this.seedIds = List.copyOf(seedIds);
    }

    public Cluster cluster() {
        return cluster;
    }

    public List<ProcessId> nodeIds() {
        return nodeIds;
    }

    public List<ProcessId> seedIds() {
        return seedIds;
    }

    public Gossiper getNode(ProcessId id) {
        return cluster.getNode(id);
    }

    public List<Gossiper> getAllNodes() {
        return nodeIds.stream().map(this::getNode).toList();
    }

    public void tick() {
        cluster.tick();
    }

    public int tickUntilConverged(int maxTicks) {
        for (int round = 1; round <= maxTicks; round++) {
            cluster.tick();
            if (isConverged()) {
                return round;
            }
        }
        return -1;
    }

    public boolean isConverged() {
        for (Gossiper node : getAllNodes()) {
            if (node.getEndpointStates().size() < nodeIds.size()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() {
        if (cluster != null) {
            cluster.close();
        }
    }
}
