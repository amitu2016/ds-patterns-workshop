package kafkalite.common;

import kafkalite.zookeeper.PartitionInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mirrors {@code kafka.admin.AdminUtils.assignReplicasToBrokers} / {@code com.workshop.common.ReplicaAssigner}.
 *
 * <p>Assigns partition replicas to brokers across a cluster.
 * <ul>
 *   <li>Distributes partition leaders evenly across brokers in round-robin order.</li>
 *   <li>Spreads follower replicas onto different brokers from the leader, shifting offsets to prevent hotspots.</li>
 *   <li>Supports an optional {@link Random} seed for deterministic testing and reproducible demos.</li>
 * </ul>
 */
public class ReplicaAssigner {
    private final Random random;

    public ReplicaAssigner(Random random) {
        this.random = random;
    }

    public ReplicaAssigner() {
        this(new Random());
    }

    /**
     * Assigns replicas for partitions across available brokers.
     *
     * @param brokerList list of available broker IDs
     * @param nPartitions number of partitions to create
     * @param replicationFactor number of replicas per partition
     * @return set of partition assignments (partition ID -> list of replica broker IDs, leader first)
     */
    public Set<PartitionInfo> assignReplicasToBrokers(List<Integer> brokerList,
                                                     int nPartitions,
                                                     int replicationFactor) {
        if (brokerList == null || brokerList.isEmpty()) {
            throw new IllegalArgumentException("Broker list cannot be empty");
        }
        if (replicationFactor > brokerList.size()) {
            throw new IllegalArgumentException(
                    "Replication factor (" + replicationFactor + ") cannot be larger than " +
                            "number of brokers (" + brokerList.size() + ")"
            );
        }
        if (nPartitions <= 0) {
            throw new IllegalArgumentException("Number of partitions must be positive");
        }
        if (replicationFactor <= 0) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }

        Map<Integer, List<Integer>> ret = new HashMap<>();
        int startIndex = random.nextInt(brokerList.size());
        int currentPartitionId = 0;
        int nextReplicaShift = random.nextInt(brokerList.size());

        for (int partitionId = 0; partitionId < nPartitions; partitionId++) {
            if (currentPartitionId > 0 && (currentPartitionId % brokerList.size() == 0)) {
                nextReplicaShift++;
            }
            int firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size();
            List<Integer> replicaList = new ArrayList<>();
            replicaList.add(brokerList.get(firstReplicaIndex));

            for (int j = 0; j < replicationFactor - 1; j++) {
                int index = getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size());
                replicaList.add(brokerList.get(index));
            }
            ret.put(currentPartitionId, replicaList);
            currentPartitionId++;
        }

        return ret.keySet().stream()
                .map(id -> new PartitionInfo(id, ret.get(id)))
                .collect(Collectors.toSet());
    }

    private int getWrappedIndex(int firstReplicaIndex, int secondReplicaShift,
                                int replicaIndex, int nBrokers) {
        int shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1);
        return (firstReplicaIndex + shift) % nBrokers;
    }
}
