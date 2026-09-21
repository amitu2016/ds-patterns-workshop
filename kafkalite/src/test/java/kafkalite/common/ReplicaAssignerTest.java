package kafkalite.common;

import kafkalite.zookeeper.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Ported from {@code com.workshop.common.ReplicaAssignerTest}.
 *
 * <p>Validates Kafka's replica assignment algorithm:
 * <ul>
 *   <li>Equal distribution of partition leaders.</li>
 *   <li>Followers placed on distinct brokers from leaders.</li>
 *   <li>Deterministic assignment with seeds.</li>
 *   <li>Input validation for empty brokers, invalid partition counts, and excessive replication factors.</li>
 * </ul>
 */
class ReplicaAssignerTest {

    @Test
    void testBasicAssignment() {
        ReplicaAssigner assigner = new ReplicaAssigner(new Random(42));
        List<Integer> brokers = List.of(0, 1, 2);

        Set<PartitionInfo> assignments = assigner.assignReplicasToBrokers(brokers, 6, 2);

        assertEquals(6, assignments.size());
        assignments.forEach(info -> {
            assertEquals(2, info.brokerIds().size(),
                    "Partition " + info.partitionId() + " should have 2 replicas");
        });

        Set<Integer> partitionIds = new HashSet<>();
        assignments.forEach(info -> partitionIds.add(info.partitionId()));
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), partitionIds);
    }

    @Test
    void testReplicasOnDifferentBrokers() {
        ReplicaAssigner assigner = new ReplicaAssigner(new Random(42));
        List<Integer> brokers = List.of(0, 1, 2, 3);

        Set<PartitionInfo> assignments = assigner.assignReplicasToBrokers(brokers, 4, 3);

        assignments.forEach(info -> {
            Set<Integer> uniqueBrokers = new HashSet<>(info.brokerIds());
            assertEquals(3, uniqueBrokers.size(),
                    "Partition " + info.partitionId() + " should have replicas on 3 different brokers");
        });
    }

    @Test
    void testLoadBalancing() {
        ReplicaAssigner assigner = new ReplicaAssigner(new Random(42));
        List<Integer> brokers = List.of(0, 1, 2);

        Set<PartitionInfo> assignments = assigner.assignReplicasToBrokers(brokers, 9, 2);

        Map<Integer, Integer> leaderCount = new HashMap<>();
        assignments.forEach(info -> {
            int leader = info.brokerIds().get(0);
            leaderCount.put(leader, leaderCount.getOrDefault(leader, 0) + 1);
        });

        assertEquals(3, leaderCount.size(), "All brokers should be leaders");
        leaderCount.values().forEach(count ->
                assertEquals(3, count, "Each broker should lead 3 partitions")
        );
    }

    @Test
    void testSingleBrokerSinglePartition() {
        ReplicaAssigner assigner = new ReplicaAssigner();
        List<Integer> brokers = List.of(0);

        Set<PartitionInfo> assignments = assigner.assignReplicasToBrokers(brokers, 1, 1);

        assertEquals(1, assignments.size());
        PartitionInfo info = assignments.iterator().next();
        assertEquals(0, info.partitionId());
        assertEquals(List.of(0), info.brokerIds());
    }

    @Test
    void testMaxReplicationFactor() {
        ReplicaAssigner assigner = new ReplicaAssigner(new Random(42));
        List<Integer> brokers = List.of(0, 1, 2, 3);

        Set<PartitionInfo> assignments = assigner.assignReplicasToBrokers(brokers, 4, 4);

        assertEquals(4, assignments.size());
        assignments.forEach(info -> {
            assertEquals(4, info.brokerIds().size());
            Set<Integer> uniqueBrokers = new HashSet<>(info.brokerIds());
            assertEquals(4, uniqueBrokers.size(), "All brokers should be unique");
        });
    }

    @Test
    void testReplicationFactorTooLarge() {
        ReplicaAssigner assigner = new ReplicaAssigner();
        List<Integer> brokers = List.of(0, 1, 2);

        assertThrows(IllegalArgumentException.class, () ->
                assigner.assignReplicasToBrokers(brokers, 3, 4)
        );
    }

    @Test
    void testEmptyBrokerList() {
        ReplicaAssigner assigner = new ReplicaAssigner();
        assertThrows(IllegalArgumentException.class, () ->
                assigner.assignReplicasToBrokers(List.of(), 3, 2)
        );
    }

    @Test
    void testDeterministicWithSeed() {
        ReplicaAssigner assigner1 = new ReplicaAssigner(new Random(12345));
        ReplicaAssigner assigner2 = new ReplicaAssigner(new Random(12345));

        List<Integer> brokers = List.of(0, 1, 2, 3);

        Set<PartitionInfo> assignments1 = assigner1.assignReplicasToBrokers(brokers, 10, 3);
        Set<PartitionInfo> assignments2 = assigner2.assignReplicasToBrokers(brokers, 10, 3);

        List<PartitionInfo> list1 = new ArrayList<>(assignments1);
        List<PartitionInfo> list2 = new ArrayList<>(assignments2);
        list1.sort(Comparator.comparingInt(PartitionInfo::partitionId));
        list2.sort(Comparator.comparingInt(PartitionInfo::partitionId));

        assertEquals(list1.size(), list2.size());
        for (int i = 0; i < list1.size(); i++) {
            assertEquals(list1.get(i).partitionId(), list2.get(i).partitionId());
            assertEquals(list1.get(i).brokerIds(), list2.get(i).brokerIds());
        }
    }
}
