package kafkalite.cluster;

import java.util.List;

/**
 * Mirrors {@code kafka.controller.LeaderIsrAndControllerEpoch} / {@code PartitionStateInfo}.
 *
 * <p>Represents the state of a partition including its elected leader and full replica list.
 */
public record PartitionStateInfo(int leaderBrokerId, List<Broker> allReplicas) {
    public PartitionStateInfo {
        allReplicas = allReplicas == null ? List.of() : List.copyOf(allReplicas);
    }
}
