package kafkalite.cluster;

import java.util.List;

/**
 * Mirrors {@code kafka.api.LeaderAndIsrRequest}.
 *
 * <p>Sent by the controller to inform brokers about partition leadership and replica assignments.
 * Stamped with {@code controllerEpoch} so stale controllers are rejected.
 */
public record LeaderAndIsrRequest(int controllerId, int controllerEpoch, List<LeaderAndReplicas> leaderReplicas) {
    public LeaderAndIsrRequest {
        leaderReplicas = leaderReplicas == null ? List.of() : List.copyOf(leaderReplicas);
    }
}
