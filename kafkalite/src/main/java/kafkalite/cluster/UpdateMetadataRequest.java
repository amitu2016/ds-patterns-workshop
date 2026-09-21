package kafkalite.cluster;

import java.util.List;

/**
 * Mirrors {@code kafka.api.UpdateMetadataRequest}.
 *
 * <p>Sent by the controller to all live brokers in the cluster so they can update
 * their local metadata cache. Stamped with {@code controllerEpoch} for fencing.
 */
public record UpdateMetadataRequest(int controllerId, int controllerEpoch, List<Broker> aliveBrokers) {
    public UpdateMetadataRequest {
        aliveBrokers = aliveBrokers == null ? List.of() : List.copyOf(aliveBrokers);
    }
}
