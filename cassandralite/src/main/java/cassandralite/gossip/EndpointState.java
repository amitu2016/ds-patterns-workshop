package cassandralite.gossip;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * Mirrors {@code org.apache.cassandra.gms.EndpointState}.
 *
 * <p>Contains all versioned metadata known for a single cluster endpoint:
 * <ul>
 *   <li>{@link HeartBeatState}: Generation and heartbeat version counter.</li>
 *   <li>{@link ApplicationState}: Map of application states (e.g. STATUS, TOKENS).</li>
 * </ul>
 */
public class EndpointState {

    private final HeartBeatState heartBeatState;
    private final Map<ApplicationState, VersionedValue> applicationStates;
    private final long updateTimestamp;

    @JsonCreator
    public EndpointState(
            @JsonProperty("heartBeatState") HeartBeatState heartBeatState,
            @JsonProperty("applicationStates") Map<ApplicationState, VersionedValue> applicationStates,
            @JsonProperty("updateTimestamp") long updateTimestamp) {
        this.heartBeatState = heartBeatState;
        this.applicationStates = applicationStates != null ? new HashMap<>(applicationStates) : new HashMap<>();
        this.updateTimestamp = updateTimestamp;
    }

    public EndpointState(HeartBeatState heartBeatState) {
        this(heartBeatState, new HashMap<>(), System.currentTimeMillis());
    }

    public EndpointState(HeartBeatState heartBeatState, Map<ApplicationState, VersionedValue> applicationStates) {
        this(heartBeatState, applicationStates, System.currentTimeMillis());
    }

    @JsonProperty("heartBeatState")
    public HeartBeatState heartBeatState() {
        return heartBeatState;
    }

    @JsonProperty("applicationStates")
    public Map<ApplicationState, VersionedValue> applicationStates() {
        return Collections.unmodifiableMap(applicationStates);
    }

    @JsonProperty("updateTimestamp")
    public long updateTimestamp() {
        return updateTimestamp;
    }

    public VersionedValue getApplicationState(ApplicationState key) {
        return applicationStates.get(key);
    }

    public EndpointState withApplicationState(ApplicationState key, VersionedValue value) {
        Map<ApplicationState, VersionedValue> updated = new HashMap<>(this.applicationStates);
        updated.put(key, value);
        return new EndpointState(this.heartBeatState, updated, System.currentTimeMillis());
    }

    public EndpointState withHeartBeatState(HeartBeatState newHeartBeat) {
        return new EndpointState(newHeartBeat, this.applicationStates, System.currentTimeMillis());
    }

    /**
     * Returns the highest version known across this endpoint's heartbeat and all application states.
     */
    public int maxVersion() {
        int max = heartBeatState != null ? heartBeatState.version() : 0;
        for (VersionedValue vv : applicationStates.values()) {
            if (vv.version() > max) {
                max = vv.version();
            }
        }
        return max;
    }

    /**
     * Computes the subset of this EndpointState that is newer than {@code remoteMaxVersion}.
     */
    public EndpointState statesGreaterThan(int remoteMaxVersion) {
        HeartBeatState hb = null;
        if (heartBeatState != null && heartBeatState.version() > remoteMaxVersion) {
            hb = heartBeatState;
        }
        Map<ApplicationState, VersionedValue> deltaApp = new HashMap<>();
        for (Map.Entry<ApplicationState, VersionedValue> entry : applicationStates.entrySet()) {
            if (entry.getValue().version() > remoteMaxVersion) {
                deltaApp.put(entry.getKey(), entry.getValue());
            }
        }
        if (hb == null && deltaApp.isEmpty()) {
            return null;
        }
        return new EndpointState(hb != null ? hb : heartBeatState, deltaApp, System.currentTimeMillis());
    }

    /**
     * Merges newer fields from {@code incoming} into this EndpointState.
     */
    public EndpointState merge(EndpointState incoming) {
        if (incoming == null) {
            return this;
        }
        HeartBeatState mergedHb = this.heartBeatState;
        if (incoming.heartBeatState() != null) {
            if (mergedHb == null || incoming.heartBeatState().generation() > mergedHb.generation()
                    || (incoming.heartBeatState().generation() == mergedHb.generation()
                    && incoming.heartBeatState().version() > mergedHb.version())) {
                mergedHb = incoming.heartBeatState();
            }
        }
        Map<ApplicationState, VersionedValue> mergedApp = new HashMap<>(this.applicationStates);
        for (Map.Entry<ApplicationState, VersionedValue> entry : incoming.applicationStates().entrySet()) {
            VersionedValue existing = mergedApp.get(entry.getKey());
            if (existing == null || entry.getValue().version() > existing.version()) {
                mergedApp.put(entry.getKey(), entry.getValue());
            }
        }
        return new EndpointState(mergedHb, mergedApp, System.currentTimeMillis());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndpointState that = (EndpointState) o;
        return Objects.equals(heartBeatState, that.heartBeatState) &&
                Objects.equals(applicationStates, that.applicationStates);
    }

    @Override
    public int hashCode() {
        return Objects.hash(heartBeatState, applicationStates);
    }

    @Override
    public String toString() {
        return "EndpointState{" +
                "hb=" + heartBeatState +
                ", appStates=" + applicationStates +
                ", maxVersion=" + maxVersion() +
                '}';
    }
}
