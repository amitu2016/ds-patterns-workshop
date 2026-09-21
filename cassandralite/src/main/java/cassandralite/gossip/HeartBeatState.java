package cassandralite.gossip;

/**
 * Mirrors {@code org.apache.cassandra.gms.HeartBeatState}.
 *
 * <p>Tracks a node's heartbeat generation and version:
 * <ul>
 *   <li><b>generation</b>: Initialized at startup (e.g. system timestamp or reboot sequence counter).
 *       A higher generation always wins regardless of version.</li>
 *   <li><b>version</b>: Incremented monotonically on every gossip tick.</li>
 * </ul>
 */
public record HeartBeatState(int generation, int version) {

    public HeartBeatState withIncrementedVersion() {
        return new HeartBeatState(generation, version + 1);
    }

    public static HeartBeatState of(int generation, int version) {
        return new HeartBeatState(generation, version);
    }
}
