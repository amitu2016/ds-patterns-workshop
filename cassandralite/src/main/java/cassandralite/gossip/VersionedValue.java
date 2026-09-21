package cassandralite.gossip;

/**
 * Mirrors {@code org.apache.cassandra.gms.VersionedValue}.
 *
 * <p>Represents an application or system state value associated with a monotonically
 * incrementing version counter. Whenever a node changes any local state, it increments
 * its local version and tags the new value with that version.
 */
public record VersionedValue(String value, int version) {

    public VersionedValue withIncrementedVersion() {
        return new VersionedValue(value, version + 1);
    }

    public static VersionedValue of(String value, int version) {
        return new VersionedValue(value, version);
    }
}
