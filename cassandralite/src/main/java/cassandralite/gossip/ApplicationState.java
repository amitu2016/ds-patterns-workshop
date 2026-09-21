package cassandralite.gossip;

/**
 * Mirrors {@code org.apache.cassandra.gms.ApplicationState}.
 *
 * <p>Categories of metadata associated with an endpoint in Cassandra gossip.
 */
public enum ApplicationState {
    STATUS,
    LOAD,
    SCHEMA,
    DC,
    RACK,
    INTERNAL_IP,
    TOKENS
}
