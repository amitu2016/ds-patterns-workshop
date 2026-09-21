package cassandralite.gossip;

import com.tickloom.ProcessId;

/**
 * Mirrors {@code org.apache.cassandra.gms.GossipDigest}.
 *
 * <p>A lightweight digest of an endpoint's current state:
 * {@code (endpoint, generation, maxVersion)}.
 * Exchanged during the gossip handshake to determine which node has newer information.
 */
public record GossipDigest(ProcessId endpoint, int generation, int maxVersion) implements Comparable<GossipDigest> {

    @Override
    public int compareTo(GossipDigest o) {
        if (this.generation != o.generation) {
            return Integer.compare(this.generation, o.generation);
        }
        return Integer.compare(this.maxVersion, o.maxVersion);
    }
}
