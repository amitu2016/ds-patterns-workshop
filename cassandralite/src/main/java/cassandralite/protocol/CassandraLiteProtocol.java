package cassandralite.protocol;

import cassandralite.gossip.EndpointState;
import cassandralite.gossip.GossipDigest;
import com.tickloom.ProcessId;
import com.tickloom.messaging.MessageType;

import java.util.List;
import java.util.Map;

/**
 * Protocol message types and data transfer objects for CassandraLite's 3-way gossip handshake.
 *
 * <p>Mirrors Cassandra's gossip message exchange:
 * <ol>
 *   <li><b>SYN</b>: Node A sends its {@code List<GossipDigest>} to Node B.</li>
 *   <li><b>ACK</b>: Node B compares digests. It returns delta {@code EndpointState}s for endpoints
 *       where B is newer, and requests delta states for endpoints where A is newer.</li>
 *   <li><b>ACK2</b>: Node A applies B's delta states, and replies with delta {@code EndpointState}s
 *       that B requested.</li>
 * </ol>
 */
public final class CassandraLiteProtocol {

    private CassandraLiteProtocol() {}

    public static final MessageType GOSSIP_DIGEST_SYN = MessageType.of("GossipDigestSyn");
    public static final MessageType GOSSIP_DIGEST_ACK = MessageType.of("GossipDigestAck");
    public static final MessageType GOSSIP_DIGEST_ACK2 = MessageType.of("GossipDigestAck2");

    /**
     * Step 1: Initiates gossip with a peer by sending digests of all known endpoints.
     */
    public record GossipDigestSyn(
            ProcessId from,
            List<GossipDigest> digests
    ) {}

    /**
     * Step 2: Responds to SYN with:
     * <ul>
     *   <li>{@code deltaStates}: Endpoints where responder is ahead of initiator.</li>
     *   <li>{@code digestsToFetch}: Endpoints where initiator is ahead of responder.</li>
     * </ul>
     */
    public record GossipDigestAck(
            ProcessId from,
            Map<ProcessId, EndpointState> deltaStates,
            List<GossipDigest> digestsToFetch
    ) {}

    /**
     * Step 3: Responds to ACK with the delta states that the responder requested.
     */
    public record GossipDigestAck2(
            ProcessId from,
            Map<ProcessId, EndpointState> deltaStates
    ) {}
}
