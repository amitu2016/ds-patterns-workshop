package cassandralite.gossip;

import cassandralite.heartbeat.FailureDetector;
import cassandralite.heartbeat.PhiAccrualFailureDetector;
import cassandralite.heartbeat.ServerState;
import cassandralite.protocol.CassandraLiteProtocol;
import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static cassandralite.protocol.CassandraLiteProtocol.*;

/**
 * Mirrors {@code org.apache.cassandra.gms.Gossiper}.
 *
 * <p>The central actor in Cassandra's peer-to-peer decentralized membership protocol.
 * Every node runs a {@code Gossiper} instance. In tickloom, the gossiper is a first-class
 * {@link Replica} whose periodic gossip rounds, heartbeats, and failure detection evaluations
 * are driven deterministically by {@link #onTick()}.
 *
 * <p><b>Protocol:</b>
 * Implements Cassandra's authentic 3-way gossip handshake:
 * <ol>
 *   <li><b>SYN</b>: Periodic tick triggers random peer selection; node sends {@link GossipDigestSyn}
 *       with summary digests for all known endpoints.</li>
 *   <li><b>ACK</b>: Remote compares digests. Responds with delta states where it is ahead,
 *       and requests newer digests where the initiator is ahead.</li>
 *   <li><b>ACK2</b>: Initiator applies remote delta states and returns the delta states requested
 *       by the responder.</li>
 * </ol>
 */
public class Gossiper extends Replica {

    private static final Logger logger = LoggerFactory.getLogger(Gossiper.class);

    private final List<ProcessId> seedNodes;
    private final int generation;
    private final Random random;

    private HeartBeatState heartBeatState;
    private Map<ProcessId, EndpointState> endpointStates;
    private FailureDetector failureDetector;

    private long currentTick = 0L;
    private boolean paused = false;
    private int gossipFanout = 1;

    public Gossiper(List<ProcessId> peerIds, ProcessParams processParams, List<ProcessId> seedNodes) {
        this(peerIds, processParams, seedNodes, (int) (System.currentTimeMillis() / 1000), new Random());
    }

    public Gossiper(List<ProcessId> peerIds, ProcessParams processParams, List<ProcessId> seedNodes,
                    int generation, Random random) {
        super(peerIds, processParams);
        this.seedNodes = seedNodes != null ? seedNodes.stream().filter(s -> !s.equals(id)).toList() : List.of();
        this.generation = generation;
        this.random = random != null ? random : new Random();
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                GOSSIP_DIGEST_SYN, this::handleGossipDigestSyn,
                GOSSIP_DIGEST_ACK, this::handleGossipDigestAck,
                GOSSIP_DIGEST_ACK2, this::handleGossipDigestAck2
        );
    }

    @Override
    protected TickCompletableFuture<?> onInit() {
        this.endpointStates = new ConcurrentHashMap<>();
        this.failureDetector = new PhiAccrualFailureDetector();
        this.heartBeatState = new HeartBeatState(this.generation, 1);
        EndpointState initialSelfState = new EndpointState(this.heartBeatState)
                .withApplicationState(ApplicationState.STATUS, new VersionedValue("NORMAL", 1))
                .withApplicationState(ApplicationState.INTERNAL_IP, new VersionedValue(id.name(), 1));
        this.endpointStates.put(id, initialSelfState);
        this.failureDetector.report(id, 0L);

        TickCompletableFuture<Void> future = new TickCompletableFuture<>();
        future.complete(null);
        return future;
    }

    @Override
    protected void onTick() {
        currentTick++;
        if (paused) {
            return;
        }

        // 1. Advance local heartbeat
        heartBeatState = heartBeatState.withIncrementedVersion();
        EndpointState currentSelf = endpointStates.get(id);
        if (currentSelf == null) {
            currentSelf = new EndpointState(heartBeatState)
                    .withApplicationState(ApplicationState.STATUS, new VersionedValue("NORMAL", 1))
                    .withApplicationState(ApplicationState.INTERNAL_IP, new VersionedValue(id.name(), 1));
        } else {
            currentSelf = currentSelf.withHeartBeatState(heartBeatState);
        }
        endpointStates.put(id, currentSelf);
        failureDetector.report(id, currentTick);

        // 2. Failure detector evaluation on all known endpoints
        for (ProcessId ep : endpointStates.keySet()) {
            if (!ep.equals(id)) {
                failureDetector.interpret(ep, currentTick);
            }
        }

        // 3. Choose gossip targets and send SYN
        List<ProcessId> targets = selectGossipTargets();
        if (targets.isEmpty()) {
            return;
        }

        List<GossipDigest> digests = makeDigests();
        for (ProcessId target : targets) {
            GossipDigestSyn syn = new GossipDigestSyn(id, digests);
            try {
                Message msg = createMessage(target, "syn-" + id + "-" + currentTick, syn, GOSSIP_DIGEST_SYN);
                messageBus.sendMessage(msg);
            } catch (IOException e) {
                logger.warn("{}: failed sending GossipDigestSyn to {}", id, target, e);
            }
        }
    }

    private List<ProcessId> selectGossipTargets() {
        Set<ProcessId> live = liveEndpoints();
        live.remove(id);

        List<ProcessId> targets = new ArrayList<>();
        if (!live.isEmpty()) {
            List<ProcessId> liveList = new ArrayList<>(live);
            for (int i = 0; i < gossipFanout; i++) {
                ProcessId chosen = liveList.get(random.nextInt(liveList.size()));
                if (!targets.contains(chosen)) {
                    targets.add(chosen);
                }
            }
        }

        maybeGossipToSeed(targets, live);
        return targets;
    }

    /**
     * Mirrors {@code Gossiper.maybeGossipToSeed}. A seed is contacted with probability
     * {@code seeds / liveEndpoints}, not on every round.
     *
     * <p>The difference is the whole shape of the protocol. Contacting a seed unconditionally makes
     * it a permanent hub: every node reaches every other node in one hop through it, and membership
     * converges in a constant number of rounds no matter how large the cluster is. Cassandra uses a
     * seed only as a rendezvous — while a node knows nobody the probability is 1, and it falls to
     * {@code 1/N} once the cluster is known, leaving ordinary fanout-1 gossip to random live peers.
     * That is the epidemic process whose dissemination time is O(log N); demo 1.5 measures both.
     */
    private void maybeGossipToSeed(List<ProcessId> targets, Set<ProcessId> live) {
        if (seedNodes.isEmpty() || targets.stream().anyMatch(seedNodes::contains)) {
            return;
        }
        double probability = live.isEmpty() ? 1.0 : (double) seedNodes.size() / live.size();
        if (random.nextDouble() > probability) {
            return;
        }
        ProcessId seed = seedNodes.get(random.nextInt(seedNodes.size()));
        if (!targets.contains(seed)) {
            targets.add(seed);
        }
    }

    private List<GossipDigest> makeDigests() {
        List<GossipDigest> digests = new ArrayList<>();
        for (Map.Entry<ProcessId, EndpointState> entry : endpointStates.entrySet()) {
            EndpointState state = entry.getValue();
            digests.add(new GossipDigest(
                    entry.getKey(),
                    state.heartBeatState() != null ? state.heartBeatState().generation() : 0,
                    state.maxVersion()
            ));
        }
        return digests;
    }

    // region Message Handlers

    private void handleGossipDigestSyn(Message message) {
        if (paused) {
            return;
        }
        GossipDigestSyn syn = deserializePayload(message.payload(), GossipDigestSyn.class);

        Map<ProcessId, EndpointState> deltaStatesForRemote = new HashMap<>();
        List<GossipDigest> digestsToFetchFromRemote = new ArrayList<>();

        Map<ProcessId, GossipDigest> remoteDigestMap = new HashMap<>();
        for (GossipDigest rd : syn.digests()) {
            remoteDigestMap.put(rd.endpoint(), rd);
        }

        // Compare local states against remote digests
        for (Map.Entry<ProcessId, EndpointState> entry : endpointStates.entrySet()) {
            ProcessId ep = entry.getKey();
            EndpointState localState = entry.getValue();
            GossipDigest remoteDigest = remoteDigestMap.get(ep);

            if (remoteDigest == null) {
                // Remote does not know about this endpoint at all -> send full local state
                deltaStatesForRemote.put(ep, localState);
            } else {
                int localGen = localState.heartBeatState() != null ? localState.heartBeatState().generation() : 0;
                int localMaxVer = localState.maxVersion();

                if (localGen > remoteDigest.generation()) {
                    deltaStatesForRemote.put(ep, localState);
                } else if (localGen < remoteDigest.generation()) {
                    digestsToFetchFromRemote.add(new GossipDigest(ep, localGen, localMaxVer));
                } else {
                    // Same generation: compare versions
                    if (localMaxVer > remoteDigest.maxVersion()) {
                        EndpointState delta = localState.statesGreaterThan(remoteDigest.maxVersion());
                        if (delta != null) {
                            deltaStatesForRemote.put(ep, delta);
                        }
                    } else if (localMaxVer < remoteDigest.maxVersion()) {
                        digestsToFetchFromRemote.add(new GossipDigest(ep, localGen, localMaxVer));
                    }
                }
            }
        }

        // Check for endpoints remote knows about that local doesn't
        for (GossipDigest rd : syn.digests()) {
            if (!endpointStates.containsKey(rd.endpoint())) {
                digestsToFetchFromRemote.add(new GossipDigest(rd.endpoint(), 0, 0));
            }
        }

        GossipDigestAck ack = new GossipDigestAck(id, deltaStatesForRemote, digestsToFetchFromRemote);
        try {
            Message reply = createResponseMessage(message, ack, GOSSIP_DIGEST_ACK);
            messageBus.sendMessage(reply);
        } catch (IOException e) {
            logger.warn("{}: failed sending GossipDigestAck to {}", id, syn.from(), e);
        }
    }

    private void handleGossipDigestAck(Message message) {
        if (paused) {
            return;
        }
        GossipDigestAck ack = deserializePayload(message.payload(), GossipDigestAck.class);

        // 1. Apply delta states that remote sent to us
        applyDeltaStates(ack.deltaStates());

        // 2. Prepare delta states that remote requested from us
        Map<ProcessId, EndpointState> deltaStatesForRemote = new HashMap<>();
        for (GossipDigest digest : ack.digestsToFetch()) {
            ProcessId ep = digest.endpoint();
            EndpointState localState = endpointStates.get(ep);
            if (localState != null) {
                int localGen = localState.heartBeatState() != null ? localState.heartBeatState().generation() : 0;
                if (localGen > digest.generation() || digest.generation() == 0) {
                    deltaStatesForRemote.put(ep, localState);
                } else if (localState.maxVersion() > digest.maxVersion()) {
                    EndpointState delta = localState.statesGreaterThan(digest.maxVersion());
                    if (delta != null) {
                        deltaStatesForRemote.put(ep, delta);
                    }
                }
            }
        }

        if (!deltaStatesForRemote.isEmpty()) {
            GossipDigestAck2 ack2 = new GossipDigestAck2(id, deltaStatesForRemote);
            try {
                Message reply = createMessage(ack.from(), "ack2-" + id + "-" + currentTick, ack2, GOSSIP_DIGEST_ACK2);
                messageBus.sendMessage(reply);
            } catch (IOException e) {
                logger.warn("{}: failed sending GossipDigestAck2 to {}", id, ack.from(), e);
            }
        }
    }

    private void handleGossipDigestAck2(Message message) {
        if (paused) {
            return;
        }
        GossipDigestAck2 ack2 = deserializePayload(message.payload(), GossipDigestAck2.class);
        applyDeltaStates(ack2.deltaStates());
    }

    private void applyDeltaStates(Map<ProcessId, EndpointState> incomingDelta) {
        if (incomingDelta == null || incomingDelta.isEmpty()) {
            return;
        }
        for (Map.Entry<ProcessId, EndpointState> entry : incomingDelta.entrySet()) {
            ProcessId ep = entry.getKey();
            EndpointState incomingState = entry.getValue();

            EndpointState current = endpointStates.get(ep);
            EndpointState updated = (current == null) ? incomingState : current.merge(incomingState);
            endpointStates.put(ep, updated);

            // Notify failure detector of heartbeat progress
            failureDetector.report(ep, currentTick);
        }
    }

    // endregion

    // region Inspection & Testing Controls

    public Set<ProcessId> liveEndpoints() {
        return endpointStates.keySet().stream()
                .filter(ep -> failureDetector.isAlive(ep))
                .collect(Collectors.toSet());
    }

    public boolean isAlive(ProcessId ep) {
        return failureDetector.isAlive(ep);
    }

    public double getPhi(ProcessId ep) {
        return failureDetector.getPhi(ep, currentTick);
    }

    public ServerState getServerState(ProcessId ep) {
        return failureDetector.getState(ep);
    }

    public Map<ProcessId, EndpointState> getEndpointStates() {
        return Collections.unmodifiableMap(endpointStates);
    }

    public EndpointState getEndpointState(ProcessId ep) {
        return endpointStates.get(ep);
    }

    public void updateApplicationState(ApplicationState key, VersionedValue value) {
        EndpointState current = endpointStates.get(id);
        if (current != null) {
            endpointStates.put(id, current.withApplicationState(key, value));
        }
    }

    public void pause() {
        this.paused = true;
    }

    public void resume() {
        this.paused = false;
    }

    public boolean isPaused() {
        return paused;
    }

    public FailureDetector failureDetector() {
        return failureDetector;
    }

    public ProcessId id() {
        return id;
    }

    public long currentTick() {
        return currentTick;
    }

    // endregion
}
