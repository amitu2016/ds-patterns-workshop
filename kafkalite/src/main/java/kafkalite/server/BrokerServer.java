package kafkalite.server;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import kafkalite.api.FetchIsolation;
import kafkalite.api.FetchRequest;
import kafkalite.api.FetchResponse;
import kafkalite.api.ProduceRequest;
import kafkalite.api.ProduceResponse;
import kafkalite.cluster.Broker;
import kafkalite.cluster.LeaderAndIsrRequest;
import kafkalite.cluster.LeaderAndReplicas;
import kafkalite.cluster.PartitionStateInfo;
import kafkalite.cluster.ReplicaManager;
import kafkalite.cluster.UpdateMetadataRequest;
import kafkalite.common.Config;
import kafkalite.common.TopicAndPartition;
import kafkalite.controller.ControllerChannelManager;
import kafkalite.controller.ZkController;
import kafkalite.partition.Partition;
import kafkalite.zookeeper.ZkEventQueue;
import kafkalite.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Mirrors {@code kafka.server.KafkaServer} — one broker.
 *
 * <p><b>What a broker does NOT do:</b> it does not watch {@code /brokers/ids}. That watch
 * belongs to the controller alone ({@code KafkaController}'s {@code BrokerChangeHandler}).
 * A regular broker learns who else is alive from the controller's {@code UpdateMetadata}
 * request, which fills its metadata cache. Only one node reads cluster membership from the
 * Consistent Core; everyone else is told.
 *
 * <p>That asymmetry is the whole point of Topic 2, so it is worth not blurring here.
 *
 * <p>Registration happens in this process's own lifecycle ({@link #onStart()}, which tickloom
 * calls once every process has been constructed), so starting a cluster is all it takes to have
 * a registered cluster.
 *
 * <p><b>Deliberate divergence.</b> Real {@code KafkaServer} runs a socket acceptor pool, a
 * {@code KafkaRequestHandlerPool} of workers, and per-broker replica fetcher threads. Here
 * everything is collapsed into one tickloom tick loop. See MAPPING.md.
 */
public class BrokerServer extends Replica {

    private static final Logger logger = LoggerFactory.getLogger(BrokerServer.class);

    public static final MessageType UPDATE_METADATA = MessageType.of("UpdateMetadata");
    public static final MessageType LEADER_AND_ISR = MessageType.of("LeaderAndIsr");
    public static final MessageType PRODUCE = MessageType.of("Produce");
    public static final MessageType PRODUCE_RESPONSE = MessageType.of("ProduceResponse");
    public static final MessageType FETCH = MessageType.of("Fetch");
    public static final MessageType FETCH_RESPONSE = MessageType.of("FetchResponse");

    private final Config config;
    private final ZookeeperClient zk;
    private final ZkController controller;
    private final ReplicaManager replicaManager;

    /**
     * Mirrors {@code kafka.server.MetadataCache}. Populated only by {@code UpdateMetadata}
     * from the controller — never from ZooKeeper directly.
     */
    private final List<Broker> aliveBrokers = new ArrayList<>();
    private final Map<TopicAndPartition, PartitionStateInfo> partitionAssignments = new LinkedHashMap<>();

    private int currentControllerEpoch = 0;
    private int currentControllerId = -1;
    private boolean epochFencingEnabled = true;
    private int staleRequestCount = 0;
    private int acceptedLeaderAndIsrCount = 0;

    private TickCompletableFuture<Void> startupFuture;

    public BrokerServer(List<ProcessId> peerIds, ProcessParams processParams,
                        Config config, ZookeeperClient zk) {
        super(peerIds, processParams);
        this.config = config;
        this.zk = zk;
        this.replicaManager = new ReplicaManager(config);
        ControllerChannelManager channelManager = new ControllerChannelManager(
                this.messageBus,
                this.messageCodec,
                config.getBrokerId(),
                this::processIdFor
        );
        this.controller = new ZkController(
                config.getBrokerId(),
                zk,
                new ZkEventQueue(),
                channelManager
        );
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                UPDATE_METADATA, this::handleUpdateMetadata,
                LEADER_AND_ISR, this::handleLeaderAndIsr,
                PRODUCE, this::handleProduce,
                PRODUCE_RESPONSE, (msg) -> {
                },
                FETCH, this::handleFetch,
                FETCH_RESPONSE, this::handleFetchResponse
        );
    }

    @Override
    public TickCompletableFuture<?> onStart() {
        startupFuture = new TickCompletableFuture<>();
        zk.registerSelf();
        System.out.println(id + ": registered at " + ZookeeperClient.getBrokerPath(config.getBrokerId())
                + " (ephemeral — lives exactly as long as this ZooKeeper session)");
        controller.startup();
        startupFuture.complete(null);
        return startupFuture;
    }

    /**
     * Drives the controller and replica fetching. Registration is not here: it happens once, in
     * {@link #onStart()}. This javadoc used to describe {@code KafkaServer#startup} because
     * registration ran on the first tick before tickloom had a start hook.
     */
    @Override
    protected void onTick() {
        controller.tick();
        maybeFetchFromLeaders();

    }

    // region Inter-Broker Message Handlers

    private void handleUpdateMetadata(Message message) {
        UpdateMetadataRequest request = deserializePayload(message.payload(), UpdateMetadataRequest.class);
        if (epochFencingEnabled && request.controllerEpoch() < currentControllerEpoch) {
            System.out.println(id + ": 🛑 REJECTED stale UpdateMetadata from controller " + request.controllerId()
                    + " (epoch " + request.controllerEpoch() + " < current " + currentControllerEpoch + ")");
            staleRequestCount++;
            return;
        }
        this.currentControllerEpoch = request.controllerEpoch();
        this.currentControllerId = request.controllerId();
        updateMetadata(request.aliveBrokers());
        System.out.println(id + ": applied UpdateMetadata from controller " + request.controllerId()
                + " (epoch " + request.controllerEpoch() + "): " + aliveBrokerIds());
    }

    private void handleLeaderAndIsr(Message message) {
        LeaderAndIsrRequest request = deserializePayload(message.payload(), LeaderAndIsrRequest.class);
        if (epochFencingEnabled && request.controllerEpoch() < currentControllerEpoch) {
            System.out.println(id + ": 🛑 REJECTED stale LeaderAndIsr from controller " + request.controllerId()
                    + " with epoch " + request.controllerEpoch() + " (current epoch is " + currentControllerEpoch + ")");
            staleRequestCount++;
            return;
        }
        this.currentControllerEpoch = request.controllerEpoch();
        this.currentControllerId = request.controllerId();
        for (LeaderAndReplicas lr : request.leaderReplicas()) {
            partitionAssignments.put(lr.topicPartition(), lr.partitionStateInfo());
        }
        replicaManager.becomeLeaderOrFollower(request);
        acceptedLeaderAndIsrCount++;
        System.out.println(id + ": ✅ ACCEPTED LeaderAndIsr from controller " + request.controllerId()
                + " (epoch " + request.controllerEpoch() + ") with " + request.leaderReplicas().size() + " assignments");
    }

    private void handleProduce(Message message) {
        ProduceRequest request = deserializePayload(message.payload(), ProduceRequest.class);
        try {
            long offset = replicaManager.append(request.topicAndPartition(), request.message());
            ProduceResponse response = new ProduceResponse(request.topicAndPartition(), offset);
            Message reply = createResponseMessage(message, response, PRODUCE_RESPONSE);
            messageBus.sendMessage(reply);
        } catch (Exception e) {
            logger.warn("{}: produce failed for {}", id, request.topicAndPartition(), e);
        }
    }

    private void handleFetch(Message message) {
        FetchRequest request = deserializePayload(message.payload(), FetchRequest.class);
        TopicAndPartition tp = request.topicAndPartition();
        Partition partition = replicaManager.getPartition(tp);
        if (partition == null || !partition.isLeader()) {
            FetchResponse response = new FetchResponse(tp, List.of(), 0L, FetchResponse.NOT_LEADER_FOR_PARTITION);
            try {
                messageBus.sendMessage(createResponseMessage(message, response, FETCH_RESPONSE));
            } catch (IOException ignored) {
            }
            return;
        }

        try {
            List<kafkalite.api.Message> records = partition.read(request.offset(), request.fetchIsolation());
            if (request.isFromFollower()) {
                if (request.offset() >= 1) {
                    partition.updateReplicaOffset(request.replicaId(), request.offset() - 1);
                }
            }
            FetchResponse response = new FetchResponse(tp, records, partition.highWatermark());
            messageBus.sendMessage(createResponseMessage(message, response, FETCH_RESPONSE));
        } catch (Exception e) {
            logger.warn("{}: fetch failed for {}", id, tp, e);
        }
    }

    private final Set<TopicAndPartition> fetchInFlight = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private void handleFetchResponse(Message message) {
        FetchResponse response = deserializePayload(message.payload(), FetchResponse.class);
        TopicAndPartition tp = response.topicAndPartition();
        fetchInFlight.remove(tp);
        Partition partition = replicaManager.getPartition(tp);
        if (partition == null || partition.isLeader()) {
            return;
        }

        try {
            for (kafkalite.api.Message msg : response.messages()) {
                partition.append(msg);
            }
            partition.setHighWatermark(response.highWatermark());
        } catch (Exception e) {
            logger.warn("{}: error applying fetched messages for {}", id, tp, e);
        }
    }

    private void maybeFetchFromLeaders() {
        for (Map.Entry<TopicAndPartition, Integer> entry : replicaManager.followerPartitions().entrySet()) {
            TopicAndPartition tp = entry.getKey();
            if (fetchInFlight.contains(tp)) {
                continue;
            }
            int leaderId = entry.getValue();
            Partition partition = replicaManager.getPartition(tp);
            if (partition != null) {
                long nextFetchOffset = partition.lastOffset() + 1;
                FetchRequest request = new FetchRequest(tp, nextFetchOffset, config.getBrokerId(), FetchIsolation.LOG_END);
                ProcessId target = processIdFor(leaderId);
                try {
                    Message msg = createMessage(target, "fetch-" + tp + "-" + nextFetchOffset, request, FETCH);
                    messageBus.sendMessage(msg);
                    fetchInFlight.add(tp);
                } catch (IOException e) {
                    logger.warn("{}: failed sending fetch to leader {}", id, target, e);
                }
            }
        }
    }

    // endregion


    /**
     * Resolves a Kafka broker id to the tickloom process that hosts it.
     *
     * <p>Only broker-named processes are considered. {@link #brokerIdOf} parses trailing digits
     * from any {@code ProcessId}, so on a shared bus — the capstone runs brokers, storage nodes and
     * Spark workers together — {@code storage-1} would otherwise answer to broker id 1 and the
     * controller would send {@code LeaderAndIsr} to a storage node. That failure is quiet: the
     * recipient logs "No handler found" to stderr and the real broker simply never hears.
     */
    public ProcessId processIdFor(int brokerId) {
        ProcessId expected = ProcessId.of("broker-" + brokerId);
        for (ProcessId p : getAllNodes()) {
            if (p.equals(expected)) {
                return p;
            }
        }
        for (ProcessId p : getAllNodes()) {
            if (!p.name().startsWith("broker-")) {
                continue;
            }
            try {
                if (brokerIdOf(p) == brokerId) {
                    return p;
                }
            } catch (Exception ignored) {
            }
        }
        return expected;
    }

    public static int brokerIdOf(ProcessId processId) {
        String name = processId.name();
        int i = name.length();
        while (i > 0 && Character.isDigit(name.charAt(i - 1))) i--;
        if (i == name.length()) {
            throw new IllegalArgumentException("ProcessId '" + name + "' must end in digits");
        }
        return Integer.parseInt(name.substring(i));
    }

    // endregion

    /**
     * Mirrors how {@code SimpleKafkaApi} applies {@code UpdateMetadataRequest}.
     */
    public void updateMetadata(List<Broker> brokersFromController) {
        aliveBrokers.clear();
        aliveBrokers.addAll(brokersFromController);
    }

    // ---- inspection, for demos and assertions ----

    public List<Broker> aliveBrokers() {
        return List.copyOf(aliveBrokers);
    }

    public Set<Integer> aliveBrokerIds() {
        Set<Integer> out = new TreeSet<>();
        aliveBrokers.forEach(b -> out.add(b.id()));
        return out;
    }

    public Config config() {
        return config;
    }

    public ZookeeperClient zookeeper() {
        return zk;
    }

    public ZkController controller() {
        return controller;
    }

    public int currentControllerEpoch() {
        return currentControllerEpoch;
    }

    public int currentControllerId() {
        return currentControllerId;
    }

    public boolean isEpochFencingEnabled() {
        return epochFencingEnabled;
    }

    public void setEpochFencingEnabled(boolean enabled) {
        this.epochFencingEnabled = enabled;
    }

    public int staleRequestCount() {
        return staleRequestCount;
    }

    public int acceptedLeaderAndIsrCount() {
        return acceptedLeaderAndIsrCount;
    }

    public Map<TopicAndPartition, PartitionStateInfo> partitionAssignments() {
        return Map.copyOf(partitionAssignments);
    }

    public PartitionStateInfo getPartitionAssignment(TopicAndPartition tp) {
        return partitionAssignments.get(tp);
    }

    public int getLeaderFor(TopicAndPartition tp) {
        PartitionStateInfo info = partitionAssignments.get(tp);
        return info != null ? info.leaderBrokerId() : -1;
    }

    public boolean isLeaderFor(TopicAndPartition tp) {
        PartitionStateInfo info = partitionAssignments.get(tp);
        return info != null && info.leaderBrokerId() == config.getBrokerId();
    }

    public List<TopicAndPartition> leaderPartitions() {
        List<TopicAndPartition> leaders = new ArrayList<>();
        for (Map.Entry<TopicAndPartition, PartitionStateInfo> entry : partitionAssignments.entrySet()) {
            if (entry.getValue().leaderBrokerId() == config.getBrokerId()) {
                leaders.add(entry.getKey());
            }
        }
        return leaders;
    }

    public List<TopicAndPartition> followerPartitions() {
        List<TopicAndPartition> followers = new ArrayList<>();
        for (Map.Entry<TopicAndPartition, PartitionStateInfo> entry : partitionAssignments.entrySet()) {
            if (entry.getValue().leaderBrokerId() != config.getBrokerId()) {
                followers.add(entry.getKey());
            }
        }
        return followers;
    }

    public ReplicaManager replicaManager() {
        return replicaManager;
    }

    public long append(TopicAndPartition tp, kafkalite.api.Message message) throws IOException {
        return replicaManager.append(tp, message);
    }

    public List<kafkalite.api.Message> read(TopicAndPartition tp, long offset, FetchIsolation isolation) throws IOException {
        return replicaManager.read(tp, offset, isolation);
    }

    public long highWatermarkFor(TopicAndPartition tp) {
        Partition p = replicaManager.getPartition(tp);
        return p != null ? p.highWatermark() : 0L;
    }

    public long lastOffsetFor(TopicAndPartition tp) {
        Partition p = replicaManager.getPartition(tp);
        return p != null ? p.lastOffset() : 0L;
    }

    @Override
    public void close() throws Exception {
        controller.close();
        replicaManager.close();
        zk.close();
        super.close();
    }
}
