package kafkalite.controller;

import com.tickloom.Tickable;
import kafkalite.cluster.Broker;
import kafkalite.cluster.UpdateMetadataRequest;
import kafkalite.zookeeper.ZkEventQueue;
import kafkalite.zookeeper.ZookeeperClient;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Mirrors {@code kafka.controller.KafkaController}.
 *
 * <p>The controller is the single broker in the cluster responsible for cluster coordination:
 * <ul>
 *   <li>Electing itself via the ephemeral {@code /controller} znode.</li>
 *   <li>Tracking and monotonically incrementing {@code controllerEpoch}.</li>
 *   <li>Exclusively watching {@code /brokers/ids} (regular brokers do not watch ZooKeeper).</li>
 *   <li>Propagating membership to all live brokers via {@link UpdateMetadataRequest}.</li>
 *   <li>Failing over automatically when the active controller crashes or loses its session.</li>
 * </ul>
 *
 * <p><b>Threading model:</b> ZooKeeper delivers watch events on its internal EventThread.
 * This class enqueues every ZooKeeper callback into a {@link ZkEventQueue}, which is drained
 * exclusively on tickloom's tick thread ({@link #tick()}).
 *
 * <p><b>ListWatch Pattern (Unified with Kubelite & Kubernetes):</b>
 * This controller uses the fundamental <i>ListWatch</i> pattern:
 * <ol>
 *   <li><b>Initial List:</b> Upon becoming controller ({@link #onBecomingController()}), it lists all existing
 *       brokers and topics directly from ZooKeeper to establish baseline state.</li>
 *   <li><b>Watch (Edge Trigger):</b> It establishes watchers on {@code /brokers/ids} and {@code /brokers/topics}.
 *       Watch callbacks deliver edge-triggered invalidation notifications (via typed {@link ControllerEvent}s)
 *       carrying <i>no data payloads</i>.</li>
 *   <li><b>List on Event (Level Trigger):</b> When draining events on the tick thread ({@link #process(ControllerEvent)}),
 *       the controller ignores stale watch snapshots and lists/pulls the authoritative, current truth directly
 *       from ZooKeeper (e.g. {@code zkClient.getAllBrokers()}, {@code zkClient.getAllTopics()}).</li>
 * </ol>
 * This mirrors the exact pattern used in {@code kubelite} and Kubernetes' {@code ListWatch} / Informer architecture
 * over etcd, ensuring level-triggered convergence and resilience against missed or coalesced notifications.
 */
public class ZkController implements Tickable {

    private static final Logger logger = LoggerFactory.getLogger(ZkController.class);

    private final int brokerId;
    private final ZookeeperClient zkClient;
    private final ZkEventQueue zkEventQueue;
    private final ControllerChannelManager channelManager;
    private final Consumer<UpdateMetadataRequest> metadataBroadcaster;
    private final BiConsumer<kafkalite.cluster.LeaderAndIsrRequest, Integer> leaderAndIsrSender;

    private final BrokerChangeHandler brokerChangeListener = new BrokerChangeHandler();
    private final ControllerChangeHandler controllerChangeListener = new ControllerChangeHandler();
    private final TopicChangeHandler topicChangeListener = new TopicChangeHandler();

    private final Set<Broker> liveBrokers = new HashSet<>();
    private final Set<String> knownTopics = new HashSet<>();
    private final Map<kafkalite.common.TopicAndPartition, kafkalite.cluster.PartitionStateInfo> partitionAssignments = new java.util.LinkedHashMap<>();

    private int epoch = 0;
    private int activeControllerId = -1;
    private boolean isController = false;

    // Recorded for verification in Demo 2.1:
    private volatile String lastZkCallbackThread;
    private volatile String lastDrainedThread;

    public ZkController(int brokerId, ZookeeperClient zkClient, ZkEventQueue zkEventQueue,
                        ControllerChannelManager channelManager) {
        this.brokerId = brokerId;
        this.zkClient = Objects.requireNonNull(zkClient, "zkClient");
        this.zkEventQueue = Objects.requireNonNull(zkEventQueue, "zkEventQueue");
        this.channelManager = channelManager;
        this.metadataBroadcaster = null;
        this.leaderAndIsrSender = null;
    }

    public ZkController(int brokerId, ZookeeperClient zkClient, ZkEventQueue zkEventQueue,
                        Consumer<UpdateMetadataRequest> metadataBroadcaster,
                        BiConsumer<kafkalite.cluster.LeaderAndIsrRequest, Integer> leaderAndIsrSender) {
        this.brokerId = brokerId;
        this.zkClient = Objects.requireNonNull(zkClient, "zkClient");
        this.zkEventQueue = Objects.requireNonNull(zkEventQueue, "zkEventQueue");
        this.channelManager = null;
        this.metadataBroadcaster = metadataBroadcaster;
        this.leaderAndIsrSender = leaderAndIsrSender != null ? leaderAndIsrSender : (req, id) -> {};
    }

    public ZkController(int brokerId, ZookeeperClient zkClient, ZkEventQueue zkEventQueue,
                        Consumer<UpdateMetadataRequest> metadataBroadcaster) {
        this(brokerId, zkClient, zkEventQueue, metadataBroadcaster, (req, id) -> {});
    }

    /**
     * Starts the controller during broker startup:
     * 1. Subscribes to {@code /controller} changes (so we notice if the controller dies).
     * 2. Attempts election.
     */
    public void startup() {
        zkClient.subscribeControllerChangeListener(controllerChangeListener);
        elect();
    }

    /**
     * Attempts to elect this broker as cluster controller.
     */
    public void elect() {
        try {
            this.epoch = zkClient.registerControllerAndIncrementControllerEpoch(brokerId);
            this.isController = true;
            this.activeControllerId = brokerId;
            logger.info("Broker {} successfully elected as controller with epoch {}", brokerId, epoch);
            onBecomingController();
        } catch (ControllerExistsException e) {
            this.isController = false;
            this.activeControllerId = e.controllerId();
            this.epoch = zkClient.getControllerEpoch();
            logger.info("Broker {} lost controller election; active controller is broker {} (epoch {})",
                    brokerId, activeControllerId, epoch);
        }
    }

    /**
     * Called when this broker successfully wins controller election.
     */
    private void onBecomingController() {
        // 1. Subscribe to broker membership changes
        zkClient.subscribeBrokerChangeListener(brokerChangeListener);

        // 2. Subscribe to topic creation changes
        zkClient.subscribeTopicChangeListener(topicChangeListener);

        // 3. Discover currently registered brokers
        liveBrokers.clear();
        liveBrokers.addAll(zkClient.getAllBrokers());

        // 4. Discover existing topics and elect partition leaders
        knownTopics.clear();
        for (String topic : zkClient.getAllTopics()) {
            knownTopics.add(topic);
            List<kafkalite.zookeeper.PartitionInfo> partitionInfos = zkClient.getPartitionInfoForTopic(topic);
            if (!partitionInfos.isEmpty()) {
                handleNewTopic(topic, partitionInfos);
            }
        }

        // 5. Push initial metadata to all live brokers
        broadcastMetadata();
    }

    /**
     * Called when this broker resigns controller duties (e.g. session loss or failover).
     */
    public void onResigning() {
        if (!isController) {
            return;
        }
        isController = false;
        zkClient.unsubscribeBrokerChangeListener(brokerChangeListener);
        zkClient.unsubscribeTopicChangeListener(topicChangeListener);
    }

    /**
     * Drains enqueued ZooKeeper events on tickloom's tick thread.
     */
    @Override
    public void tick() {
        zkEventQueue.drain(this::process);
    }

    public void onTick() {
        tick();
    }

    /**
     * Broadcasts {@link UpdateMetadataRequest} to all live brokers over the network.
     */
    public void broadcastMetadata() {
        UpdateMetadataRequest request = new UpdateMetadataRequest(
                brokerId,
                epoch,
                List.copyOf(liveBrokers)
        );
        if (channelManager != null) {
            channelManager.sendUpdateMetadata(liveBrokers, request);
        }
        if (metadataBroadcaster != null) {
            metadataBroadcaster.accept(request);
        }
    }

    /**
     * Handles newly detected topic:
     * 1. Elects partition leaders (first replica in the list).
     * 2. Groups assignments by broker.
     * 3. Dispatches {@link kafkalite.cluster.LeaderAndIsrRequest} to each replica broker over the network.
     * 4. Updates metadata across the cluster.
     */
    public void handleNewTopic(String topicName, List<kafkalite.zookeeper.PartitionInfo> partitionInfos) {
        logger.info("Controller {} handling new topic: '{}' with {} partitions", brokerId, topicName, partitionInfos.size());
        List<kafkalite.cluster.LeaderAndReplicas> leaderAndReplicasList = selectLeadersForPartitions(topicName, partitionInfos);
        for (kafkalite.cluster.LeaderAndReplicas lr : leaderAndReplicasList) {
            partitionAssignments.put(lr.topicPartition(), lr.partitionStateInfo());
        }

        // Group assignments by recipient broker
        Map<Integer, List<kafkalite.cluster.LeaderAndReplicas>> brokerToAssignments = new java.util.LinkedHashMap<>();
        for (kafkalite.cluster.LeaderAndReplicas lr : leaderAndReplicasList) {
            for (Broker replica : lr.partitionStateInfo().allReplicas()) {
                brokerToAssignments.computeIfAbsent(replica.id(), k -> new ArrayList<>()).add(lr);
            }
        }

        // Send LeaderAndIsrRequest to each replica broker strictly over the network
        for (Map.Entry<Integer, List<kafkalite.cluster.LeaderAndReplicas>> entry : brokerToAssignments.entrySet()) {
            int targetBrokerId = entry.getKey();
            List<kafkalite.cluster.LeaderAndReplicas> assignments = entry.getValue();
            kafkalite.cluster.LeaderAndIsrRequest request = new kafkalite.cluster.LeaderAndIsrRequest(brokerId, epoch, assignments);
            if (channelManager != null) {
                channelManager.sendLeaderAndIsr(targetBrokerId, request);
            }
            if (leaderAndIsrSender != null) {
                leaderAndIsrSender.accept(request, targetBrokerId);
            }
            logger.info("Controller {} sent LeaderAndIsr to broker {} with {} assignments (epoch {})",
                    brokerId, targetBrokerId, assignments.size(), epoch);
        }

        broadcastMetadata();
    }

    /**
     * Elects the first replica as leader, pairing with full replica broker objects.
     */
    public List<kafkalite.cluster.LeaderAndReplicas> selectLeadersForPartitions(String topicName,
                                                                                List<kafkalite.zookeeper.PartitionInfo> partitionInfos) {
        List<kafkalite.cluster.LeaderAndReplicas> result = new ArrayList<>();
        for (kafkalite.zookeeper.PartitionInfo info : partitionInfos) {
            int partitionId = info.partitionId();
            List<Integer> replicaIds = info.brokerIds();
            if (replicaIds.isEmpty()) {
                logger.warn("No replicas defined for {}-{}", topicName, partitionId);
                continue;
            }
            int leaderBrokerId = replicaIds.get(0);

            List<Broker> replicaBrokers = new ArrayList<>();
            for (int rId : replicaIds) {
                Broker b = findBroker(rId);
                if (b != null) {
                    replicaBrokers.add(b);
                } else {
                    replicaBrokers.add(new Broker(rId, "localhost", 9092));
                }
            }

            kafkalite.common.TopicAndPartition tp = new kafkalite.common.TopicAndPartition(topicName, partitionId);
            kafkalite.cluster.PartitionStateInfo stateInfo = new kafkalite.cluster.PartitionStateInfo(leaderBrokerId, replicaBrokers);
            result.add(new kafkalite.cluster.LeaderAndReplicas(tp, stateInfo));
            logger.info("Elected broker {} as leader for {} with replicas: {}", leaderBrokerId, tp, replicaIds);
        }
        return result;
    }

    private Broker findBroker(int brokerId) {
        for (Broker b : liveBrokers) {
            if (b.id() == brokerId) return b;
        }
        return null;
    }

    // region Event Processing & Watch Handlers (enqueue on ZK thread, process on tick thread)

    /**
     * Processes a typed controller event on tickloom's tick thread.
     *
     * <p><b>ListWatch / Cache Invalidation Pattern (Kubelite & K8s):</b>
     * Watch callbacks carry no data; they serve purely as edge-triggered invalidation signals.
     * When processed here on the tick thread, the controller performs a List operation directly
     * against ZooKeeper, pulling the authoritative, current truth (reconciliation loop).
     */
    public void process(ControllerEvent event) {
        lastDrainedThread = Thread.currentThread().getName();
        logger.debug("Controller {} processing event on tick thread: {}", brokerId, event);
        switch (event) {
            case ControllerEvent.BrokerChange ignored -> processBrokerChange();
            case ControllerEvent.TopicChange ignored  -> processTopicChange();
            case ControllerEvent.ControllerUpdated ignored -> processControllerUpdated();
            case ControllerEvent.ControllerDeleted ignored -> processControllerDeleted();
        }
    }

    private void processBrokerChange() {
        if (!isController) {
            return;
        }
        logger.debug("Broker {} handling broker change on tick thread", brokerId);
        liveBrokers.clear();
        liveBrokers.addAll(zkClient.getAllBrokers());
        broadcastMetadata();
    }

    private void processTopicChange() {
        if (!isController) {
            return;
        }
        // Pull fresh topics directly from ZooKeeper
        List<String> allTopics = zkClient.getAllTopics();
        logger.debug("Controller {} handling topic change on tick thread: {}", brokerId, allTopics);
        Set<String> newTopics = new HashSet<>(allTopics);
        newTopics.removeAll(knownTopics);

        knownTopics.clear();
        knownTopics.addAll(allTopics);

        for (String topicName : newTopics) {
            List<kafkalite.zookeeper.PartitionInfo> partitionInfos = zkClient.getPartitionInfoForTopic(topicName);
            if (!partitionInfos.isEmpty()) {
                handleNewTopic(topicName, partitionInfos);
            }
        }
    }

    private void processControllerUpdated() {
        activeControllerId = zkClient.getControllerId();
        epoch = zkClient.getControllerEpoch();
    }

    private void processControllerDeleted() {
        logger.info("Broker {} observed /controller deleted; attempting re-election on tick", brokerId);
        if (isController) {
            onResigning();
        }
        elect();
    }

    class BrokerChangeHandler implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChildren) {
            lastZkCallbackThread = Thread.currentThread().getName();
            zkEventQueue.enqueue(new ControllerEvent.BrokerChange());
        }
    }

    class ControllerChangeHandler implements IZkDataListener {
        @Override
        public void handleDataChange(String dataPath, Object data) {
            lastZkCallbackThread = Thread.currentThread().getName();
            zkEventQueue.enqueue(new ControllerEvent.ControllerUpdated());
        }

        @Override
        public void handleDataDeleted(String dataPath) {
            lastZkCallbackThread = Thread.currentThread().getName();
            zkEventQueue.enqueue(new ControllerEvent.ControllerDeleted());
        }
    }

    class TopicChangeHandler implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChildren) {
            lastZkCallbackThread = Thread.currentThread().getName();
            zkEventQueue.enqueue(new ControllerEvent.TopicChange());
        }
    }

    // endregion

    // region Inspection & Getters

    public int brokerId() { return brokerId; }
    public int epoch() { return epoch; }
    public int activeControllerId() { return activeControllerId; }
    public boolean isController() { return isController; }
    public Set<Broker> liveBrokers() { return Collections.unmodifiableSet(liveBrokers); }
    public Set<String> knownTopics() { return Collections.unmodifiableSet(knownTopics); }
    public Map<kafkalite.common.TopicAndPartition, kafkalite.cluster.PartitionStateInfo> partitionAssignments() {
        return Map.copyOf(partitionAssignments);
    }
    public ZkEventQueue zkEventQueue() { return zkEventQueue; }
    public ControllerChannelManager channelManager() { return channelManager; }
    public String lastZkCallbackThread() { return lastZkCallbackThread; }
    public String lastDrainedThread() { return lastDrainedThread; }

    public void close() {
        onResigning();
        zkClient.unsubscribeControllerChangeListener(controllerChangeListener);
    }

    // endregion
}
