package kafkalite.zookeeper;

import kafkalite.cluster.Broker;
import kafkalite.common.Config;
import kafkalite.common.JsonSerDes;
import kafkalite.common.ZKStringSerializer;
import kafkalite.controller.ControllerExistsException;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Mirrors {@code kafka.zk.KafkaZkClient}.
 *
 * <p><b>Read this before deciding the class is too big.</b> Real {@code KafkaZkClient} is
 * roughly 2000 lines and is one class for every subsystem's ZooKeeper access — brokers,
 * topics, the controller, ISR, configs, ACLs, delegation tokens. This class grows the same
 * way, deliberately, so the real source is recognizable. Sections are folded by topic;
 * collapse all and expand the one being taught.
 *
 * <p>Every method here is a synchronous ZooKeeper call. Watches registered through the
 * {@code subscribe*} methods fire on ZooKeeper's event thread — callers must not mutate
 * their own state from those callbacks. See {@link ZkEventQueue}.
 */
public class ZookeeperClient {

    /** DEBUG by default so demo narrative stays clean; raise it to show ZooKeeper traffic. */
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperClient.class);

    public static final String BrokerIdsPath = "/brokers/ids";
    public static final String BrokerTopicsPath = "/brokers/topics";
    public static final String ControllerPath = "/controller";

    private final ZkConnection zkConnection;
    private final ZkClient zkClient;
    private final Config config;

    public ZookeeperClient(Config config) {
        this.config = config;
        this.zkConnection = new ZkConnection(config.getZkConnect(), config.getZkSessionTimeoutMs());
        this.zkClient = new ZkClient(
                this.zkConnection,
                config.getZkConnectionTimeoutMs(),
                new ZKStringSerializer());
        this.zkClient.subscribeStateChanges(new SessionExpireListener());
    }

    // region Topic 1 · Group Membership   (KafkaZkClient#registerBroker, #getAllBrokersInCluster)

    /** Publishes this broker at {@code /brokers/ids/<id>} as an EPHEMERAL znode. */
    public void registerSelf() {
        registerBroker(new Broker(config.getBrokerId(), config.getHostName(), config.getPort()));
    }

    /**
     * The whole of failure detection, in one word: <b>ephemeral</b>.
     *
     * <p>The znode lives exactly as long as this client's ZooKeeper session. No heartbeat
     * protocol, no timeout bookkeeping in the broker — session death removes the node and
     * every watcher is told. That is the Consistent Core providing a lease.
     */
    public void registerBroker(Broker broker) {
        String brokerData = JsonSerDes.toJson(broker);
        String brokerPath = getBrokerPath(broker.id());
        createEphemeralPath(zkClient, brokerPath, brokerData);
        logger.debug("Registered {} = {}", brokerPath, brokerData);
    }

    public Set<Broker> getAllBrokers() {
        Set<Broker> brokers = new HashSet<>();
        for (String idString : brokerIdChildren()) {
            try {
                brokers.add(getBrokerInfo(Integer.parseInt(idString)));
            } catch (ZkNoNodeException e) {
                // The broker's session ended between listing children and reading its node.
                // Benign: the next watch notification carries the settled view.
                logger.debug("Broker {} vanished mid-read", idString);
            }
        }
        return brokers;
    }

    public Set<Integer> getAllBrokerIds() {
        Set<Integer> ids = new HashSet<>();
        for (String id : brokerIdChildren()) {
            ids.add(Integer.parseInt(id));
        }
        return ids;
    }

    /**
     * Empty, not an exception, when {@code /brokers/ids} does not exist yet — no broker has
     * ever registered. Mirrors {@code KafkaZkClient#getAllBrokersInCluster}, which treats a
     * missing path as an empty cluster.
     */
    private List<String> brokerIdChildren() {
        try {
            return zkClient.getChildren(BrokerIdsPath);
        } catch (ZkNoNodeException e) {
            return List.of();
        }
    }

    public Broker getBrokerInfo(int brokerId) {
        String data = zkClient.readData(getBrokerPath(brokerId));
        return JsonSerDes.fromJson(data.getBytes(), Broker.class);
    }

    /**
     * Registers a watch on the broker set.
     *
     * <p><b>The listener runs on ZooKeeper's event thread.</b> Enqueue and return —
     * do not touch broker state here.
     */
    public Optional<List<String>> subscribeBrokerChangeListener(IZkChildListener listener) {
        return Optional.ofNullable(zkClient.subscribeChildChanges(BrokerIdsPath, listener));
    }

    public static String getBrokerPath(int brokerId) {
        return BrokerIdsPath + "/" + brokerId;
    }

    // endregion

    public static final String ControllerEpochPath = "/controller_epoch";

    public static class ControllerData {
        private int brokerId;
        private int epoch;

        public ControllerData(int brokerId, int epoch) {
            this.brokerId = brokerId;
            this.epoch = epoch;
        }

        private ControllerData() { this(-1, 0); } // for Jackson

        public int brokerId() { return brokerId; }
        public int epoch() { return epoch; }
        public int getBrokerId() { return brokerId; }
        public int getEpoch() { return epoch; }
    }

    public void subscribeControllerChangeListener(IZkDataListener listener) {
        zkClient.subscribeDataChanges(ControllerPath, listener);
    }

    public void unsubscribeControllerChangeListener(IZkDataListener listener) {
        zkClient.unsubscribeDataChanges(ControllerPath, listener);
    }

    public void unsubscribeBrokerChangeListener(IZkChildListener listener) {
        zkClient.unsubscribeChildChanges(BrokerIdsPath, listener);
    }

    private record EpochAndVersion(int epoch, int zkVersion) {}

    private EpochAndVersion getOrInitializeControllerEpoch() {
        return zkClient.retryUntilConnected(() -> {
            try {
                Stat stat = new Stat();
                byte[] data = zkConnection.readData(ControllerEpochPath, stat, false);
                int epoch = Integer.parseInt(new String(data, StandardCharsets.UTF_8).trim());
                return new EpochAndVersion(epoch, stat.getVersion());
            } catch (KeeperException.NoNodeException e) {
                try {
                    zkConnection.create(ControllerEpochPath, "0".getBytes(StandardCharsets.UTF_8), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    Stat stat = new Stat();
                    zkConnection.readData(ControllerEpochPath, stat, false);
                    return new EpochAndVersion(0, stat.getVersion());
                } catch (KeeperException.NodeExistsException exists) {
                    Stat stat = new Stat();
                    byte[] data = zkConnection.readData(ControllerEpochPath, stat, false);
                    int epoch = Integer.parseInt(new String(data, StandardCharsets.UTF_8).trim());
                    return new EpochAndVersion(epoch, stat.getVersion());
                }
            }
        });
    }

    /**
     * Mirrors {@code KafkaZkClient#registerControllerAndIncrementControllerEpoch}.
     *
     * <p>Executes an atomic ZooKeeper multi-operation transaction:
     * <ol>
     *   <li>Create ephemeral {@code /controller} with this broker's ID and new epoch.</li>
     *   <li>Set persistent {@code /controller_epoch} conditionally checking expected zkVersion.</li>
     * </ol>
     *
     * <p>Because this uses {@code ZooKeeper.multi}, both operations succeed or fail atomically:
     * <ul>
     *   <li>No ephemeral {@code /controller} znode is ever created without the epoch increment.</li>
     *   <li>No watcher ever observes a provisional or zero epoch.</li>
     *   <li>Races fail with {@link ControllerExistsException} containing the winning controller's ID.</li>
     * </ul>
     */
    public int registerControllerAndIncrementControllerEpoch(int brokerId) throws ControllerExistsException {
        EpochAndVersion current = getOrInitializeControllerEpoch();
        int newEpoch = current.epoch() + 1;
        int expectedVersion = current.zkVersion();

        ControllerData controllerData = new ControllerData(brokerId, newEpoch);
        byte[] controllerBytes = JsonSerDes.toJson(controllerData).getBytes(StandardCharsets.UTF_8);
        byte[] epochBytes = String.valueOf(newEpoch).getBytes(StandardCharsets.UTF_8);

        List<Op> ops = List.of(
                Op.create(ControllerPath, controllerBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL),
                Op.setData(ControllerEpochPath, epochBytes, expectedVersion)
        );

        try {
            zkClient.retryUntilConnected(() -> {
                zkConnection.multi(ops);
                return null;
            });
            logger.info("Broker {} successfully elected as controller with epoch {}", brokerId, newEpoch);
            return newEpoch;
        } catch (Exception e) {
            Throwable cause = e;
            while (cause.getCause() != null && !(cause instanceof KeeperException)) {
                cause = cause.getCause();
            }
            if (cause instanceof KeeperException.NodeExistsException || cause instanceof KeeperException.BadVersionException) {
                throw new ControllerExistsException(getControllerId());
            }
            if (e instanceof RuntimeException re) throw re;
            throw new RuntimeException("Failed during controller election", e);
        }
    }

    public int getControllerId() {
        try {
            String data = zkClient.readData(ControllerPath);
            if (data == null || data.isBlank()) return -1;
            if (data.trim().startsWith("{")) {
                ControllerData cd = JsonSerDes.fromJson(data.getBytes(), ControllerData.class);
                return cd.brokerId();
            }
            return Integer.parseInt(data.trim());
        } catch (ZkNoNodeException e) {
            return -1;
        }
    }

    public int getControllerEpoch() {
        try {
            String data = zkClient.readData(ControllerEpochPath);
            return data != null ? Integer.parseInt(data.trim()) : 0;
        } catch (ZkNoNodeException e) {
            return 0;
        }
    }

    // endregion

    // region Topic 3 · Partitioning & Topic Management

    public static String getTopicPath(String topicName) {
        return BrokerTopicsPath + "/" + topicName;
    }

    public void setPartitionInfoForTopic(String topicName, List<PartitionInfo> partitionInfos) {
        String topicsPath = getTopicPath(topicName);
        String topicsData = JsonSerDes.toJson(partitionInfos);
        createPersistentPath(zkClient, topicsPath, topicsData);
        logger.debug("Stored partition assignments for topic {} at {}: {}", topicName, topicsPath, topicsData);
    }

    public List<PartitionInfo> getPartitionInfoForTopic(String topicName) {
        try {
            String data = zkClient.readData(getTopicPath(topicName));
            if (data == null || data.isBlank()) return List.of();
            return JsonSerDes.fromJson(data.getBytes(StandardCharsets.UTF_8),
                    new com.fasterxml.jackson.core.type.TypeReference<List<PartitionInfo>>() {});
        } catch (ZkNoNodeException e) {
            return List.of();
        }
    }

    public List<String> getAllTopics() {
        try {
            return zkClient.getChildren(BrokerTopicsPath);
        } catch (ZkNoNodeException e) {
            return List.of();
        }
    }

    public Optional<List<String>> subscribeTopicChangeListener(IZkChildListener listener) {
        createParentPath(zkClient, BrokerTopicsPath + "/dummy");
        return Optional.ofNullable(zkClient.subscribeChildChanges(BrokerTopicsPath, listener));
    }

    public void unsubscribeTopicChangeListener(IZkChildListener listener) {
        zkClient.unsubscribeChildChanges(BrokerTopicsPath, listener);
    }

    // endregion

    // region ZooKeeper plumbing

    private void createPersistentPath(ZkClient client, String path, String data) {
        try {
            if (client.exists(path)) {
                client.writeData(path, data);
            } else {
                client.createPersistent(path, data);
            }
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createPersistent(path, data);
        }
    }

    private void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    private void createParentPath(ZkClient client, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (!parentDir.isEmpty()) {
            client.createPersistent(parentDir, true);
        }
    }

    /** Test/demo seam: lets a demo end a session and show the ephemeral node vanish. */
    public void close() {
        zkClient.close();
    }

    /** Mirrors {@code KafkaZkClient}'s session state listener. */
    class SessionExpireListener implements IZkStateListener {
        @Override
        public void handleStateChanged(Watcher.Event.KeeperState state) {
            logger.debug("ZooKeeper session state: {}", state);
        }

        @Override
        public void handleNewSession() {
            logger.debug("New ZooKeeper session; re-registering broker");
            registerSelf();
        }

        @Override
        public void handleSessionEstablishmentError(Throwable error) {
            logger.error("Could not establish ZooKeeper session", error);
        }
    }

    // endregion
}
