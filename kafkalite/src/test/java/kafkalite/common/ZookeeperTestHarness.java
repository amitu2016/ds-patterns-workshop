package kafkalite.common;

import com.tickloom.ProcessId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Starts a real embedded ZooKeeper per test, on a fresh port. Mirrors Kafka's own harness. */
public abstract class ZookeeperTestHarness {

    protected String zkConnect;
    protected EmbeddedZookeeper zookeeper;

    @BeforeEach
    void startZookeeper() throws Exception {
        zkConnect = "127.0.0.1:" + TestUtils.choosePort();
        zookeeper = new EmbeddedZookeeper(zkConnect);
    }

    @AfterEach
    void stopZookeeper() {
        if (zookeeper != null) zookeeper.shutdown();
    }

    protected Config configFor(int brokerId) {
        return new Config(brokerId, "127.0.0.1", TestUtils.choosePort(), zkConnect,
                List.of(TestUtils.tempDir().getAbsolutePath()));
    }

    /**
     * One {@link Config} per broker — so one ZooKeeper session and one port each, as in a real
     * deployment. Ports and log dirs are plumbing; the interesting wiring stays in the demo.
     */
    protected Map<ProcessId, Config> configsFor(ProcessId... brokers) {
        Map<ProcessId, Config> configs = new LinkedHashMap<>();
        for (ProcessId broker : brokers) {
            configs.put(broker, configFor(brokerIdOf(broker)));
        }
        return configs;
    }

    /**
     * Derives a Kafka broker id from a tickloom {@code ProcessId}, so {@code ProcessId.of("broker-2")}
     * is broker 2 and the two identity schemes line up on screen instead of needing translation.
     */
    public static int brokerIdOf(ProcessId processId) {
        String name = processId.name();
        int i = name.length();
        while (i > 0 && Character.isDigit(name.charAt(i - 1))) i--;
        if (i == name.length()) {
            throw new IllegalArgumentException("ProcessId '" + name
                    + "' must end in digits so it maps to a Kafka broker id, e.g. broker-1");
        }
        return Integer.parseInt(name.substring(i));
    }
}
