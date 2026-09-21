package kafkalite.common;

import java.util.List;

/**
 * Mirrors {@code kafka.server.KafkaConfig}.
 *
 * <p>Real KafkaConfig carries several hundred settings. This keeps the handful the
 * workshop touches: broker identity, the ZooKeeper connect string, and log dirs.
 */
public class Config {
    private final int brokerId;
    private final String hostName;
    private final int port;
    private final String zkConnect;
    private final List<String> logDirs;

    public int DefaultNumPartitions = 3;
    private final int zkSessionTimeoutMs = 10000;
    private final int zkConnectionTimeoutMs = 10000;

    public Config(int brokerId, String hostName, int port, String zkConnect, List<String> logDirs) {
        if (logDirs.isEmpty()) {
            throw new IllegalArgumentException("logDirs cannot be empty");
        }
        this.brokerId = brokerId;
        this.hostName = hostName;
        this.port = port;
        this.zkConnect = zkConnect;
        this.logDirs = logDirs;
    }

    public int getBrokerId() { return brokerId; }
    public String getHostName() { return hostName; }
    public int getPort() { return port; }
    public String getZkConnect() { return zkConnect; }
    public List<String> getLogDirs() { return logDirs; }
    public int getZkSessionTimeoutMs() { return zkSessionTimeoutMs; }
    public int getZkConnectionTimeoutMs() { return zkConnectionTimeoutMs; }
}
