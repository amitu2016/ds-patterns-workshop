package kafkalite.common;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A real ZooKeeper server, in-process.
 *
 * <p>Real on purpose. ZooKeeper is the Consistent Core this module teaches; replacing it
 * with a model would hollow out the lesson. Embedded rather than Docker so a demo starts
 * in milliseconds with nothing installed.
 */
public class EmbeddedZookeeper {
    private final File snapshotDir = TestUtils.tempDir();
    private final File logDir = TestUtils.tempDir();
    private final NIOServerCnxnFactory factory;
    private final int port;

    public EmbeddedZookeeper(String connectString) throws IOException {
        this.port = Integer.parseInt(connectString.split(":")[1]);
        ZooKeeperServer zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500);
        this.factory = new NIOServerCnxnFactory();
        try {
            factory.configure(new InetSocketAddress("127.0.0.1", port), 60);
            factory.startup(zookeeper);
        } catch (Exception e) {
            throw new IOException("Failed to start ZooKeeper server", e);
        }
    }

    public int port() { return port; }

    public void shutdown() {
        factory.shutdown();
        TestUtils.rm(logDir);
        TestUtils.rm(snapshotDir);
    }
}
