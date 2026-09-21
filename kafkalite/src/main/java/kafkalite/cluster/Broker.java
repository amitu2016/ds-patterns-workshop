package kafkalite.cluster;

import java.util.Objects;

/**
 * Mirrors {@code kafka.cluster.Broker} — a broker's identity as published to ZooKeeper.
 *
 * <p>This is the value stored at {@code /brokers/ids/<id>}. Real Kafka also carries
 * multiple listener endpoints and a rack id; we keep one host/port.
 *
 * <p>Not a record: Jackson needs a no-arg constructor to read it back out of a znode.
 */
public final class Broker {
    private final int id;
    private final String host;
    private final int port;

    public Broker(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    private Broker() { this(-1, "", -1); } // for Jackson

    public int id() { return id; }
    public String host() { return host; }
    public int port() { return port; }

    public int getId() { return id; }
    public String getHost() { return host; }
    public int getPort() { return port; }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (Broker) obj;
        return this.id == that.id && Objects.equals(this.host, that.host) && this.port == that.port;
    }

    @Override
    public int hashCode() { return Objects.hash(id, host, port); }

    @Override
    public String toString() { return "Broker[id=" + id + ", host=" + host + ", port=" + port + ']'; }
}
