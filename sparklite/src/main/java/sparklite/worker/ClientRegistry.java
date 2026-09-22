package sparklite.worker;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The clients a worker can hand to a task, keyed by client type.
 *
 * <p>A real Spark executor does not hold a registry like this. Each data source keeps its own
 * process-wide pool — {@code KafkaDataConsumer}'s consumer cache, Hadoop's {@code FileSystem.CACHE}
 * — and an RDD's {@code compute} <i>pulls</i> from the pool it names at compile time, which
 * constructs the client on a miss. That works because a JVM can build a client anywhere.
 *
 * <p>tickloom cannot: a client is a {@code Process}, it needs {@code ProcessParams}, and only
 * {@code Cluster} creates those. A client can therefore only be <i>handed</i> to a worker, never
 * constructed by one, so sparklite pushes where Spark pulls. This registry plays the part DNS,
 * the NameNode and Kafka's Metadata request play upstream — it resolves a name to a connection —
 * except it is prepopulated at cluster-build time rather than queried at read time.
 *
 * <p>Values are {@code Object} because a worker hosts clients whose types sparklite cannot name:
 * {@code KafkaRDDLite} lives in the {@code integration} module. Each RDD casts in
 * {@code setClient}, where it knows its own type.
 *
 * <p>Not a model of {@code org.apache.spark.SparkEnv}, which is a fixed record of framework
 * services (block manager, shuffle manager, serializer) with no lookup by type.
 */
public final class ClientRegistry {

    private final Map<Class<?>, Object> clients = new HashMap<>();

    public ClientRegistry with(Class<?> type, Object client) {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(client, "client");
        if (!type.isInstance(client)) {
            throw new IllegalArgumentException(
                    "client registered as " + type.getSimpleName()
                            + " is a " + client.getClass().getSimpleName());
        }
        clients.put(type, client);
        return this;
    }

    /**
     * The client of the given type. Fails loudly rather than returning null: a task that silently
     * fell back to some other connection is the failure this design exists to prevent.
     */
    public Object client(Class<?> type) {
        Object client = clients.get(type);
        if (client == null) {
            throw new IllegalStateException(
                    "no " + type.getSimpleName() + " installed on this worker; installed: "
                            + (clients.isEmpty() ? "none" : describe()));
        }
        return client;
    }

    private String describe() {
        return clients.keySet().stream().map(Class::getSimpleName).collect(Collectors.joining(", "));
    }

    @Override
    public String toString() {
        return "ClientRegistry(" + (clients.isEmpty() ? "empty" : describe()) + ")";
    }
}
