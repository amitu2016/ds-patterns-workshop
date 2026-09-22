package deltalite.store;

import com.tickloom.future.TickCompletableFuture;
import objectstorelite.client.ObjectStoreClient;

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of {@link Storage} backed by an {@link ObjectStoreClient}.
 *
 * <p>Translates {@code putObjectIfAbsent} precondition failures (HTTP 412 / PreconditionFailed)
 * into {@link ConcurrentModificationException}, which signals optimistic transaction conflict
 * to {@code OptimisticTransaction}.
 */
public class ObjectStoreStorage implements Storage {

    private final ObjectStoreClient client;
    private final String bucket;

    public ObjectStoreStorage(ObjectStoreClient client) {
        this(client, ObjectStoreClient.DEFAULT_BUCKET);
    }

    public ObjectStoreStorage(ObjectStoreClient client, String bucket) {
        this.client = Objects.requireNonNull(client, "client");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
    }

    public ObjectStoreClient getClient() {
        return client;
    }

    public String getBucket() {
        return bucket;
    }

    @Override
    public TickCompletableFuture<byte[]> readObject(String path) {
        return client.getObject(bucket, path).thenApply(resp -> {
            if (!resp.success()) {
                throw new RuntimeException("Failed to read " + path + ": " + resp.error());
            }
            return resp.data();
        });
    }

    @Override
    public TickCompletableFuture<Void> writeObject(String path, byte[] data) {
        return client.putObject(bucket, path, data).thenApply(resp -> {
            if (!resp.success()) {
                throw new RuntimeException("Failed to write " + path + ": " + resp.error());
            }
            return null;
        });
    }

    @Override
    public TickCompletableFuture<Void> writeObjectIfAbsent(String path, byte[] data) {
        return client.putObjectIfAbsent(bucket, path, data).thenApply(resp -> {
            if (!resp.success()) {
                if (resp.error() != null && resp.error().contains("PreconditionFailed")) {
                    throw new ConcurrentModificationException("Object already exists (If-None-Match condition failed): " + path);
                }
                throw new RuntimeException("Failed to write conditional " + path + ": " + resp.error());
            }
            return null;
        });
    }

    @Override
    public TickCompletableFuture<List<String>> listObjects(String prefix) {
        return client.listObjects(bucket, prefix).thenApply(resp -> {
            if (!resp.success()) {
                throw new RuntimeException("Failed to list prefix " + prefix + ": " + resp.error());
            }
            return resp.keys();
        });
    }

    @Override
    public TickCompletableFuture<Boolean> objectExists(String path) {
        return client.getObjectSize(bucket, path).thenApply(resp -> resp != null && resp.success());
    }

    @Override
    public TickCompletableFuture<Long> getObjectSize(String path) {
        return client.getObjectSize(bucket, path).thenApply(resp -> {
            if (!resp.success()) {
                throw new RuntimeException("Failed to get size for " + path + ": " + resp.error());
            }
            return resp.size();
        });
    }

    @Override
    public TickCompletableFuture<byte[]> readRange(String path, long offset, int length) {
        return client.getObjectRange(bucket, path, offset, length).thenApply(resp -> {
            if (!resp.success()) {
                throw new RuntimeException("Failed to read range for " + path + ": " + resp.error());
            }
            return resp.data();
        });
    }
}
