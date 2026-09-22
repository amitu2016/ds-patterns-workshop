package deltalite.store;

import com.tickloom.future.TickCompletableFuture;
import java.util.List;

/**
 * Asynchronous storage interface for deltalite over an object store.
 *
 * <p>Steps 1–3 interacted with {@code java.nio.file.Files} directly, which was fine
 * for local NVMe/SSD disks. In Step 4, storage is a network service (e.g. MinIO / S3),
 * where operations are asynchronous and blocking an OS thread per I/O is unacceptable.
 */
public interface Storage {
    TickCompletableFuture<byte[]> readObject(String path);
    TickCompletableFuture<Void> writeObject(String path, byte[] data);
    TickCompletableFuture<Void> writeObjectIfAbsent(String path, byte[] data);
    TickCompletableFuture<List<String>> listObjects(String prefix);
    TickCompletableFuture<Boolean> objectExists(String path);
    TickCompletableFuture<Long> getObjectSize(String path);
    TickCompletableFuture<byte[]> readRange(String path, long offset, int length);
}
