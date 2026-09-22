package objectstorelite.client;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import objectstorelite.protocol.*;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static objectstorelite.protocol.ObjectStoreProtocol.*;

/**
 * Thin S3 client for interacting with the objectstorelite cluster.
 *
 * <p>Modeled after MinIO's client architecture:
 * The client is a thin API caller (similar to standard S3 / AWS SDK). It does NOT
 * perform Reed-Solomon encoding or shard mapping. Instead, it sends high-level
 * object requests (PUT, GET, GET_RANGE, DELETE, GET_SIZE) to any storage node in the cluster.
 * The receiving node acts as the S3 coordinator, executing erasure coding across the cluster.
 *
 * <p>Extends {@link ClusterClient} directly and returns {@link TickCompletableFuture} for all operations.
 * Never blocks OS threads or calls {@code .join()}: in simulation and testing, synchronous
 * executions are driven via {@code cluster.tickUntilComplete(future)}, which ticks the
 * simulation until the future resolves and returns its value.
 */
public class ObjectStoreClient extends ClusterClient {

    public static final String DEFAULT_BUCKET = "default";

    private ProcessId connectedNode;

    public ObjectStoreClient(List<ProcessId> storageNodes, ProcessParams processParams) {
        super(storageNodes, processParams);
        this.connectedNode = storageNodes.isEmpty() ? null : storageNodes.get(0);
    }

    public ObjectStoreClient(List<ProcessId> storageNodes, ProcessParams processParams, ProcessId connectedNode) {
        super(storageNodes, processParams);
        this.connectedNode = Objects.requireNonNull(connectedNode, "connectedNode");
    }

    public void setConnectedNode(ProcessId connectedNode) {
        this.connectedNode = Objects.requireNonNull(connectedNode, "connectedNode");
    }

    public ProcessId connectedNode() {
        return connectedNode;
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(
                PUT_OBJECT_RESPONSE, this::handlePutObjectResponse,
                GET_OBJECT_RESPONSE, this::handleGetObjectResponse,
                GET_OBJECT_RANGE_RESPONSE, this::handleGetObjectRangeResponse,
                GET_OBJECT_SIZE_RESPONSE, this::handleGetObjectSizeResponse,
                DELETE_OBJECT_RESPONSE, this::handleDeleteObjectResponse,
                LIST_OBJECTS_RESPONSE, this::handleListObjectsResponse
        );
    }

    private void handlePutObjectResponse(Message msg) {
        PutObjectResponse resp = deserialize(msg.payload(), PutObjectResponse.class);
        handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleGetObjectResponse(Message msg) {
        GetObjectResponse resp = deserialize(msg.payload(), GetObjectResponse.class);
        handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleGetObjectRangeResponse(Message msg) {
        GetObjectRangeResponse resp = deserialize(msg.payload(), GetObjectRangeResponse.class);
        handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleListObjectsResponse(Message msg) {
        ListObjectsResponse resp = deserialize(msg.payload(), ListObjectsResponse.class);
        handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleGetObjectSizeResponse(Message msg) {
        GetObjectSizeResponse resp = deserialize(msg.payload(), GetObjectSizeResponse.class);
        handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleDeleteObjectResponse(Message msg) {
        DeleteObjectResponse resp = deserialize(msg.payload(), DeleteObjectResponse.class);
        handleResponse(msg.correlationId(), resp, msg.source());
    }

    // ==========================================
    // Asynchronous S3 Operations (return TickCompletableFuture)
    // ==========================================

    public TickCompletableFuture<PutObjectResponse> putObject(String key, byte[] data) {
        return putObject(DEFAULT_BUCKET, key, data, false);
    }

    public TickCompletableFuture<PutObjectResponse> putObject(String bucket, String key, byte[] data) {
        return putObject(bucket, key, data, false);
    }

    public TickCompletableFuture<PutObjectResponse> putObject(String bucket, String key, byte[] data, boolean ifNoneMatch) {
        PutObjectRequest req = new PutObjectRequest(bucket, key, data, ifNoneMatch);
        return sendRequest(req, connectedNode, PUT_OBJECT);
    }

    /**
     * Uploads an object with put-if-absent semantics (mirroring S3 {@code If-None-Match: *}).
     * Fails with {@code PreconditionFailed} if the key already exists.
     */
    public TickCompletableFuture<PutObjectResponse> putObjectIfAbsent(String key, byte[] data) {
        return putObject(DEFAULT_BUCKET, key, data, true);
    }

    public TickCompletableFuture<PutObjectResponse> putObjectIfAbsent(String bucket, String key, byte[] data) {
        return putObject(bucket, key, data, true);
    }

    public TickCompletableFuture<GetObjectResponse> getObject(String key) {
        return getObject(DEFAULT_BUCKET, key);
    }

    public TickCompletableFuture<GetObjectResponse> getObject(String bucket, String key) {
        GetObjectRequest req = new GetObjectRequest(bucket, key);
        return sendRequest(req, connectedNode, GET_OBJECT);
    }

    public TickCompletableFuture<GetObjectRangeResponse> getObjectRange(String key, long offset, int length) {
        return getObjectRange(DEFAULT_BUCKET, key, offset, length);
    }

    public TickCompletableFuture<GetObjectRangeResponse> getObjectRange(String bucket, String key, long offset, int length) {
        GetObjectRangeRequest req = new GetObjectRangeRequest(bucket, key, offset, length);
        return sendRequest(req, connectedNode, GET_OBJECT_RANGE);
    }

    public TickCompletableFuture<GetObjectSizeResponse> getObjectSize(String key) {
        return getObjectSize(DEFAULT_BUCKET, key);
    }

    public TickCompletableFuture<GetObjectSizeResponse> getObjectSize(String bucket, String key) {
        GetObjectSizeRequest req = new GetObjectSizeRequest(bucket, key);
        return sendRequest(req, connectedNode, GET_OBJECT_SIZE);
    }

    /**
     * Lists keys under a prefix. Mirrors S3 {@code ListObjectsV2}.
     *
     * <p>Expensive relative to GET: there is no key to hash, so the coordinator asks every
     * node and unions the answers. Table formats built on object storage avoid LIST where
     * they can, which is why Delta keeps a checkpoint and a _last_checkpoint hint.
     */
    public TickCompletableFuture<ListObjectsResponse> listObjects(String prefix) {
        return listObjects(DEFAULT_BUCKET, prefix);
    }

    public TickCompletableFuture<ListObjectsResponse> listObjects(String bucket, String prefix) {
        ListObjectsRequest req = new ListObjectsRequest(bucket, prefix);
        return sendRequest(req, connectedNode, LIST_OBJECTS);
    }

    public TickCompletableFuture<DeleteObjectResponse> deleteObject(String key) {
        return deleteObject(DEFAULT_BUCKET, key);
    }

    public TickCompletableFuture<DeleteObjectResponse> deleteObject(String bucket, String key) {
        DeleteObjectRequest req = new DeleteObjectRequest(bucket, key);
        return sendRequest(req, connectedNode, DELETE_OBJECT);
    }
}
