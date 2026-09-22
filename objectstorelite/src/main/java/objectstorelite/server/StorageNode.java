package objectstorelite.server;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.Replica;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import com.tickloom.messaging.RequestCallback;
import objectstorelite.codec.ErasureCodec;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.model.ObjectMeta;
import objectstorelite.model.ShardMetadata;
import objectstorelite.model.VersionEntry;
import objectstorelite.protocol.*;
import objectstorelite.storage.AtomicFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static objectstorelite.protocol.ObjectStoreProtocol.*;

/**
 * StorageNode represents a single storage server process owning an isolated disk directory.
 *
 * <p>Modeled after MinIO's symmetric architecture:
 * 1. <b>S3 Gateway / Coordinator</b>: Any node can receive client S3 requests (PUT, GET, GET_RANGE, DELETE).
 *    The coordinator maps the object key to an ErasureSet, encodes/decodes shards using Reed-Solomon,
 *    and coordinates with peer nodes over internal RPCs.
 * 2. <b>Storage Peer</b>: Stores local shard files and versioned metadata (xl.meta) on disk.
 *
 * <p>On disk, the directory structure mirrors MinIO's XL storage:
 * <pre>
 *   dataDir/
 *     buckets/
 *       photos/
 *         2025/vacation.jpg/
 *           xl.meta
 *           &lt;version-uuid&gt;/
 *             part.1
 *             shard.meta
 * </pre>
 */
public class StorageNode extends Replica {

    private static final Logger logger = LoggerFactory.getLogger(StorageNode.class);

    private final Path dataDir;
    private final ErasureSetMapper mapper;
    private final int dataShards;
    private final int parityShards;

    private volatile boolean online = true;
    private int writesHandled = 0;
    private int readsHandled = 0;

    public StorageNode(List<ProcessId> peerIds, ProcessParams processParams, Path dataDir) {
        this(peerIds, processParams, dataDir, null, 4, 2);
    }

    public StorageNode(List<ProcessId> peerIds, ProcessParams processParams, Path dataDir,
                       ErasureSetMapper mapper, int dataShards, int parityShards) {
        super(peerIds, processParams);
        this.dataDir = dataDir;
        this.dataShards = dataShards;
        this.parityShards = parityShards;
        this.mapper = (mapper != null) ? mapper : createDefaultMapper();
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create storage directory: " + dataDir, e);
        }
    }

    private ErasureSetMapper createDefaultMapper() {
        return new ErasureSetMapper(
                ErasureSetMapper.Algorithm.CRCMOD,
                null,
                List.of(new ErasureSet(0, getAllNodes()))
        );
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.ofEntries(
                // Client-facing S3 Object Operations (Coordinator role)
                Map.entry(PUT_OBJECT, this::handlePutObject),
                Map.entry(GET_OBJECT, this::handleGetObject),
                Map.entry(GET_OBJECT_RANGE, this::handleGetObjectRange),
                Map.entry(GET_OBJECT_SIZE, this::handleGetObjectSize),
                Map.entry(LIST_OBJECTS, this::handleListObjects),
                Map.entry(DELETE_OBJECT, this::handleDeleteObject),

                // Internal Cluster Shard Operations (Peer role)
                Map.entry(PUT_SHARD, this::handlePutShard),
                Map.entry(PUT_SHARD_RESPONSE, this::handlePutShardResponse),
                Map.entry(GET_SHARD, this::handleGetShard),
                Map.entry(GET_SHARD_RESPONSE, this::handleGetShardResponse),
                Map.entry(DELETE_SHARD, this::handleDeleteShard),
                Map.entry(DELETE_SHARD_RESPONSE, this::handleDeleteShardResponse),
                Map.entry(LIST_KEYS, this::handleListKeys),
                Map.entry(LIST_KEYS_RESPONSE, this::handleListKeysResponse),
                Map.entry(GET_META, this::handleGetMeta),
                Map.entry(GET_META_RESPONSE, this::handleGetMetaResponse)
        );
    }

    @Override
    protected TickCompletableFuture<?> onInit() {
        return TickCompletableFuture.completed(null);
    }

    @Override
    protected void onTick() {
        // Periodic background maintenance
    }

    public void setOnline(boolean online) {
        this.online = online;
    }

    public boolean isOnline() {
        return online;
    }

    public Path dataDir() {
        return dataDir;
    }

    public int writesHandled() {
        return writesHandled;
    }

    public int readsHandled() {
        return readsHandled;
    }

    public ErasureSetMapper mapper() {
        return mapper;
    }

    public int dataShards() {
        return dataShards;
    }

    public int parityShards() {
        return parityShards;
    }

    public Path resolveObjectDir(String bucket, String key) {
        return dataDir.resolve("buckets").resolve(bucket).resolve(key);
    }

    public Path resolveVersionDir(String bucket, String key, String versionId) {
        return resolveObjectDir(bucket, key).resolve(versionId);
    }

    public Path resolveShardFile(String bucket, String key, String versionId, int shardIndex) {
        return resolveVersionDir(bucket, key, versionId).resolve("part." + (shardIndex + 1));
    }

    public boolean hasShard(String bucket, String key, String versionId, int shardIndex) {
        return Files.exists(resolveShardFile(bucket, key, versionId, shardIndex));
    }

    // =========================================================================
    // Client-facing S3 API Handlers (Coordinator Role)
    // =========================================================================

    private void handlePutObject(Message msg) {
        if (!online) return;
        PutObjectRequest request = deserializePayload(msg.payload(), PutObjectRequest.class);
        try {
            ErasureSet set = mapper.setFor(request.key());
            if (request.ifNoneMatch()) {
                // -------------------------------------------------------------------------
                // ARCHITECTURE NOTE: how put-if-absent is actually made atomic
                //
                // Delta Lake commits by creating _delta_log/<version>.json *only if absent*. That
                // one conditional write is what makes a multi-file table update atomic, so the
                // whole table format rests on whatever the store does here. Three real systems
                // answer it three different ways.
                //
                // 1. AWS S3 -- serialise per key, no lock anywhere.
                //    S3 keeps bytes in one subsystem and a "keymap" index in another: key ->
                //    {blobId, size, etag, version}. The keymap is partitioned by key and each
                //    partition is a consensus group, so every operation on one key has a total
                //    order without anything global being ordered. A PUT writes the bytes first,
                //    under a fresh never-reused blobId, where nothing references them and nobody
                //    can see them; then proposes ONE entry to that key's partition:
                //
                //        CAS(key, expect = ABSENT, set = {blobId, size, etag})
                //
                //    The check and the write are the same log entry, applied in log order on
                //    every replica, so there is no instant between them. Lose the CAS and the
                //    bytes are orphaned garbage to be collected -- exactly Delta's orphaned
                //    parquet files when a commit loses the race.
                //
                //    Read-after-write then falls out rather than being added: a PUT does not
                //    return 200 until the CAS commits, and a GET resolves through the keymap, so
                //    an acknowledged write cannot be unseen. S3's pre-2020 eventual consistency
                //    was never in this mechanism -- it was the caches in front of the index
                //    (hence the old "GET before PUT makes it eventual" caveat: the 404 populated
                //    a negative cache) and reads served by lagging replicas.
                //
                //    AWS has not published the mechanism; the shape above is forced by the
                //    problem rather than confirmed. What is public: strong read-after-write since
                //    Dec 2020, and If-None-Match on PutObject since Aug 2024.
                //
                // 2. Delta on S3 before Aug 2024 -- borrow a CAS that *is* exposed.
                //    S3 must always have had CAS internally; it simply did not offer it to
                //    clients. So S3DynamoDBLogStore put a row in DynamoDB conditionally
                //    (attribute_not_exists) and whoever won wrote the bytes to S3. Same
                //    primitive, but arbitration now lives in a different system from the data:
                //    if the row commits and the S3 write does not, the arbiter and the storage
                //    disagree about who owns version N, and that recovery is real code. One
                //    atomic operation became two plus a reconciliation problem, which is why
                //    native conditional PUT was worth the wait.
                //
                // 3. MinIO -- a genuine distributed lock.
                //    MinIO has no single metadata authority to serialise at: several nodes must
                //    agree before shards are committed. So xl-storage/cmd/dsync takes an
                //    exclusive lock on the key across the erasure set before checking existence
                //    and writing. This is the one of the three that really is locking -- and,
                //    being a lock, it is held over time and therefore needs lease expiry, which
                //    is the fencing problem demo 2.2 is about. CAS has no interval to defend.
                //
                // WHAT objectstorelite DOES, AND WHAT IT COSTS
                //
                // Neither of the above: the coordinator checks existence and then writes, as two
                // separate asynchronous steps. The check resolves on a later tick than the write,
                // so two concurrent PUTs for the same absent key can both pass the check and both
                // commit -- a distributed TOCTOU, and a violation of the put-if-absent contract
                // Delta depends on. Demo 6.5 passes because tickloom is deterministic and the
                // demo does not interleave them, not because the race is closed.
                //
                // Closing it needs one of: a lock manager across the erasure set (MinIO's answer,
                // and a lot of machinery), or a single authority per key that orders the check and
                // the write together (S3's answer, which would mean a consensus-backed keymap
                // rather than reading existence from the shards). Both are out of scope here; the
                // gap is recorded instead so nobody mistakes this for the real guarantee.
                // -------------------------------------------------------------------------
                checkObjectExists(request.bucket(), request.key(), set, (exists, err) -> {
                    if (err != null) {
                        PutObjectResponse clientResp = PutObjectResponse.fail(request.bucket(), request.key(), err);
                        sendClientResponse(msg, clientResp, PUT_OBJECT_RESPONSE);
                    } else if (exists) {
                        PutObjectResponse clientResp = PutObjectResponse.fail(request.bucket(), request.key(), "PreconditionFailed: Object already exists");
                        sendClientResponse(msg, clientResp, PUT_OBJECT_RESPONSE);
                    } else {
                        coordinatePutObject(msg, request, set);
                    }
                });
            } else {
                coordinatePutObject(msg, request, set);
            }
        } catch (Exception e) {
            logger.warn("{}: Failed to coordinate PutObject: {}", id, e.getMessage());
            PutObjectResponse clientResp = PutObjectResponse.fail(request.bucket(), request.key(), e.getMessage());
            sendClientResponse(msg, clientResp, PUT_OBJECT_RESPONSE);
        }
    }

    private void coordinatePutObject(Message msg, PutObjectRequest request, ErasureSet set) {
        try {
            int totalShards = set.size();
            int ds = (totalShards == dataShards + parityShards) ? dataShards : (totalShards * 2 / 3);
            int ps = totalShards - ds;

            byte[][] shards = ErasureCodec.encode(request.data(), ds, ps);
            long shardSize = shards[0].length;
            String versionId = UUID.randomUUID().toString();

            AtomicInteger acks = new AtomicInteger(0);
            AtomicInteger fails = new AtomicInteger(0);
            AtomicBoolean completed = new AtomicBoolean(false);

            List<ProcessId> nodes = set.nodes();
            for (int i = 0; i < nodes.size(); i++) {
                ProcessId targetNode = nodes.get(i);
                int shardIndex = i;
                byte[] shardData = shards[shardIndex];
                ShardMetadata meta = new ShardMetadata(shardIndex, shardSize, request.data().length, ds, ps);
                PutShardRequest shardReq = new PutShardRequest(request.bucket(), request.key(), versionId, shardIndex, shardData, meta);

                TickCompletableFuture<PutShardResponse> f = sendInternalRequest(shardReq, targetNode, PUT_SHARD);
                f.whenComplete((resp, ex) -> {
                    if (ex == null && resp != null && resp.success()) {
                        if (acks.incrementAndGet() >= ds && completed.compareAndSet(false, true)) {
                            PutObjectResponse clientResp = PutObjectResponse.ok(request.bucket(), request.key(), versionId);
                            sendClientResponse(msg, clientResp, PUT_OBJECT_RESPONSE);
                        }
                    } else {
                        int fCount = fails.incrementAndGet();
                        if (fCount > ps && completed.compareAndSet(false, true)) {
                            PutObjectResponse clientResp = PutObjectResponse.fail(request.bucket(), request.key(), "Write quorum not met");
                            sendClientResponse(msg, clientResp, PUT_OBJECT_RESPONSE);
                        }
                    }
                });
            }
        } catch (Exception e) {
            logger.warn("{}: Failed to coordinate PutObject encoding: {}", id, e.getMessage());
            PutObjectResponse clientResp = PutObjectResponse.fail(request.bucket(), request.key(), e.getMessage());
            sendClientResponse(msg, clientResp, PUT_OBJECT_RESPONSE);
        }
    }

    private interface ExistsCallback {
        void onResult(boolean exists, String error);
    }

    private void checkObjectExists(String bucket, String key, ErasureSet set, ExistsCallback callback) {
        List<ProcessId> nodes = set.nodes();
        AtomicInteger checked = new AtomicInteger(0);
        AtomicBoolean found = new AtomicBoolean(false);
        AtomicBoolean completed = new AtomicBoolean(false);

        for (ProcessId node : nodes) {
            GetMetaRequest metaReq = new GetMetaRequest(bucket, key);
            TickCompletableFuture<GetMetaResponse> f = sendInternalRequest(metaReq, node, GET_META);
            f.whenComplete((resp, ex) -> {
                int c = checked.incrementAndGet();
                if (ex == null && resp != null && resp.exists() && resp.latestVersion() != null) {
                    VersionEntry latest = resp.latestVersion();
                    if (!latest.deleted() && found.compareAndSet(false, true)) {
                        if (completed.compareAndSet(false, true)) {
                            callback.onResult(true, null);
                        }
                        return;
                    }
                }
                if (c >= nodes.size() && !found.get()) {
                    if (completed.compareAndSet(false, true)) {
                        callback.onResult(false, null);
                    }
                }
            });
        }
    }

    private void handleGetObject(Message msg) {
        if (!online) return;
        GetObjectRequest request = deserializePayload(msg.payload(), GetObjectRequest.class);
        coordinateGetObject(request.bucket(), request.key(), (reconstructed, error) -> {
            if (error != null) {
                sendClientResponse(msg, GetObjectResponse.fail(request.bucket(), request.key(), error), GET_OBJECT_RESPONSE);
            } else {
                sendClientResponse(msg, GetObjectResponse.ok(request.bucket(), request.key(), reconstructed), GET_OBJECT_RESPONSE);
            }
        });
    }

    private void handleGetObjectRange(Message msg) {
        if (!online) return;
        GetObjectRangeRequest request = deserializePayload(msg.payload(), GetObjectRangeRequest.class);
        coordinateGetObject(request.bucket(), request.key(), (reconstructed, error) -> {
            if (error != null) {
                sendClientResponse(msg, GetObjectRangeResponse.fail(request.bucket(), request.key(), error), GET_OBJECT_RANGE_RESPONSE);
            } else {
                int start = (int) Math.max(0, request.offset());
                if (start >= reconstructed.length) {
                    sendClientResponse(msg, GetObjectRangeResponse.ok(request.bucket(), request.key(), new byte[0]), GET_OBJECT_RANGE_RESPONSE);
                    return;
                }
                int available = reconstructed.length - start;
                int len = (request.length() > 0) ? Math.min(request.length(), available) : available;
                byte[] slice = new byte[len];
                System.arraycopy(reconstructed, start, slice, 0, len);
                sendClientResponse(msg, GetObjectRangeResponse.ok(request.bucket(), request.key(), slice), GET_OBJECT_RANGE_RESPONSE);
            }
        });
    }

    private interface ObjectFetchCallback {
        void onResult(byte[] data, String error);
    }

    private void coordinateGetObject(String bucket, String key, ObjectFetchCallback callback) {
        try {
            ErasureSet set = mapper.setFor(key);
            int totalShards = set.size();
            int ds = (totalShards == dataShards + parityShards) ? dataShards : (totalShards * 2 / 3);
            int ps = totalShards - ds;

            byte[][] shards = new byte[totalShards][];
            boolean[] shardPresent = new boolean[totalShards];
            AtomicInteger available = new AtomicInteger(0);
            AtomicInteger failed = new AtomicInteger(0);
            AtomicBoolean completed = new AtomicBoolean(false);
            AtomicLong objSize = new AtomicLong(-1);

            List<ProcessId> nodes = set.nodes();
            for (int i = 0; i < nodes.size(); i++) {
                ProcessId targetNode = nodes.get(i);
                int shardIndex = i;
                GetShardRequest shardReq = GetShardRequest.full(bucket, key, null, shardIndex);

                TickCompletableFuture<GetShardResponse> f = sendInternalRequest(shardReq, targetNode, GET_SHARD);
                f.whenComplete((resp, ex) -> {
                    if (ex == null && resp != null && resp.success()) {
                        shards[shardIndex] = resp.shardData();
                        shardPresent[shardIndex] = true;
                        if (resp.metadata() != null) {
                            objSize.compareAndSet(-1, resp.metadata().objectSize());
                        }
                        int count = available.incrementAndGet();
                        if (count >= ds && completed.compareAndSet(false, true)) {
                            try {
                                long size = objSize.get();
                                if (size < 0) {
                                    for (byte[] s : shards) {
                                        if (s != null) {
                                            size = (long) s.length * ds;
                                            break;
                                        }
                                    }
                                }
                                byte[] reconstructed = ErasureCodec.decode(shards, shardPresent, size, ds, ps);
                                callback.onResult(reconstructed, null);
                            } catch (Exception e) {
                                callback.onResult(null, e.getMessage());
                            }
                        }
                    } else {
                        int fCount = failed.incrementAndGet();
                        if (fCount > ps && completed.compareAndSet(false, true)) {
                            callback.onResult(null, "Read quorum failed: " + fCount + " nodes unavailable");
                        }
                    }
                });
            }
        } catch (Exception e) {
            callback.onResult(null, e.getMessage());
        }
    }

    private void handleGetObjectSize(Message msg) {
        if (!online) return;
        GetObjectSizeRequest request = deserializePayload(msg.payload(), GetObjectSizeRequest.class);
        try {
            ErasureSet set = mapper.setFor(request.key());
            List<ProcessId> nodes = set.nodes();
            AtomicInteger checked = new AtomicInteger(0);
            AtomicBoolean found = new AtomicBoolean(false);

            for (ProcessId node : nodes) {
                GetMetaRequest metaReq = new GetMetaRequest(request.bucket(), request.key());
                TickCompletableFuture<GetMetaResponse> f = sendInternalRequest(metaReq, node, GET_META);
                f.whenComplete((resp, ex) -> {
                    int c = checked.incrementAndGet();
                    if (ex == null && resp != null && resp.exists() && resp.latestVersion() != null) {
                        VersionEntry latest = resp.latestVersion();
                        if (!latest.deleted() && found.compareAndSet(false, true)) {
                            sendClientResponse(msg, GetObjectSizeResponse.ok(request.bucket(), request.key(), latest.objectSize()), GET_OBJECT_SIZE_RESPONSE);
                            return;
                        }
                    }
                    if (c >= nodes.size() && !found.get()) {
                        sendClientResponse(msg, GetObjectSizeResponse.fail(request.bucket(), request.key(), "Object not found or deleted"), GET_OBJECT_SIZE_RESPONSE);
                    }
                });
            }
        } catch (Exception e) {
            sendClientResponse(msg, GetObjectSizeResponse.fail(request.bucket(), request.key(), e.getMessage()), GET_OBJECT_SIZE_RESPONSE);
        }
    }

    /**
     * Client-facing LIST. Mirrors S3 {@code ListObjectsV2} / MinIO's listing.
     *
     * <p>Unlike GET, there is no key to hash, so there is no single erasure set to ask.
     * Every node holds a slice of the keyspace, so the coordinator must ask all of them and
     * union the answers — which is why LIST is the expensive operation on an object store,
     * and why table formats work hard to avoid it.
     */
    private void handleListObjects(Message msg) {
        if (!online) return;
        ListObjectsRequest request = deserializePayload(msg.payload(), ListObjectsRequest.class);
        try {
            // Storage nodes only — NOT getAllNodes(). On a shared bus that would send
            // ListKeys to brokers and Spark workers, which have no handler for it, and the
            // listing would silently miss whatever those ticks were waiting on.
            List<ProcessId> nodes = mapper.allNodes();
            Set<String> union = java.util.concurrent.ConcurrentHashMap.newKeySet();
            AtomicInteger answered = new AtomicInteger(0);
            AtomicBoolean replied = new AtomicBoolean(false);

            for (ProcessId node : nodes) {
                ListKeysRequest keysReq = new ListKeysRequest(request.bucket(), request.prefix());
                TickCompletableFuture<ListKeysResponse> f = sendInternalRequest(keysReq, node, LIST_KEYS);
                f.whenComplete((resp, ex) -> {
                    if (ex == null && resp != null && resp.success()) {
                        union.addAll(resp.keys());
                    }
                    // Reply once every node has answered. A node that fails contributes
                    // nothing rather than failing the whole listing — the same degraded-read
                    // posture the erasure coding takes.
                    if (answered.incrementAndGet() >= nodes.size() && replied.compareAndSet(false, true)) {
                        List<String> keys = new java.util.ArrayList<>(union);
                        java.util.Collections.sort(keys);
                        sendClientResponse(msg, ListObjectsResponse.ok(request.bucket(), request.prefix(), keys),
                                LIST_OBJECTS_RESPONSE);
                    }
                });
            }
        } catch (Exception e) {
            sendClientResponse(msg, ListObjectsResponse.fail(request.bucket(), request.prefix(), e.getMessage()),
                    LIST_OBJECTS_RESPONSE);
        }
    }

    /** Peer role: the keys this node holds locally, found by walking for xl.meta files. */
    private void handleListKeys(Message msg) {
        if (!online) return;
        ListKeysRequest request = deserializePayload(msg.payload(), ListKeysRequest.class);
        try {
            Path bucketDir = dataDir.resolve("buckets").resolve(request.bucket());
            List<String> keys = new java.util.ArrayList<>();
            if (Files.exists(bucketDir)) {
                try (var walk = Files.walk(bucketDir)) {
                    walk.filter(pth -> pth.getFileName().toString().equals("xl.meta"))
                        .map(pth -> bucketDir.relativize(pth.getParent()).toString())
                        .filter(key -> key.startsWith(request.prefix()))
                        .forEach(keys::add);
                }
            }
            messageBus.sendMessage(createResponseMessage(msg, ListKeysResponse.ok(keys), LIST_KEYS_RESPONSE));
        } catch (Exception e) {
            try {
                messageBus.sendMessage(createResponseMessage(msg, ListKeysResponse.fail(e.getMessage()), LIST_KEYS_RESPONSE));
            } catch (IOException ignored) {}
        }
    }

    private void handleListKeysResponse(Message msg) {
        ListKeysResponse resp = deserializePayload(msg.payload(), ListKeysResponse.class);
        waitingList.handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleDeleteObject(Message msg) {
        if (!online) return;
        DeleteObjectRequest request = deserializePayload(msg.payload(), DeleteObjectRequest.class);
        try {
            ErasureSet set = mapper.setFor(request.key());
            String versionId = UUID.randomUUID().toString();
            AtomicInteger acks = new AtomicInteger(0);
            AtomicBoolean completed = new AtomicBoolean(false);

            for (ProcessId node : set.nodes()) {
                DeleteShardRequest shardReq = new DeleteShardRequest(request.bucket(), request.key(), versionId);
                TickCompletableFuture<DeleteShardResponse> f = sendInternalRequest(shardReq, node, DELETE_SHARD);
                f.whenComplete((resp, ex) -> {
                    if (ex == null && resp != null && resp.success()) {
                        if (acks.incrementAndGet() >= dataShards && completed.compareAndSet(false, true)) {
                            sendClientResponse(msg, DeleteObjectResponse.ok(request.bucket(), request.key(), versionId), DELETE_OBJECT_RESPONSE);
                        }
                    }
                });
            }
        } catch (Exception e) {
            sendClientResponse(msg, DeleteObjectResponse.fail(request.bucket(), request.key(), e.getMessage()), DELETE_OBJECT_RESPONSE);
        }
    }

    private void sendClientResponse(Message incomingMsg, Object responsePayload, MessageType type) {
        try {
            Message responseMsg = createResponseMessage(incomingMsg, responsePayload, type);
            messageBus.sendMessage(responseMsg);
        } catch (IOException e) {
            logger.warn("{}: Failed to send client response: {}", id, e.getMessage());
        }
    }

    private <T> TickCompletableFuture<T> sendInternalRequest(Object request, ProcessId destination, MessageType messageType) {
        String correlationId = idGen.generateCorrelationId("internal");
        TickCompletableFuture<T> future = new TickCompletableFuture<>();
        waitingList.add(correlationId, new RequestCallback<Object>() {
            @SuppressWarnings("unchecked")
            @Override
            public void onResponse(Object response, ProcessId fromNode) {
                future.complete((T) response);
            }

            @Override
            public void onError(Exception error) {
                future.fail(error);
            }
        });

        Message msg = createMessage(destination, correlationId, request, messageType);
        try {
            messageBus.sendMessage(msg);
        } catch (IOException e) {
            waitingList.handleError(correlationId, e);
        }
        return future;
    }

    private void handlePutShardResponse(Message msg) {
        PutShardResponse resp = deserializePayload(msg.payload(), PutShardResponse.class);
        waitingList.handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleGetShardResponse(Message msg) {
        GetShardResponse resp = deserializePayload(msg.payload(), GetShardResponse.class);
        waitingList.handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleDeleteShardResponse(Message msg) {
        DeleteShardResponse resp = deserializePayload(msg.payload(), DeleteShardResponse.class);
        waitingList.handleResponse(msg.correlationId(), resp, msg.source());
    }

    private void handleGetMetaResponse(Message msg) {
        GetMetaResponse resp = deserializePayload(msg.payload(), GetMetaResponse.class);
        waitingList.handleResponse(msg.correlationId(), resp, msg.source());
    }

    // =========================================================================
    // Internal Disk / Shard Storage Handlers (Peer Role)
    // =========================================================================

    private void handlePutShard(Message msg) {
        if (!online) {
            return;
        }
        PutShardRequest request = deserializePayload(msg.payload(), PutShardRequest.class);
        try {
            Path versionDir = resolveVersionDir(request.bucket(), request.key(), request.versionId());
            Files.createDirectories(versionDir);

            // 1. Write shard data atomically via temp file and atomic rename
            Path targetShardFile = resolveShardFile(request.bucket(), request.key(), request.versionId(), request.shardIndex());
            AtomicFiles.writeBytesAtomic(targetShardFile, request.shardData() != null ? request.shardData() : new byte[0]);

            // 2. Write shard metadata atomically if present
            if (request.metadata() != null) {
                Path metaFile = versionDir.resolve("shard.meta");
                AtomicFiles.writeStringAtomic(metaFile, request.metadata().serialize());
            }

            // 3. Update xl.meta on disk
            Path objectDir = resolveObjectDir(request.bucket(), request.key());
            Path xlMetaFile = objectDir.resolve("xl.meta");
            ObjectMeta objectMeta = ObjectMeta.read(xlMetaFile);

            long shardSize = request.shardData() != null ? request.shardData().length : 0;
            long objectSize = request.metadata() != null ? request.metadata().objectSize() : shardSize;
            int dataShards = request.metadata() != null ? request.metadata().dataShards() : 1;
            int parityShards = request.metadata() != null ? request.metadata().parityShards() : 0;

            VersionEntry entry = VersionEntry.createWithId(
                    request.versionId(),
                    objectSize,
                    dataShards,
                    parityShards,
                    shardSize,
                    List.of("part." + (request.shardIndex() + 1))
            );
            objectMeta.add(entry);
            objectMeta.write(xlMetaFile);

            writesHandled++;
            PutShardResponse response = PutShardResponse.ok(request.bucket(), request.key(), request.shardIndex());
            messageBus.sendMessage(createResponseMessage(msg, response, PUT_SHARD_RESPONSE));
        } catch (Exception e) {
            logger.warn("{}: Failed to store shard for {}/{}: {}", id, request.bucket(), request.key(), e.getMessage());
            PutShardResponse response = PutShardResponse.fail(request.bucket(), request.key(), request.shardIndex(), e.getMessage());
            try {
                messageBus.sendMessage(createResponseMessage(msg, response, PUT_SHARD_RESPONSE));
            } catch (IOException ignored) {}
        }
    }

    private void handleGetShard(Message msg) {
        if (!online) {
            return;
        }
        GetShardRequest request = deserializePayload(msg.payload(), GetShardRequest.class);
        try {
            Path objectDir = resolveObjectDir(request.bucket(), request.key());
            Path xlMetaFile = objectDir.resolve("xl.meta");
            if (!Files.exists(xlMetaFile)) {
                sendGetShardFail(msg, request, "Object not found: " + request.key());
                return;
            }

            ObjectMeta meta = ObjectMeta.read(xlMetaFile);
            Optional<VersionEntry> versionOpt = (request.versionId() != null)
                    ? meta.findVersion(request.versionId())
                    : meta.latestVersion();

            if (versionOpt.isEmpty() || versionOpt.get().deleted()) {
                sendGetShardFail(msg, request, "Version not found or deleted");
                return;
            }

            VersionEntry version = versionOpt.get();
            Path shardFile = resolveShardFile(request.bucket(), request.key(), version.versionId(), request.shardIndex());
            if (!Files.exists(shardFile)) {
                sendGetShardFail(msg, request, "Shard part." + (request.shardIndex() + 1) + " not found on " + id);
                return;
            }

            byte[] data;
            long fileSize = Files.size(shardFile);
            if (request.length() <= 0 && request.offset() == 0) {
                data = Files.readAllBytes(shardFile);
            } else {
                long offset = Math.max(0, request.offset());
                long available = Math.max(0, fileSize - offset);
                int lengthToRead = (request.length() > 0)
                        ? (int) Math.min(request.length(), available)
                        : (int) available;

                data = new byte[lengthToRead];
                try (RandomAccessFile raf = new RandomAccessFile(shardFile.toFile(), "r")) {
                    raf.seek(offset);
                    raf.readFully(data);
                }
            }

            ShardMetadata shardMeta = new ShardMetadata(
                    request.shardIndex(),
                    version.shardSize(),
                    version.objectSize(),
                    version.dataShards(),
                    version.parityShards()
            );

            readsHandled++;
            GetShardResponse response = GetShardResponse.ok(request.bucket(), request.key(), request.shardIndex(), data, shardMeta);
            messageBus.sendMessage(createResponseMessage(msg, response, GET_SHARD_RESPONSE));
        } catch (Exception e) {
            logger.warn("{}: Failed to read shard for {}/{}: {}", id, request.bucket(), request.key(), e.getMessage());
            sendGetShardFail(msg, request, e.getMessage());
        }
    }

    private void handleDeleteShard(Message msg) {
        if (!online) {
            return;
        }
        DeleteShardRequest request = deserializePayload(msg.payload(), DeleteShardRequest.class);
        try {
            Path objectDir = resolveObjectDir(request.bucket(), request.key());
            Path xlMetaFile = objectDir.resolve("xl.meta");
            ObjectMeta objectMeta = ObjectMeta.read(xlMetaFile);

            String versionId = request.versionId() != null ? request.versionId() : UUID.randomUUID().toString();
            VersionEntry deleteMarker = VersionEntry.createDeleteMarker(versionId);
            objectMeta.add(deleteMarker);
            objectMeta.write(xlMetaFile);

            messageBus.sendMessage(createResponseMessage(msg, DeleteShardResponse.ok(request.bucket(), request.key()), DELETE_SHARD_RESPONSE));
        } catch (Exception e) {
            try {
                messageBus.sendMessage(createResponseMessage(msg, DeleteShardResponse.fail(request.bucket(), request.key(), e.getMessage()), DELETE_SHARD_RESPONSE));
            } catch (IOException ignored) {}
        }
    }

    private void handleGetMeta(Message msg) {
        if (!online) {
            return;
        }
        GetMetaRequest request = deserializePayload(msg.payload(), GetMetaRequest.class);
        try {
            Path objectDir = resolveObjectDir(request.bucket(), request.key());
            Path xlMetaFile = objectDir.resolve("xl.meta");
            if (!Files.exists(xlMetaFile)) {
                messageBus.sendMessage(createResponseMessage(msg, GetMetaResponse.notFound(request.bucket(), request.key()), GET_META_RESPONSE));
                return;
            }
            ObjectMeta meta = ObjectMeta.read(xlMetaFile);
            Optional<VersionEntry> latest = meta.latestVersion();
            if (latest.isEmpty()) {
                messageBus.sendMessage(createResponseMessage(msg, GetMetaResponse.notFound(request.bucket(), request.key()), GET_META_RESPONSE));
            } else {
                messageBus.sendMessage(createResponseMessage(msg, GetMetaResponse.ok(request.bucket(), request.key(), latest.get()), GET_META_RESPONSE));
            }
        } catch (Exception e) {
            try {
                messageBus.sendMessage(createResponseMessage(msg, GetMetaResponse.fail(request.bucket(), request.key(), e.getMessage()), GET_META_RESPONSE));
            } catch (IOException ignored) {}
        }
    }

    private void sendGetShardFail(Message msg, GetShardRequest request, String error) {
        GetShardResponse response = GetShardResponse.fail(request.bucket(), request.key(), request.shardIndex(), error);
        try {
            messageBus.sendMessage(createResponseMessage(msg, response, GET_SHARD_RESPONSE));
        } catch (IOException ignored) {}
    }

    /**
     * Publishes a file by renaming it into place, so a reader never sees a partial write.
     *
     * <p>Note what this does <b>not</b> give: POSIX {@code rename()} overwrites the target
     * unconditionally, so this is atomic publish only — never put-if-absent. Code that needs
     * to claim a name exclusively must use {@code Files.createLink}, as deltalite's commit log
     * does. See WORKSHOP-PLAN.md D13.
     *
     * <p>The fallback is a deliberate portability hedge on filesystems without atomic rename
     * (some network mounts). It silently reopens the torn-read window, which is acceptable for
     * a workshop and would not be in production.
     */
    private void moveAtomic(Path source, Path target) throws IOException {
        AtomicFiles.moveAtomic(source, target);
    }
}
