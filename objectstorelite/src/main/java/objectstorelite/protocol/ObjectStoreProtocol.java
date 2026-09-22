package objectstorelite.protocol;

import com.tickloom.messaging.MessageType;

public final class ObjectStoreProtocol {
    private ObjectStoreProtocol() {}

    // Client-facing Object Operations
    public static final MessageType PUT_OBJECT = MessageType.of("PutObject");
    public static final MessageType PUT_OBJECT_RESPONSE = MessageType.of("PutObjectResponse");
    public static final MessageType GET_OBJECT = MessageType.of("GetObject");
    public static final MessageType GET_OBJECT_RESPONSE = MessageType.of("GetObjectResponse");
    public static final MessageType GET_OBJECT_RANGE = MessageType.of("GetObjectRange");
    public static final MessageType GET_OBJECT_RANGE_RESPONSE = MessageType.of("GetObjectRangeResponse");
    public static final MessageType GET_OBJECT_SIZE = MessageType.of("GetObjectSize");
    public static final MessageType GET_OBJECT_SIZE_RESPONSE = MessageType.of("GetObjectSizeResponse");
    public static final MessageType LIST_OBJECTS = MessageType.of("ListObjects");
    public static final MessageType LIST_OBJECTS_RESPONSE = MessageType.of("ListObjectsResponse");
    public static final MessageType DELETE_OBJECT = MessageType.of("DeleteObject");
    public static final MessageType DELETE_OBJECT_RESPONSE = MessageType.of("DeleteObjectResponse");

    // Internal Cluster Shard Operations
    public static final MessageType PUT_SHARD = MessageType.of("PutShard");
    public static final MessageType PUT_SHARD_RESPONSE = MessageType.of("PutShardResponse");
    public static final MessageType GET_SHARD = MessageType.of("GetShard");
    public static final MessageType GET_SHARD_RESPONSE = MessageType.of("GetShardResponse");
    public static final MessageType DELETE_SHARD = MessageType.of("DeleteShard");
    public static final MessageType DELETE_SHARD_RESPONSE = MessageType.of("DeleteShardResponse");
    public static final MessageType LIST_KEYS = MessageType.of("ListKeys");
    public static final MessageType LIST_KEYS_RESPONSE = MessageType.of("ListKeysResponse");
    public static final MessageType GET_META = MessageType.of("GetMeta");
    public static final MessageType GET_META_RESPONSE = MessageType.of("GetMetaResponse");
}
