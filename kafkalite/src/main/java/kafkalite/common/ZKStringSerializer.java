package kafkalite.common;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.nio.charset.StandardCharsets;

/** Mirrors {@code kafka.utils.ZKStringSerializer}: znode data is UTF-8 JSON. */
public class ZKStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) {
        if (!(data instanceof String)) {
            throw new ZkMarshallingError("Expected a String object");
        }
        return ((String) data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }
}
