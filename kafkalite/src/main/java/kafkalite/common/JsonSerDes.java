package kafkalite.common;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * JSON for data stored in ZooKeeper znodes.
 *
 * <p>Mirrors what {@code kafka.zk.ZkData} does: broker and topic metadata live in
 * ZooKeeper as JSON, so it stays inspectable with {@code zkCli.sh} — which is exactly
 * what makes it demonstrable on screen.
 *
 * <p>Note: inter-broker messages do NOT go through here. Those are serialized by
 * tickloom's own {@code MessageCodec}.
 */
public class JsonSerDes {

    private static ObjectMapper mapper() {
        ObjectMapper m = new ObjectMapper(new JsonFactory());
        m.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        m.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        m.registerModule(new ParameterNamesModule(JsonCreator.Mode.DEFAULT));
        return m;
    }

    public static String toJson(Object obj) {
        try {
            return new String(mapper().writeValueAsBytes(obj));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(byte[] json, Class<T> clazz) {
        try {
            return mapper().readValue(json, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T fromJson(byte[] json, TypeReference<T> typeRef) {
        try {
            return mapper().readValue(json, typeRef);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing object", e);
        }
    }
}
