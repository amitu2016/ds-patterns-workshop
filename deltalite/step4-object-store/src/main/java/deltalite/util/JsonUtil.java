package deltalite.util;

import deltalite.actions.Action;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;

/**
 * Utility class for JSON serialization and deserialization of Delta log actions.
 */
public class JsonUtil {
    private static final ObjectMapper mapper = new ObjectMapper();
    
    static {
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.disable(SerializationFeature.INDENT_OUTPUT);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }
    
    public static String toJson(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing object to JSON", e);
        }
    }
    
    public static Action fromJson(String json) {
        try {
            if (json == null || json.trim().isEmpty()) {
                throw new IllegalArgumentException("JSON string cannot be null or empty");
            }
            return mapper.readValue(json, Action.class);
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing JSON to Action: " + e.getMessage(), e);
        }
    }
}
