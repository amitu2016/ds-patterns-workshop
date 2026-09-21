package kafkalite.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a key-value message stored in a log or transmitted in requests.
 */
public class Message {
    private final byte[] key;
    private final byte[] value;

    @JsonCreator
    public Message(@JsonProperty("key") byte[] key,
                   @JsonProperty("value") byte[] value) {
        this.key = key != null ? key : new byte[0];
        this.value = value != null ? value : new byte[0];
    }

    public Message(String key, String value) {
        this(key != null ? key.getBytes(StandardCharsets.UTF_8) : new byte[0],
             value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public String keyAsString() {
        return new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Message message)) return false;
        return Arrays.equals(key, message.key) && Arrays.equals(value, message.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(key), Arrays.hashCode(value));
    }

    @Override
    public String toString() {
        return "Message[key=" + keyAsString() + ", value=" + valueAsString() + "]";
    }
}
