package parquetlite.schema;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * In-memory representation of a table row / record.
 */
public record TableRecord(String primaryKey, Map<String, Object> values) {

    public TableRecord {
        Objects.requireNonNull(primaryKey, "primaryKey");
        values = Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }

    public Object get(String fieldName) {
        return values.get(fieldName);
    }

    public String getString(String fieldName) {
        Object val = values.get(fieldName);
        return val != null ? val.toString() : null;
    }

    public Integer getInteger(String fieldName) {
        Object val = values.get(fieldName);
        if (val instanceof Number n) {
            return n.intValue();
        }
        return null;
    }
}
