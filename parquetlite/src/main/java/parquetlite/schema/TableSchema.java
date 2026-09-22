package parquetlite.schema;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.util.*;

/**
 * Defines the logical schema for a table, including column definitions, data types, and primary key.
 * Can be converted to an Apache Parquet {@link MessageType}.
 */
public class TableSchema {

    public enum ColumnType {
        STRING(String.class, PrimitiveType.PrimitiveTypeName.BINARY),
        INTEGER(Integer.class, PrimitiveType.PrimitiveTypeName.INT32),
        LONG(Long.class, PrimitiveType.PrimitiveTypeName.INT64),
        DOUBLE(Double.class, PrimitiveType.PrimitiveTypeName.DOUBLE),
        BOOLEAN(Boolean.class, PrimitiveType.PrimitiveTypeName.BOOLEAN);

        private final Class<?> javaType;
        private final PrimitiveType.PrimitiveTypeName parquetType;

        ColumnType(Class<?> javaType, PrimitiveType.PrimitiveTypeName parquetType) {
            this.javaType = javaType;
            this.parquetType = parquetType;
        }

        public Class<?> getJavaType() {
            return javaType;
        }

        public PrimitiveType.PrimitiveTypeName getParquetType() {
            return parquetType;
        }

        public boolean isValidValue(Object value) {
            return value == null || javaType.isInstance(value);
        }
    }

    public static class ColumnDefinition {
        private final String name;
        private final ColumnType type;
        private final boolean required;

        public ColumnDefinition(String name, ColumnType type, boolean required) {
            this.name = Objects.requireNonNull(name, "name");
            this.type = Objects.requireNonNull(type, "type");
            this.required = required;
        }

        public String getName() {
            return name;
        }

        public ColumnType getType() {
            return type;
        }

        public boolean isRequired() {
            return required;
        }

        public void validate(Object value) {
            if (value == null) {
                if (required) {
                    throw new IllegalArgumentException("Column '" + name + "' is required but got null");
                }
                return;
            }
            if (!type.isValidValue(value)) {
                throw new IllegalArgumentException("Column '" + name + "' expects " + type +
                        " but got " + value.getClass().getSimpleName() + ": " + value);
            }
        }

        @Override
        public String toString() {
            return String.format("%s:%s%s", name, type, required ? "*" : "");
        }
    }

    private final String primaryKeyColumn;
    private final Map<String, ColumnDefinition> columns;
    private final List<String> columnOrder;

    public TableSchema(String primaryKeyColumn, List<ColumnDefinition> columnList) {
        this.primaryKeyColumn = Objects.requireNonNull(primaryKeyColumn, "primaryKeyColumn");
        this.columns = new LinkedHashMap<>();
        this.columnOrder = new ArrayList<>();

        for (ColumnDefinition col : columnList) {
            this.columns.put(col.getName(), col);
            this.columnOrder.add(col.getName());
        }

        if (!this.columns.containsKey(primaryKeyColumn)) {
            throw new IllegalArgumentException("Primary key column '" + primaryKeyColumn + "' not found in schema");
        }
    }

    public String getPrimaryKeyColumn() {
        return primaryKeyColumn;
    }

    public boolean hasColumn(String name) {
        return columns.containsKey(name);
    }

    public ColumnDefinition getColumn(String name) {
        return columns.get(name);
    }

    public List<String> getColumnNames() {
        return Collections.unmodifiableList(columnOrder);
    }

    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Converts this schema to an Apache Parquet {@link MessageType}.
     */
    public MessageType toParquetSchema(String messageName) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (String colName : columnOrder) {
            ColumnDefinition col = columns.get(colName);
            if (col.isRequired()) {
                builder.required(col.getType().getParquetType()).named(col.getName());
            } else {
                builder.optional(col.getType().getParquetType()).named(col.getName());
            }
        }
        return builder.named(messageName != null ? messageName : "record");
    }

    /**
     * Factory for standard customer schema used in demos and tests.
     */
    public static TableSchema createCustomerSchema() {
        return new TableSchema("id", List.of(
                new ColumnDefinition("id", ColumnType.STRING, true),
                new ColumnDefinition("name", ColumnType.STRING, true),
                new ColumnDefinition("email", ColumnType.STRING, false),
                new ColumnDefinition("age", ColumnType.INTEGER, false),
                new ColumnDefinition("city", ColumnType.STRING, false)
        ));
    }

    @Override
    public String toString() {
        return String.format("TableSchema{pk='%s', cols=%s}", primaryKeyColumn, columnOrder);
    }
}
