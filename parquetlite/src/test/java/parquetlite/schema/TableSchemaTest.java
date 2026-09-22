package parquetlite.schema;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TableSchemaTest {

    @Test
    void createSchemaAndConvertToParquet() {
        TableSchema schema = TableSchema.createCustomerSchema();
        assertEquals("id", schema.getPrimaryKeyColumn());
        assertEquals(5, schema.getColumnCount());
        assertTrue(schema.hasColumn("name"));
        assertTrue(schema.hasColumn("age"));

        MessageType parquetSchema = schema.toParquetSchema("customer");
        assertNotNull(parquetSchema);
        assertEquals("customer", parquetSchema.getName());
        assertEquals(5, parquetSchema.getFieldCount());

        assertEquals(PrimitiveType.PrimitiveTypeName.BINARY,
                parquetSchema.getType("id").asPrimitiveType().getPrimitiveTypeName());
        assertEquals(PrimitiveType.PrimitiveTypeName.INT32,
                parquetSchema.getType("age").asPrimitiveType().getPrimitiveTypeName());
    }

    @Test
    void validateTableRecord() {
        TableSchema schema = TableSchema.createCustomerSchema();
        TableRecord validRecord = new TableRecord("C001", java.util.Map.of(
                "id", "C001", "name", "Alice", "age", 30
        ));

        assertEquals("C001", validRecord.primaryKey());
        assertEquals("Alice", validRecord.getString("name"));
        assertEquals(30, validRecord.getInteger("age"));
    }

    @Test
    void invalidPrimaryKeyThrowsException() {
        assertThrows(IllegalArgumentException.class, () ->
                new TableSchema("nonexistent", List.of(
                        new TableSchema.ColumnDefinition("id", TableSchema.ColumnType.STRING, true)
                )));
    }
}
