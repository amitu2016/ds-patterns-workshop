package deltalite.util;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParquetUtilTest {

    @TempDir
    Path tempDir;
    private Path parquetFile;
    private List<Map<String, String>> testData;

    @BeforeEach
    void setUp() throws IOException {
        parquetFile = tempDir.resolve("test.parquet");
        testData = createTestData();
        ParquetUtil.writeRecords(testData, parquetFile);
    }

    private List<Map<String, String>> createTestData() {
        List<Map<String, String>> data = new ArrayList<>();
        
        // Add customer records
        addCustomer(data, "John", "25", "New York");
        addCustomer(data, "Alice", "30", "Los Angeles");
        addCustomer(data, "Bob", "35", "New York");
        addCustomer(data, "Carol", "40", "Chicago");
        addCustomer(data, "Dave", "45", "New York");
        
        return data;
    }

    private void addCustomer(List<Map<String, String>> data, String name, String age, String location) {
        Map<String, String> record = new HashMap<>();
        record.put("name", name);
        record.put("age", age);
        record.put("location", location);
        data.add(record);
    }

    @Test
    void testReadAllRecords() throws IOException {
        // Verify the file was created and is not empty
        assertTrue(Files.exists(parquetFile));
        assertTrue(Files.size(parquetFile) > 0);

        List<Map<String, String>> records = ParquetUtil.readRecords(parquetFile);
        assertEquals(5, records.size());
    }

    @Test
    void testFilterByAge() throws IOException {
        // Create a filter for age > "30" (since age is stored as string)
        Operators.BinaryColumn ageColumn = FilterApi.binaryColumn("age");
        FilterPredicate ageFilter = FilterApi.gt(ageColumn, Binary.fromString("30"));

        List<Map<String, String>> filteredRecords = ParquetUtil.readRecords(parquetFile, ageFilter);
        assertEquals(3, filteredRecords.size());
        
        // Verify all records have age > 30
        for (Map<String, String> record : filteredRecords) {
            int age = Integer.parseInt(record.get("age"));
            assert(age > 30);
        }
    }

    @Test
    void testFilterByLocation() throws IOException {
        // Create a filter for location = "New York"
        Operators.BinaryColumn locationColumn = FilterApi.binaryColumn("location");
        FilterPredicate locationFilter = FilterApi.eq(locationColumn, Binary.fromString("New York"));

        List<Map<String, String>> filteredRecords = ParquetUtil.readRecords(parquetFile, locationFilter);
        assertEquals(3, filteredRecords.size());
        
        // Verify all records are from New York
        for (Map<String, String> record : filteredRecords) {
            assertEquals("New York", record.get("location"));
        }
    }

    @Test
    void testFilterByAgeAndLocation() throws IOException {
        // Create a filter for age > "30" AND location = "New York"
        Operators.BinaryColumn ageColumn = FilterApi.binaryColumn("age");
        Operators.BinaryColumn locationColumn = FilterApi.binaryColumn("location");
        
        FilterPredicate ageFilter = FilterApi.gt(ageColumn, Binary.fromString("30"));
        FilterPredicate locationFilter = FilterApi.eq(locationColumn, Binary.fromString("New York"));
        FilterPredicate combinedFilter = FilterApi.and(ageFilter, locationFilter);

        List<Map<String, String>> filteredRecords = ParquetUtil.readRecords(parquetFile, combinedFilter);
        assertEquals(2, filteredRecords.size());
        
        // Verify all records match both conditions
        for (Map<String, String> record : filteredRecords) {
            int age = Integer.parseInt(record.get("age"));
            assert(age > 30);
            assertEquals("New York", record.get("location"));
        }
    }
} 