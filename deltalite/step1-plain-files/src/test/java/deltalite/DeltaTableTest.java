package deltalite;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the DeltaTable class.
 */
public class DeltaTableTest {
    

    
    private String tablePath;
    private DeltaTable deltaTable;

    @TempDir
    public Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tablePath = tempDir.resolve("delta_module1").toString();
        deltaTable = new DeltaTable(tablePath);
    }
    
    @AfterEach
    void tearDown() throws IOException {
        // Clean up if needed
    }

    @Test
    void testInsertRecords() throws IOException {
        // Create test records
        List<Map<String, String>> records = createTestRecords(5);
        
        // Insert the records
        int inserted = deltaTable.insert(records);
        
        // Verify the number of records inserted
        assertEquals(5, inserted, "Should have inserted 5 records");
        
        // Verify there's a new file in the data directory
        Path dataDir = Path.of(tablePath, "data");
        List<Path> dataFiles = Files.list(dataDir)
                .filter(path -> path.toString().endsWith(".parquet"))
                .collect(Collectors.toList());
        assertEquals(1, dataFiles.size(), "Should have one data file");
    }
    
    @Test
    void testReadAllRecords() throws IOException {
        // Create and insert test records
        List<Map<String, String>> records = createTestRecords(10);
        deltaTable.insert(records);
        
        // Insert more records
        List<Map<String, String>> moreRecords = createTestRecords(5, 10);
        deltaTable.insert(moreRecords);
        
        // Read all records
        List<Map<String, String>> readRecords = deltaTable.readAll();
        
        // Verify the number of records read
        assertEquals(15, readRecords.size(), "Should read 15 records");
        
        // Verify the content of the records
        List<Map<String, String>> allExpectedRecords = new ArrayList<>();
        allExpectedRecords.addAll(records);
        allExpectedRecords.addAll(moreRecords);
        
        // Check that all expected records are present in the result
        for (Map<String, String> expected : allExpectedRecords) {
            boolean found = readRecords.stream()
                    .anyMatch(actual -> recordsMatch(expected, actual));
            assertTrue(found, "Should find record: " + expected);
        }
    }
    
    @Test
    void testConsecutiveInserts() throws IOException {
        // Insert records in multiple transactions
        for (int i = 0; i < 3; i++) {
            List<Map<String, String>> batch = createTestRecords(2, i * 2);
            deltaTable.insert(batch);
        }
        
        // Read all records
        List<Map<String, String>> readRecords = deltaTable.readAll();
        
        // Verify the total number of records
        assertEquals(6, readRecords.size(), "Should have 6 records in total");
        
        // Verify the content of each record
        List<Map<String, String>> allExpectedRecords = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            allExpectedRecords.addAll(createTestRecords(2, i * 2));
        }
        
        for (Map<String, String> expected : allExpectedRecords) {
            boolean found = readRecords.stream()
                    .anyMatch(actual -> recordsMatch(expected, actual));
            assertTrue(found, "Should find record: " + expected);
        }
    }
    
    /**
     * Creates a list of test records with sequential IDs.
     *
     * @param count the number of records to create
     * @return a list of test records
     */
    private List<Map<String, String>> createTestRecords(int count) {
        return createTestRecords(count, 0);
    }
    
    /**
     * Creates a list of test records with sequential IDs starting from the specified ID.
     *
     * @param count the number of records to create
     * @param startId the starting ID
     * @return a list of test records
     */
    private List<Map<String, String>> createTestRecords(int count, int startId) {
        List<Map<String, String>> records = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            int id = startId + i;
            Map<String, String> record = new HashMap<>();
            record.put("id", String.valueOf(id));
            record.put("name", "Name" + id);
            // Deterministic: expectations are built by calling this generator a second time,
            // so a random value here can never match what was actually inserted.
            record.put("age", String.valueOf(20 + (id % 50)));
            record.put("value", String.valueOf(id * 10));
            
            records.add(record);
        }
        
        return records;
    }
    
    /**
     * Checks if two records match by comparing their field values.
     *
     * @param record1 the first record
     * @param record2 the second record
     * @return true if the records match, false otherwise
     */
    private boolean recordsMatch(Map<String, String> record1, Map<String, String> record2) {
        if (record1.size() != record2.size()) {
            return false;
        }
        
        for (Map.Entry<String, String> entry : record1.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            
            if (!record2.containsKey(key) || !Objects.equals(record2.get(key), value)) {
                return false;
            }
        }
        
        return true;
    }
} 