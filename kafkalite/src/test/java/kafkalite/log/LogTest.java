package kafkalite.log;

import kafkalite.api.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTest {

    @TempDir
    Path tempDir;

    private File logFile;
    private Log log;

    @BeforeEach
    void setUp() throws IOException {
        logFile = tempDir.resolve("test-log.log").toFile();
        log = new Log(logFile);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (log != null) {
            log.close();
        }
    }

    @Test
    void appendsAndReadsMessagesWithMonotonicOffsets() throws IOException {
        long offset1 = log.append(new Message("key1", "value1"));
        long offset2 = log.append(new Message("key2", "value2"));
        long offset3 = log.append(new Message("key3", "value3"));

        assertEquals(1, offset1);
        assertEquals(2, offset2);
        assertEquals(3, offset3);
        assertEquals(3, log.lastOffset());

        Message m1 = log.readSingleMessage(1);
        assertEquals("key1", m1.keyAsString());
        assertEquals("value1", m1.valueAsString());

        Message m2 = log.readSingleMessage(2);
        assertEquals("key2", m2.keyAsString());
        assertEquals("value2", m2.valueAsString());

        Message m3 = log.readSingleMessage(3);
        assertEquals("key3", m3.keyAsString());
        assertEquals("value3", m3.valueAsString());
    }

    @Test
    void rangeReadReturnsMessagesInclusive() throws IOException {
        for (int i = 1; i <= 5; i++) {
            log.append(new Message("k" + i, "v" + i));
        }

        List<Message> slice = log.read(2, 4);
        assertEquals(3, slice.size());
        assertEquals("k2", slice.get(0).keyAsString());
        assertEquals("k3", slice.get(1).keyAsString());
        assertEquals("k4", slice.get(2).keyAsString());
    }

    @Test
    void recoversIndexAndNextOffsetOnReopen() throws IOException {
        log.append(new Message("k1", "v1"));
        log.append(new Message("k2", "v2"));
        log.close();

        // Reopen existing file
        try (Log reopened = new Log(logFile)) {
            assertEquals(2, reopened.lastOffset());
            assertEquals(3, reopened.nextOffset());

            Message m1 = reopened.readSingleMessage(1);
            assertEquals("k1", m1.keyAsString());
            assertEquals("v1", m1.valueAsString());

            long offset3 = reopened.append(new Message("k3", "v3"));
            assertEquals(3, offset3);
            assertEquals(3, reopened.lastOffset());
        }
    }
}
