package kafkalite.partition;

import kafkalite.api.FetchIsolation;
import kafkalite.api.Message;
import kafkalite.cluster.Broker;
import kafkalite.cluster.PartitionStateInfo;
import kafkalite.common.TopicAndPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionTest {

    @TempDir
    Path tempDir;

    private Partition partition;
    private final TopicAndPartition tp = new TopicAndPartition("orders", 0);

    @BeforeEach
    void setUp() throws IOException {
        partition = new Partition(1, tempDir.toFile(), tp);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (partition != null) {
            partition.close();
        }
    }

    @Test
    void makeLeaderTracksAllReplicasAndComputesHighWatermark() throws IOException {
        Broker b1 = new Broker(1, "localhost", 9092);
        Broker b2 = new Broker(2, "localhost", 9093);
        PartitionStateInfo stateInfo = new PartitionStateInfo(1, List.of(b1, b2));

        partition.makeLeader(stateInfo);
        assertTrue(partition.isLeader());
        assertEquals(0L, partition.highWatermark());

        // Leader writes message 1
        long offset1 = partition.append(new Message("k1", "v1"));
        assertEquals(1L, offset1);
        assertEquals(1L, partition.lastOffset());
        // Follower b2 has not replicated yet, so HW remains 0
        assertEquals(0L, partition.highWatermark());

        // Consumer reading with HIGH_WATERMARK sees nothing
        List<Message> consumerRead = partition.read(1, FetchIsolation.HIGH_WATERMARK);
        assertTrue(consumerRead.isEmpty(), "Uncommitted message must NOT be visible under HIGH_WATERMARK isolation");

        // Follower reading with LOG_END sees message 1
        List<Message> followerRead = partition.read(1, FetchIsolation.LOG_END);
        assertEquals(1, followerRead.size());
        assertEquals("k1", followerRead.get(0).keyAsString());

        // Follower b2 catches up and acknowledges offset 1
        partition.updateReplicaOffset(2, 1L);
        assertEquals(1L, partition.highWatermark(), "HW must advance to 1 once all replicas reach offset 1");

        // Now consumer can read message 1
        List<Message> committedRead = partition.read(1, FetchIsolation.HIGH_WATERMARK);
        assertEquals(1, committedRead.size());
        assertEquals("k1", committedRead.get(0).keyAsString());
    }

    @Test
    void makeFollowerSwitchesRole() {
        partition.makeFollower(2);
        assertFalse(partition.isLeader());
        assertEquals(2, partition.leaderBrokerId());
    }
}
