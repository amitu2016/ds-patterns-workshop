package objectstorelite.client;

import com.tickloom.ProcessId;
import com.tickloom.testkit.Cluster;
import objectstorelite.codec.ErasureSetMapper;
import objectstorelite.model.ErasureSet;
import objectstorelite.protocol.*;
import objectstorelite.server.StorageNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ObjectStoreClusterTest {

    private static final int DATA_SHARDS = 4;
    private static final int PARITY_SHARDS = 2;
    private static final int TOTAL_SHARDS = DATA_SHARDS + PARITY_SHARDS;

    @TempDir
    Path clusterStorageRoot;

    private Cluster cluster;
    private ObjectStoreClient client;
    private List<ProcessId> nodeIds;

    @BeforeEach
    void setUp() throws Exception {
        nodeIds = new ArrayList<>();
        List<Path> nodePaths = new ArrayList<>();
        for (int i = 0; i < TOTAL_SHARDS; i++) {
            ProcessId id = ProcessId.of("node-" + i);
            nodeIds.add(id);
            nodePaths.add(clusterStorageRoot.resolve("disk-node-" + i));
        }

        ErasureSet erasureSet = new ErasureSet(0, nodeIds);
        ErasureSetMapper mapper = new ErasureSetMapper(
                ErasureSetMapper.Algorithm.CRCMOD, null, List.of(erasureSet));

        cluster = new Cluster()
                .withProcessIds(nodeIds)
                .withRequestTimeoutTicks(10)
                .useSimulatedNetwork()
                .build((peerIds, params) -> {
                    int index = nodeIds.indexOf(params.id());
                    return new StorageNode(peerIds, params, nodePaths.get(index), mapper, DATA_SHARDS, PARITY_SHARDS);
                })
                .start();

        // Thin client connected to node-0
        client = cluster.newClient(
                ProcessId.of("client-test"),
                (peers, params) -> new ObjectStoreClient(peers, params, nodeIds.get(0))
        );
    }

    @AfterEach
    void tearDown() {
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception ignored) {}
        }
    }

    @Test
    void listObjectsReturnsEveryKeyUnderThePrefix() {
        // A Delta-style log: the keys a table format would need to discover by listing.
        List<String> logKeys = List.of(
                "_delta_log/00000000000000000000.json",
                "_delta_log/00000000000000000001.json",
                "_delta_log/00000000000000000002.json");
        for (String key : logKeys) {
            var put = client.putObject(key, ("actions for " + key).getBytes(StandardCharsets.UTF_8));
            cluster.tickUntil(put::isCompleted);
            assertTrue(put.getResult().success());
        }
        // ...and a data file that must NOT appear under the log prefix.
        var dataPut = client.putObject("data/part-0.parquet", "rows".getBytes(StandardCharsets.UTF_8));
        cluster.tickUntil(dataPut::isCompleted);

        var list = client.listObjects("_delta_log/");
        cluster.tickUntil(list::isCompleted);
        assertTrue(list.getResult().success(), "ListObjects should succeed");

        assertEquals(logKeys, list.getResult().keys(),
                "every log version, sorted, and nothing from outside the prefix");
    }

    @Test
    void listObjectsUnionsKeysHeldByDifferentNodes() {
        // Keys hash to different erasure sets, so no single node holds them all. The
        // coordinator must fan out and union, or a listing silently loses keys.
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            String key = "objects/key-" + i;
            keys.add(key);
            var put = client.putObject(key, ("v" + i).getBytes(StandardCharsets.UTF_8));
            cluster.tickUntil(put::isCompleted);
        }

        var list = client.listObjects("objects/");
        cluster.tickUntil(list::isCompleted);

        java.util.List<String> expected = new ArrayList<>(keys);
        java.util.Collections.sort(expected);
        assertEquals(expected, list.getResult().keys(), "no key may be dropped by the union");
    }

    @Test
    void listObjectsOnAnEmptyPrefixReturnsNothing() {
        var list = client.listObjects("nothing-here/");
        cluster.tickUntil(list::isCompleted);
        assertTrue(list.getResult().success(), "an empty listing is a success, not an error");
        assertTrue(list.getResult().keys().isEmpty());
    }

    @Test
    void testPutAndGetObjectRoundTrip() {
        String key = "datasets/orders.parquet";
        byte[] payload = "order_id,customer,amount\n1,Alice,50.0\n2,Bob,75.5\n".getBytes(StandardCharsets.UTF_8);

        // Put object: returns future from client, test ticks the cluster until it completes
        var putFuture = client.putObject(key, payload);
        cluster.tickUntil(putFuture::isCompleted);
        assertTrue(putFuture.getResult().success(), "PutObject should succeed");

        // Get object: returns future from client, test ticks the cluster until it completes
        var getFuture = client.getObject(key);
        cluster.tickUntil(getFuture::isCompleted);
        assertTrue(getFuture.getResult().success(), "GetObject should succeed");
        assertArrayEquals(payload, getFuture.getResult().data());

        // Get object size: returns future from client
        var sizeFuture = client.getObjectSize(key);
        cluster.tickUntil(sizeFuture::isCompleted);
        assertTrue(sizeFuture.getResult().success(), "GetObjectSize should succeed");
        assertEquals(payload.length, sizeFuture.getResult().size());
    }

    @Test
    void testGetObjectRange() {
        String key = "parquet/table.parquet";
        byte[] payload = "0123456789ABCDEF0123456789ABCDEF".repeat(10).getBytes(StandardCharsets.UTF_8);

        var putFuture = client.putObject(key, payload);
        cluster.tickUntil(putFuture::isCompleted);
        assertTrue(putFuture.getResult().success());

        // Range read bytes 10 to 30 (length 20)
        var rangeFuture = client.getObjectRange(key, 10, 20);
        cluster.tickUntil(rangeFuture::isCompleted);
        assertTrue(rangeFuture.getResult().success(), "GetObjectRange should succeed");

        byte[] range = rangeFuture.getResult().data();
        assertEquals(20, range.length);

        byte[] expected = new byte[20];
        System.arraycopy(payload, 10, expected, 0, 20);
        assertArrayEquals(expected, range);
    }

    @Test
    void testDeleteObject() {
        String key = "temp/to_delete.txt";
        byte[] payload = "Temporary data".getBytes(StandardCharsets.UTF_8);

        var putFuture = client.putObject(key, payload);
        cluster.tickUntil(putFuture::isCompleted);
        assertTrue(putFuture.getResult().success());

        var getFuture = client.getObject(key);
        cluster.tickUntil(getFuture::isCompleted);
        assertTrue(getFuture.getResult().success());
        assertArrayEquals(payload, getFuture.getResult().data());

        var deleteFuture = client.deleteObject(key);
        cluster.tickUntil(deleteFuture::isCompleted);
        assertTrue(deleteFuture.getResult().success(), "DeleteObject should succeed");

        // Subsequent get after delete must return not found / deleted
        var getAfterDelete = client.getObject(key);
        cluster.tickUntil(getAfterDelete::isCompleted);
        assertFalse(getAfterDelete.getResult().success(), "Object should not be found after delete");
    }

    @Test
    void testReadSurvivesTwoNodeFailures() {
        String key = "resilient/data.bin";
        byte[] payload = "Critical transaction data replicated with RS(4,2) coding".repeat(5)
                .getBytes(StandardCharsets.UTF_8);

        var putFuture = client.putObject(key, payload);
        cluster.tickUntil(putFuture::isCompleted);
        assertTrue(putFuture.getResult().success());

        // Kill node-1 and node-4 (1 data, 1 parity)
        StorageNode node1 = cluster.getNode(nodeIds.get(1));
        StorageNode node4 = cluster.getNode(nodeIds.get(4));
        node1.setOnline(false);
        node4.setOnline(false);

        // Surviving nodes = 4 of 6 (exact K=4 quorum met)
        // Client connects to node-0 (coordinator), which queries peers and reconstructs via RS
        var getFuture = client.getObject(key);
        cluster.tickUntil(getFuture::isCompleted);
        assertTrue(getFuture.getResult().success(), "Read should survive 2 node failures via RS reconstruction");
        assertArrayEquals(payload, getFuture.getResult().data());

        // Range read also succeeds under failure
        var rangeFuture = client.getObjectRange(key, 5, 15);
        cluster.tickUntil(rangeFuture::isCompleted);
        assertTrue(rangeFuture.getResult().success());
        byte[] expectedRange = new byte[15];
        System.arraycopy(payload, 5, expectedRange, 0, 15);
        assertArrayEquals(expectedRange, rangeFuture.getResult().data());
    }

    @Test
    void testReadFailsWhenThreeNodesFail() {
        String key = "fragile/data.bin";
        byte[] payload = "Data about to be lost".getBytes(StandardCharsets.UTF_8);

        var putFuture = client.putObject(key, payload);
        cluster.tickUntil(putFuture::isCompleted);
        assertTrue(putFuture.getResult().success());

        // Kill 3 peer nodes (node-1, node-2, node-3)
        // node-0 survives, but only 3 nodes total survive (node-0, node-4, node-5 < K=4 quorum)
        cluster.<StorageNode>getNode(nodeIds.get(1)).setOnline(false);
        cluster.<StorageNode>getNode(nodeIds.get(2)).setOnline(false);
        cluster.<StorageNode>getNode(nodeIds.get(3)).setOnline(false);

        // Surviving nodes = 3 of 6 (< K=4 quorum)
        var getFuture = client.getObject(key);
        cluster.tickUntil(() -> getFuture.isFailed() || (getFuture.isCompleted() && !getFuture.getResult().success()));

        if (getFuture.isCompleted()) {
            assertFalse(getFuture.getResult().success(), "Read must fail when below quorum");
        } else {
            assertTrue(getFuture.isFailed(), "Future must fail when below quorum");
        }
    }

    @Test
    void testPutObjectIfAbsent() {
        String key = "commit-log/00000000000000000000.json";
        byte[] v1 = "{\"commit\": 1}".getBytes(StandardCharsets.UTF_8);
        byte[] v2 = "{\"commit\": 2}".getBytes(StandardCharsets.UTF_8);

        // 1. Initial put-if-absent on non-existent key succeeds
        var put1 = client.putObjectIfAbsent(key, v1);
        cluster.tickUntil(put1::isCompleted);
        assertTrue(put1.getResult().success(), "First put-if-absent must succeed");

        // Verify content is v1
        var get1 = client.getObject(key);
        cluster.tickUntil(get1::isCompleted);
        assertTrue(get1.getResult().success());
        assertArrayEquals(v1, get1.getResult().data());

        // 2. Second put-if-absent on existing key fails with PreconditionFailed
        var put2 = client.putObjectIfAbsent(key, v2);
        cluster.tickUntil(put2::isCompleted);
        assertFalse(put2.getResult().success(), "Second put-if-absent must fail");
        assertTrue(put2.getResult().error().contains("PreconditionFailed"),
                "Error should indicate precondition failure: " + put2.getResult().error());

        // Verify content remains v1
        var get2 = client.getObject(key);
        cluster.tickUntil(get2::isCompleted);
        assertTrue(get2.getResult().success());
        assertArrayEquals(v1, get2.getResult().data());

        // 3. Unconditional putObject succeeds and overwrites
        var put3 = client.putObject(key, v2);
        cluster.tickUntil(put3::isCompleted);
        assertTrue(put3.getResult().success(), "Unconditional PUT must overwrite and succeed");

        var get3 = client.getObject(key);
        cluster.tickUntil(get3::isCompleted);
        assertTrue(get3.getResult().success());
        assertArrayEquals(v2, get3.getResult().data());
    }
}
