package kafkalite.controller;

import kafkalite.cluster.Broker;
import kafkalite.cluster.UpdateMetadataRequest;
import kafkalite.common.Config;
import kafkalite.common.ZookeeperTestHarness;
import kafkalite.zookeeper.ZkEventQueue;
import kafkalite.zookeeper.ZookeeperClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class ZkControllerTest extends ZookeeperTestHarness {

    @Test
    @DisplayName("Multiple brokers race to elect; exactly one wins and claims epoch 1")
    void multipleBrokersElectExactlyOneController() {
        Config config1 = configFor(1);
        Config config2 = configFor(2);
        Config config3 = configFor(3);

        ZookeeperClient zk1 = new ZookeeperClient(config1);
        ZookeeperClient zk2 = new ZookeeperClient(config2);
        ZookeeperClient zk3 = new ZookeeperClient(config3);

        zk1.registerSelf();
        zk2.registerSelf();
        zk3.registerSelf();

        List<UpdateMetadataRequest> updates = new CopyOnWriteArrayList<>();
        ZkController c1 = new ZkController(1, zk1, new ZkEventQueue(), updates::add);
        ZkController c2 = new ZkController(2, zk2, new ZkEventQueue(), updates::add);
        ZkController c3 = new ZkController(3, zk3, new ZkEventQueue(), updates::add);

        c1.elect();
        c2.elect();
        c3.elect();

        assertTrue(c1.isController());
        assertFalse(c2.isController());
        assertFalse(c3.isController());

        assertEquals(1, c1.activeControllerId());
        assertEquals(1, c2.activeControllerId());
        assertEquals(1, c3.activeControllerId());

        assertEquals(1, c1.epoch());
        assertEquals(1, zk1.getControllerEpoch());

        zk1.close();
        zk2.close();
        zk3.close();
    }

    @Test
    @DisplayName("When controller dies, re-election occurs and epoch increments")
    void reElectionOnControllerFailureIncrementsEpoch() throws Exception {
        Config config1 = configFor(1);
        Config config2 = configFor(2);

        ZookeeperClient zk1 = new ZookeeperClient(config1);
        ZookeeperClient zk2 = new ZookeeperClient(config2);

        zk1.registerSelf();
        zk2.registerSelf();

        List<UpdateMetadataRequest> updates1 = new CopyOnWriteArrayList<>();
        List<UpdateMetadataRequest> updates2 = new CopyOnWriteArrayList<>();

        ZkController c1 = new ZkController(1, zk1, new ZkEventQueue(), updates1::add);
        ZkController c2 = new ZkController(2, zk2, new ZkEventQueue(), updates2::add);

        c1.startup();
        c2.startup();

        assertEquals(1, c1.activeControllerId());
        assertEquals(1, c2.activeControllerId());
        assertEquals(1, c1.epoch());

        // Controller 1 crashes / session ends
        zk1.close();

        // Wait for ZK notification to reach c2 and drain on tick
        long deadline = System.currentTimeMillis() + 5000;
        while (!c2.isController() && System.currentTimeMillis() < deadline) {
            c2.onTick();
            Thread.sleep(20);
        }

        assertTrue(c2.isController(), "Broker 2 should have taken over as controller");
        assertEquals(2, c2.activeControllerId());
        assertEquals(2, c2.epoch(), "Epoch must increment upon new controller election");

        zk2.close();
    }
}
