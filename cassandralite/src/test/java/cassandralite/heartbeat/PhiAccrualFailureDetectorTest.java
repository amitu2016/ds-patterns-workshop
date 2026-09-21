package cassandralite.heartbeat;

import com.tickloom.ProcessId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PhiAccrualFailureDetectorTest {

    @Test
    void arrivalWindowComputesMeanAndPhiCorrectly() {
        ArrivalWindow window = new ArrivalWindow(100, 1L, 1000L);
        // Regular heartbeats every 1 tick
        window.add(10);
        window.add(11);
        window.add(12);
        window.add(13);
        window.add(14);

        assertEquals(1.0, window.mean(), 0.05);
        // At tick 14 (just arrived), phi should be 0
        assertEquals(0.0, window.phi(14), 0.01);

        // At tick 20 (6 ticks late, mean=1.0)
        // phi = (6 / 1.0) * (1 / ln(10)) ~ 6 * 0.434 ~ 2.6
        double phi = window.phi(20);
        assertTrue(phi > 2.0 && phi < 3.5, "Expected phi ~2.6 but was " + phi);
    }

    @Test
    void failureDetectorConvictsEndpointWhenPhiExceedsThreshold() {
        PhiAccrualFailureDetector fd = new PhiAccrualFailureDetector(8.0);
        ProcessId node = ProcessId.of("node-1");

        // Report heartbeats at ticks 1, 2, 3, 4, 5
        for (long t = 1; t <= 5; t++) {
            fd.report(node, t);
            fd.interpret(node, t);
            assertTrue(fd.isAlive(node));
            assertEquals(ServerState.UP, fd.getState(node));
        }

        // Now heartbeats stop. Ticks keep advancing.
        // At tick 10 (5 ticks late, mean ~1), phi ~ 2.17 -> still UP
        fd.interpret(node, 10);
        assertTrue(fd.isAlive(node));

        // At tick 20 (15 ticks late, mean ~1), phi ~ 6.5 -> still UP (threshold 8.0)
        fd.interpret(node, 20);
        assertTrue(fd.isAlive(node));

        // At tick 30 (25 ticks late, mean ~1), phi = 25 * 0.434 ~ 10.8 > 8.0 -> convicted to DOWN!
        fd.interpret(node, 30);
        assertFalse(fd.isAlive(node));
        assertEquals(ServerState.DOWN, fd.getState(node));

        // When node resumes and sends heartbeat at tick 35, it recovers to UP!
        fd.report(node, 35);
        assertTrue(fd.isAlive(node));
        assertEquals(ServerState.UP, fd.getState(node));
    }
}
