package cassandralite.heartbeat;

import com.tickloom.ProcessId;

/**
 * Mirrors {@code org.apache.cassandra.gms.IFailureDetector}.
 *
 * <p>Interface for endpoint liveness failure detection.
 */
public interface FailureDetector {

    /**
     * Reports that a heartbeat was received from {@code endpoint} at tick {@code nowTick}.
     */
    void report(ProcessId endpoint, long nowTick);

    /**
     * Evaluates whether {@code endpoint} should be convicted at tick {@code nowTick}.
     */
    void interpret(ProcessId endpoint, long nowTick);

    /**
     * Returns true if {@code endpoint} is currently considered alive.
     */
    boolean isAlive(ProcessId endpoint);

    /**
     * Returns the current &phi; value for {@code endpoint} at tick {@code nowTick}.
     */
    double getPhi(ProcessId endpoint, long nowTick);

    /**
     * Returns the current server state (UP or DOWN) for {@code endpoint}.
     */
    ServerState getState(ProcessId endpoint);

    /**
     * Returns the underlying arrival window for inspecting metrics.
     */
    ArrivalWindow getArrivalWindow(ProcessId endpoint);
}
