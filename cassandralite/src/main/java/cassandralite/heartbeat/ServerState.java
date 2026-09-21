package cassandralite.heartbeat;

/**
 * Endpoint liveness state determined by the failure detector.
 */
public enum ServerState {
    UP,
    DOWN
}
