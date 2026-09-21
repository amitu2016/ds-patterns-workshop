package kafkalite.controller;

/**
 * Thrown when a broker attempts to elect itself as controller, but an active
 * controller already exists at {@code /controller}.
 */
public class ControllerExistsException extends RuntimeException {
    private final int controllerId;

    public ControllerExistsException(int controllerId) {
        super("Controller already exists with broker id " + controllerId);
        this.controllerId = controllerId;
    }

    public int controllerId() {
        return controllerId;
    }
}
