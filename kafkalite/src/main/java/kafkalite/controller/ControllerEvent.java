package kafkalite.controller;

/**
 * Mirrors {@code kafka.controller.ControllerEvent}.
 *
 * <p>Represents typed coordination events delivered from ZooKeeper watchers.
 *
 * <p><b>Design Principle (Level-Triggered Notifications):</b>
 * Watch callbacks are <i>invalidation triggers</i> rather than data payloads.
 * Events like {@link BrokerChange} and {@link TopicChange} intentionally carry
 * <b>no snapshot state</b>. When processed on the controller's tick thread,
 * the controller queries ZooKeeper directly for the authoritative, current truth.
 * This mirrors upstream Kafka and aligns with the ListWatch pattern used in Kubernetes.
 */
public sealed interface ControllerEvent {

    /** Emitted when child znodes under {@code /brokers/ids} change. */
    record BrokerChange() implements ControllerEvent {}

    /** Emitted when child znodes under {@code /brokers/topics} change. */
    record TopicChange() implements ControllerEvent {}

    /** Emitted when data at {@code /controller} changes. */
    record ControllerUpdated() implements ControllerEvent {}

    /** Emitted when {@code /controller} is deleted (controller died or resigned). */
    record ControllerDeleted() implements ControllerEvent {}
}
