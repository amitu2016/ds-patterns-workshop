package kafkalite.zookeeper;

import kafkalite.controller.ControllerEvent;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * The bridge between ZooKeeper's callback thread and a tickloom tick loop.
 *
 * <p><b>Why this exists.</b> The ZooKeeper client delivers watch callbacks on its own
 * event thread. A tickloom {@code Process} is single-threaded by design — that is what
 * makes demos reproducible. If a ZK callback mutated broker state directly, we would lose
 * determinism and reintroduce the data races that make distributed code untestable.
 *
 * <p><b>This is what real Kafka does.</b> Modern Kafka's controller has exactly this
 * shape: {@code ControllerEventManager} holds a queue, ZooKeeper watches only
 * <i>enqueue</i> a typed {@link ControllerEvent}, and a single {@code ControllerEventThread}
 * drains it. The redesign happened precisely because ZK callbacks mutating controller
 * state directly caused concurrency bugs. Here the tick loop plays the part of that
 * single thread.
 *
 * <pre>
 *   ZooKeeper event thread ──enqueue(ControllerEvent)──▶ [queue] ──drain on tick()──▶ handler
 *        (many threads)                                             (exactly one thread)
 * </pre>
 *
 * <p>Only {@link #enqueue} is safe to call from the ZooKeeper thread. {@link #drain} must
 * be called from the tick thread and nowhere else.
 *
 * <p><b>Level-triggered invalidation.</b> Watch events carry no snapshot data. When
 * drained, the controller queries ZooKeeper directly for the authoritative, current truth
 * (identical to the Kubernetes ListWatch pattern).
 */
public final class ZkEventQueue {

    private final Queue<ControllerEvent> events = new ConcurrentLinkedQueue<>();
    private final AtomicLong enqueued = new AtomicLong();
    private long drained;

    /** Called from the ZooKeeper event thread. Does no work beyond handing the event over. */
    public void enqueue(ControllerEvent event) {
        events.add(event);
        enqueued.incrementAndGet();
    }

    /**
     * Called from the tick thread only. Processes every event queued since the last tick,
     * in arrival order.
     *
     * @param processor the event processor (e.g. {@code ZkController::process})
     * @return how many events ran this tick
     */
    public int drain(Consumer<ControllerEvent> processor) {
        int count = 0;
        ControllerEvent event;
        while ((event = events.poll()) != null) {
            processor.accept(event);
            count++;
            drained++;
        }
        return count;
    }

    public ControllerEvent peek() { return events.peek(); }
    public int size() { return events.size(); }
    public long enqueuedCount() { return enqueued.get(); }
    public long drainedCount() { return drained; }
    public boolean isEmpty() { return events.isEmpty(); }
}
