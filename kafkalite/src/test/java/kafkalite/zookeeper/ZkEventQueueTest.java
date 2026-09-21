package kafkalite.zookeeper;

import kafkalite.controller.ControllerEvent;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test, not a demo — deliberately not named {@code Demo_*}.
 *
 * <p>It pins the queue's contract with typed {@link ControllerEvent}s.
 */
class ZkEventQueueTest {

    @Test
    void enqueueDoesNoWork() {
        ZkEventQueue queue = new ZkEventQueue();
        List<ControllerEvent> processed = new ArrayList<>();

        queue.enqueue(new ControllerEvent.BrokerChange());
        queue.enqueue(new ControllerEvent.TopicChange());

        assertEquals(List.of(), processed, "enqueue must only hand the event over");
        assertEquals(2, queue.enqueuedCount());
        assertEquals(0, queue.drainedCount());
    }

    @Test
    void drainRunsEveryEventInArrivalOrder() {
        ZkEventQueue queue = new ZkEventQueue();
        List<ControllerEvent> processed = new ArrayList<>();

        queue.enqueue(new ControllerEvent.BrokerChange());
        queue.enqueue(new ControllerEvent.TopicChange());
        queue.enqueue(new ControllerEvent.ControllerUpdated());
        queue.enqueue(new ControllerEvent.ControllerDeleted());

        assertEquals(4, queue.drain(processed::add));
        assertEquals(List.of(
                new ControllerEvent.BrokerChange(),
                new ControllerEvent.TopicChange(),
                new ControllerEvent.ControllerUpdated(),
                new ControllerEvent.ControllerDeleted()
        ), processed, "events must run in the order they arrived");
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.drain(processed::add), "a second drain has nothing left to do");
    }

    @Test
    void acceptsConcurrentProducersAndRunsThemOnTheDrainingThreadOnly() throws Exception {
        ZkEventQueue queue = new ZkEventQueue();
        List<String> executingThreads = new ArrayList<>(); // unsynchronised on purpose: only drain writes it

        int producers = 4, perProducer = 250;
        CountDownLatch done = new CountDownLatch(producers);
        for (int p = 0; p < producers; p++) {
            new Thread(() -> {
                for (int i = 0; i < perProducer; i++) {
                    queue.enqueue(new ControllerEvent.BrokerChange());
                }
                done.countDown();
            }).start();
        }
        assertTrue(done.await(5, TimeUnit.SECONDS));

        queue.drain(e -> executingThreads.add(Thread.currentThread().getName()));

        assertEquals(producers * perProducer, executingThreads.size());
        assertEquals(1, executingThreads.stream().distinct().count(),
                "every event ran on whichever single thread called drain()");
    }
}
