package kafkalite.controller;

import com.tickloom.ProcessId;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageBus;
import com.tickloom.messaging.MessageType;
import com.tickloom.network.JsonMessageCodec;
import com.tickloom.network.MessageCodec;
import com.tickloom.network.PeerType;
import kafkalite.cluster.Broker;
import kafkalite.cluster.LeaderAndIsrRequest;
import kafkalite.cluster.UpdateMetadataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Function;

/**
 * Mirrors {@code kafka.controller.ControllerChannelManager}.
 *
 * <p>Encapsulates controller-to-broker network communication. In real Kafka,
 * {@code ControllerChannelManager} maintains network connections and per-broker
 * message queues. In {@code kafkalite}, it routes requests through tickloom's
 * {@link MessageBus} (over {@code SimulatedNetwork} in tests or {@code NioNetwork} in production).
 */
public class ControllerChannelManager {
    private static final Logger logger = LoggerFactory.getLogger(ControllerChannelManager.class);

    public static final MessageType UPDATE_METADATA = MessageType.of("UpdateMetadata");
    public static final MessageType LEADER_AND_ISR = MessageType.of("LeaderAndIsr");

    private final MessageBus messageBus;
    private final MessageCodec messageCodec;
    private final int controllerBrokerId;
    private final Function<Integer, ProcessId> processIdResolver;

    public ControllerChannelManager(MessageBus messageBus,
                                    MessageCodec messageCodec,
                                    int controllerBrokerId,
                                    Function<Integer, ProcessId> processIdResolver) {
        this.messageBus = Objects.requireNonNull(messageBus, "messageBus");
        this.messageCodec = messageCodec != null ? messageCodec : new JsonMessageCodec();
        this.controllerBrokerId = controllerBrokerId;
        this.processIdResolver = processIdResolver != null ? processIdResolver : id -> ProcessId.of("broker-" + id);
    }

    public ControllerChannelManager(MessageBus messageBus, int controllerBrokerId) {
        this(messageBus, new JsonMessageCodec(), controllerBrokerId, id -> ProcessId.of("broker-" + id));
    }

    public void sendLeaderAndIsr(int targetBrokerId, LeaderAndIsrRequest request) {
        ProcessId sender = processIdResolver.apply(controllerBrokerId);
        ProcessId target = processIdResolver.apply(targetBrokerId);
        try {
            byte[] payload = messageCodec.encode(request);
            Message msg = Message.of(
                    sender,
                    target,
                    PeerType.SERVER,
                    LEADER_AND_ISR,
                    payload,
                    "lisr-" + request.controllerEpoch()
            );
            messageBus.sendMessage(msg);
        } catch (IOException e) {
            logger.warn("Broker {}: failed sending LeaderAndIsr to {}", controllerBrokerId, target, e);
        }
    }

    public void sendUpdateMetadata(Collection<Broker> targetBrokers, UpdateMetadataRequest request) {
        ProcessId sender = processIdResolver.apply(controllerBrokerId);
        byte[] payload = messageCodec.encode(request);
        for (Broker broker : targetBrokers) {
            ProcessId target = processIdResolver.apply(broker.id());
            try {
                Message msg = Message.of(
                        sender,
                        target,
                        PeerType.SERVER,
                        UPDATE_METADATA,
                        payload,
                        "meta-" + request.controllerEpoch()
                );
                messageBus.sendMessage(msg);
            } catch (IOException e) {
                logger.warn("Broker {}: failed sending UpdateMetadata to {}", controllerBrokerId, target, e);
            }
        }
    }

    public MessageBus messageBus() {
        return messageBus;
    }

    public MessageCodec messageCodec() {
        return messageCodec;
    }
}
