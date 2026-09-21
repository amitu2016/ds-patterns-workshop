package kafkalite.client;

import com.tickloom.ProcessId;
import com.tickloom.ProcessParams;
import com.tickloom.algorithms.replication.ClusterClient;
import com.tickloom.future.TickCompletableFuture;
import com.tickloom.messaging.Message;
import com.tickloom.messaging.MessageType;
import kafkalite.api.FetchRequest;
import kafkalite.api.FetchResponse;
import kafkalite.common.TopicAndPartition;
import kafkalite.server.BrokerServer;

import java.util.List;
import java.util.Map;

/**
 * The read side of a Kafka client, corresponding to {@code org.apache.kafka.clients.consumer.KafkaConsumer}
 * — narrowed to the one thing a batch reader needs: fetch a range of a partition from its leader.
 *
 * <p>Sends {@link FetchRequest} with {@code replicaId = -1}, which makes the broker apply
 * {@code FetchIsolation.HIGH_WATERMARK}: records a leader has appended but not yet committed stay
 * invisible. That isolation is enforced by the broker, not here — a consumer cannot opt out of it.
 *
 * <p><b>No metadata lookup.</b> A real consumer is configured with {@code bootstrap.servers}, asks
 * any broker which one leads each partition, and refreshes that mapping when it is told
 * {@code NOT_LEADER_FOR_PARTITION}. kafkalite has no client-facing metadata request — the controller
 * pushes leadership to brokers via {@code UpdateMetadata}/{@code LeaderAndIsr}, and nothing exposes
 * the resulting cache — so the caller passes the leader explicitly. The consequence is real and is
 * the reason {@link FetchResponse#NOT_LEADER_FOR_PARTITION} matters: a leader resolved when a job was
 * planned can have moved by the time the task runs, which is demo 2.2's scenario one module earlier.
 */
public class KafkaConsumerLite extends ClusterClient {

    public KafkaConsumerLite(List<ProcessId> brokers, ProcessParams processParams) {
        super(brokers, processParams);
    }

    @Override
    protected Map<MessageType, Handler> initialiseHandlers() {
        return Map.of(BrokerServer.FETCH_RESPONSE, this::handleFetchResponse);
    }

    /**
     * Fetches from {@code startOffset} through the partition's high-watermark.
     *
     * @param leader the broker believed to lead this partition; if it does not, the response carries
     *               {@link FetchResponse#NOT_LEADER_FOR_PARTITION} rather than stale records
     */
    public TickCompletableFuture<FetchResponse> fetch(ProcessId leader,
                                                      TopicAndPartition topicAndPartition,
                                                      long startOffset) {
        return sendRequest(new FetchRequest(topicAndPartition, startOffset), leader, BrokerServer.FETCH);
    }

    private void handleFetchResponse(Message message) {
        FetchResponse response = deserializePayload(message.payload(), FetchResponse.class);
        handleResponse(message.correlationId(), response, message.source());
    }
}
