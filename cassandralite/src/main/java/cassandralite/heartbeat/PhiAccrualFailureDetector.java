package cassandralite.heartbeat;

import com.tickloom.ProcessId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Mirrors {@code org.apache.cassandra.gms.FailureDetector}.
 *
 * <p>Implementation of the &phi; (Phi) Accrual Failure Detector by Hayashibara et al.
 * Rather than returning a binary alive/dead verdict based on a fixed timeout, the failure
 * detector computes a continuous suspiciousness metric &phi; representing the probability
 * of heartbeat delay.
 *
 * <p>When &phi; exceeds {@code phiConvictThreshold} (default 8.0, meaning delay probability
 * is less than 10^-8), the endpoint is convicted and marked {@link ServerState#DOWN}.
 */
public class PhiAccrualFailureDetector implements FailureDetector {

    private static final Logger logger = LoggerFactory.getLogger(PhiAccrualFailureDetector.class);

    public static final double DEFAULT_CONVICT_THRESHOLD = 8.0;
    private static final int DEFAULT_SAMPLE_SIZE = 1000;

    private final double convictThreshold;
    private final int sampleSize;
    private final Map<ProcessId, ArrivalWindow> arrivalSamples = new ConcurrentHashMap<>();
    private final Map<ProcessId, ServerState> serverStates = new ConcurrentHashMap<>();
    private BiConsumer<ProcessId, ServerState> stateChangeListener;

    public PhiAccrualFailureDetector() {
        this(DEFAULT_CONVICT_THRESHOLD, DEFAULT_SAMPLE_SIZE);
    }

    public PhiAccrualFailureDetector(double convictThreshold) {
        this(convictThreshold, DEFAULT_SAMPLE_SIZE);
    }

    public PhiAccrualFailureDetector(double convictThreshold, int sampleSize) {
        this.convictThreshold = convictThreshold;
        this.sampleSize = sampleSize;
    }

    public void setStateChangeListener(BiConsumer<ProcessId, ServerState> listener) {
        this.stateChangeListener = listener;
    }

    @Override
    public void report(ProcessId endpoint, long nowTick) {
        ArrivalWindow window = arrivalSamples.computeIfAbsent(endpoint, ep -> new ArrivalWindow(sampleSize));
        window.add(nowTick);

        ServerState previousState = serverStates.put(endpoint, ServerState.UP);
        if (previousState != ServerState.UP) {
            logger.info("Endpoint '{}' marked UP at tick {}", endpoint, nowTick);
            if (stateChangeListener != null) {
                stateChangeListener.accept(endpoint, ServerState.UP);
            }
        }
    }

    @Override
    public void interpret(ProcessId endpoint, long nowTick) {
        ArrivalWindow window = arrivalSamples.get(endpoint);
        if (window == null) {
            return;
        }

        double phi = window.phi(nowTick);
        ServerState currentState = serverStates.getOrDefault(endpoint, ServerState.DOWN);

        if (currentState == ServerState.UP && phi >= convictThreshold) {
            logger.warn("Endpoint '{}' phi {} >= threshold {} -> convicted to DOWN at tick {}",
                    endpoint, phi, convictThreshold, nowTick);
            serverStates.put(endpoint, ServerState.DOWN);
            if (stateChangeListener != null) {
                stateChangeListener.accept(endpoint, ServerState.DOWN);
            }
        }
    }

    @Override
    public boolean isAlive(ProcessId endpoint) {
        return serverStates.getOrDefault(endpoint, ServerState.DOWN) == ServerState.UP;
    }

    @Override
    public double getPhi(ProcessId endpoint, long nowTick) {
        ArrivalWindow window = arrivalSamples.get(endpoint);
        return window != null ? window.phi(nowTick) : 0.0;
    }

    @Override
    public ServerState getState(ProcessId endpoint) {
        return serverStates.getOrDefault(endpoint, ServerState.DOWN);
    }

    public double convictThreshold() {
        return convictThreshold;
    }

    public ArrivalWindow getArrivalWindow(ProcessId endpoint) {
        return arrivalSamples.get(endpoint);
    }
}
