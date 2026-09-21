package cassandralite.heartbeat;

import java.util.Arrays;

/**
 * Mirrors {@code org.apache.cassandra.gms.ArrivalWindow}.
 *
 * <p>A sliding window of inter-arrival times (measured in ticks) used by the
 * {@link PhiAccrualFailureDetector} to compute the suspiciousness value &phi;.
 *
 * <p>See CASSANDRA-2597 and Hayashibara et al. (2004) "The &phi; Accrual Failure Detector".
 */
public class ArrivalWindow {

    private static final double PHI_FACTOR = 1.0 / Math.log(10.0); // 0.4342944819...

    private final long[] intervals;
    private int index = 0;
    private boolean isFilled = false;
    private long sum = 0;
    private double mean = 1.0;

    private long tLast = 0L;
    private double lastReportedPhi = 0.0;

    private final long initialInterval;
    private final long maxInterval;

    public ArrivalWindow(int size, long initialInterval, long maxInterval) {
        this.intervals = new long[size];
        this.initialInterval = initialInterval;
        this.maxInterval = maxInterval;
    }

    public ArrivalWindow(int size) {
        this(size, 2L, 1000L);
    }

    /**
     * Records arrival of a heartbeat at tick {@code nowTick}.
     */
    public synchronized void add(long nowTick) {
        if (tLast > 0L) {
            long interArrivalTime = nowTick - tLast;
            if (interArrivalTime <= maxInterval) {
                addInterval(Math.max(1L, interArrivalTime));
            }
        } else {
            addInterval(initialInterval);
        }
        tLast = nowTick;
    }

    private void addInterval(long interval) {
        if (index == intervals.length) {
            isFilled = true;
            index = 0;
        }
        if (isFilled) {
            sum -= intervals[index];
        }
        intervals[index++] = interval;
        sum += interval;
        mean = (double) sum / size();
    }

    public synchronized int size() {
        return isFilled ? intervals.length : index;
    }

    public synchronized double mean() {
        return Math.max(0.1, mean);
    }

    public synchronized long getLastArrivalTick() {
        return tLast;
    }

    /**
     * Calculates the &phi; value at tick {@code nowTick}.
     *
     * <p>&phi; represents the probability that a heartbeat is late. Assuming an exponential distribution:
     * <pre>
     *   P_later(t) = exp(-t / mean)
     *   &phi; = -log10(P_later(t)) = (t / mean) * (1 / ln(10))
     * </pre>
     */
    public synchronized double phi(long nowTick) {
        if (tLast <= 0L) {
            return 0.0;
        }
        long t = nowTick - tLast;
        if (t <= 0) {
            lastReportedPhi = 0.0;
            return 0.0;
        }
        lastReportedPhi = (t / mean()) * PHI_FACTOR;
        return lastReportedPhi;
    }

    public synchronized double getLastReportedPhi() {
        return lastReportedPhi;
    }

    @Override
    public synchronized String toString() {
        return "ArrivalWindow{mean=" + mean() + ", tLast=" + tLast + ", lastPhi=" + lastReportedPhi + "}";
    }
}
