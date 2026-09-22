package parquetlite.io;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tracks and logs explicit object store range read requests for educational visibility.
 */
public class RangeMetrics {

    public record RangeRequest(String key, long offset, int length, String description) {
        @Override
        public String toString() {
            return String.format("Range[%d..%d] (%d bytes) - %s", offset, offset + length - 1, length, description);
        }
    }

    private final List<RangeRequest> requests = new ArrayList<>();
    private long totalBytesTransferred = 0;

    public synchronized void record(String key, long offset, int length, String description) {
        requests.add(new RangeRequest(key, offset, length, description));
        totalBytesTransferred += length;
    }

    public synchronized long totalBytesTransferred() {
        return totalBytesTransferred;
    }

    public synchronized int totalRequests() {
        return requests.size();
    }

    public synchronized List<RangeRequest> requests() {
        return Collections.unmodifiableList(new ArrayList<>(requests));
    }

    public synchronized void reset() {
        requests.clear();
        totalBytesTransferred = 0;
    }

    public synchronized void printTimeline() {
        System.out.printf("   🌐 Network Timeline (%d range requests, %d total bytes transferred):%n",
                requests.size(), totalBytesTransferred);
        for (int i = 0; i < requests.size(); i++) {
            RangeRequest req = requests.get(i);
            System.out.printf("      [%d] GET %s bytes=%d-%d (%d bytes) ──► %s%n",
                    i + 1, req.key(), req.offset(), req.offset() + req.length() - 1, req.length(), req.description());
        }
    }
}
