package sparklite.rdd;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A partition implementation for {@link ParallelCollectionRDDLite} that stores elements locally.
 *
 * @param <T> The type of data stored in this partition
 */
public class ParallelCollectionPartition<T> implements Partition {
    private final int partitionId;
    private final List<T> data;

    public ParallelCollectionPartition(int partitionId, List<T> data) {
        this.partitionId = partitionId;
        this.data = new ArrayList<>(data);
    }

    @Override
    public int index() {
        return partitionId;
    }

    public Iterator<T> iterator() {
        return data.iterator();
    }

    public List<T> getData() {
        return data;
    }
}
