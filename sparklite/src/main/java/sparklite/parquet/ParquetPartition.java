package sparklite.parquet;

import sparklite.rdd.Partition;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a partition of an RDD mapped 1:1 with a Parquet Row Group.
 *
 * <p>Contains physical metadata parsed from the Parquet file footer:
 * <ul>
 *   <li>{@code rowGroupIndex}: The ordinal block index within the Parquet file</li>
 *   <li>{@code startOffset}: Starting byte offset in object storage</li>
 *   <li>{@code length}: Total byte size of all column chunks in the row group</li>
 *   <li>{@code rowCount}: Total records stored within this row group</li>
 *   <li>{@code preferredLocations}: Node/worker IDs where the target data shards are co-located</li>
 * </ul>
 */
public class ParquetPartition implements Partition {
    private final int partitionIndex;
    private final String objectKey;
    private final int rowGroupIndex;
    private final long rowCount;
    private final long startOffset;
    private final long length;
    private final List<String> preferredLocations;

    public ParquetPartition(int partitionIndex,
                            String objectKey,
                            int rowGroupIndex,
                            long rowCount,
                            long startOffset,
                            long length,
                            List<String> preferredLocations) {
        this.partitionIndex = partitionIndex;
        this.objectKey = Objects.requireNonNull(objectKey, "objectKey");
        this.rowGroupIndex = rowGroupIndex;
        this.rowCount = rowCount;
        this.startOffset = startOffset;
        this.length = length;
        this.preferredLocations = preferredLocations == null
                ? Collections.emptyList()
                : List.copyOf(preferredLocations);
    }

    @Override
    public int index() {
        return partitionIndex;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public int getRowGroupIndex() {
        return rowGroupIndex;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getLength() {
        return length;
    }

    public List<String> getPreferredLocations() {
        return preferredLocations;
    }

    @Override
    public String toString() {
        return String.format("ParquetPartition[index=%d, key='%s', rowGroup=%d, rows=%d, range=[%d..%d], preferred=%s]",
                partitionIndex, objectKey, rowGroupIndex, rowCount, startOffset, startOffset + length, preferredLocations);
    }
}
