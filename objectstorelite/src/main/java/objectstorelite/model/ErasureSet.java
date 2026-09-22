package objectstorelite.model;

import com.tickloom.ProcessId;

import java.util.List;

/**
 * Represents an erasure set consisting of a fixed group of storage nodes.
 *
 * <p>In MinIO, disks are grouped into erasure sets of size N = K + M (e.g. 6 nodes: 4 data + 2 parity).
 * An object key is hashed deterministically to one erasure set, and its N shards map
 * directly to the N nodes in that set.
 */
public record ErasureSet(int setId, List<ProcessId> nodes) {
    public ErasureSet {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("ErasureSet must have at least one node");
        }
        nodes = List.copyOf(nodes);
    }

    public int size() {
        return nodes.size();
    }

    public ProcessId nodeAt(int index) {
        return nodes.get(index);
    }
}
