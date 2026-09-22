package sparklite.rdd;

import java.io.Serializable;

/**
 * An identifier for a partition in an {@link RDDLite}.
 * Corresponds to {@code org.apache.spark.Partition}.
 */
public interface Partition extends Serializable {
    /**
     * The index of this partition within its parent RDD.
     */
    int index();
}
