package sparklite.function;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * A serializable predicate interface, corresponding to {@code org.apache.spark.api.java.function.FilterFunction}.
 *
 * @param <T> Target element type
 */
@FunctionalInterface
public interface PredicateLite<T> extends Predicate<T>, Serializable {
}
