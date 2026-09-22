package sparklite.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A serializable function interface, corresponding to {@code org.apache.spark.api.java.function.Function}.
 *
 * @param <T> Input type
 * @param <R> Return type
 */
@FunctionalInterface
public interface FunctionLite<T, R> extends Function<T, R>, Serializable {
}
