package parquetlite.io;

/**
 * A half-open byte range {@code [offset, offset + length)} of an object.
 *
 * <p>What a reader computes before it transfers anything: a Parquet read over object storage is a
 * sequence of explicit range GETs, and this is the request. Deciding the ranges up front — rather
 * than letting a stream discover them as it decodes — is what lets the same code run on a driver
 * that can block and on a worker that cannot.
 */
public record ByteRange(long offset, int length) {

    public ByteRange {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must not be negative: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must not be negative: " + length);
        }
    }

    public long endExclusive() {
        return offset + length;
    }

    @Override
    public String toString() {
        return "[" + offset + ", " + endExclusive() + ") " + length + " bytes";
    }
}
