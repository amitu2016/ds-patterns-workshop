package parquetlite.io;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * An {@link InputFile} over byte ranges that were fetched <b>before</b> decoding began.
 *
 * <h2>Why an {@code InputFile} implementation is needed at all</h2>
 *
 * <p>{@code parquet-mr} only decodes through its own abstraction: {@code ParquetFileReader.open}
 * takes an {@link InputFile}, calls {@code newStream()}, and then seeks wherever the footer tells it
 * to. There is no API that accepts "here are some bytes, decode this row group". So anyone reading
 * Parquet over a network must supply an {@code InputFile} — and over object storage, nobody ever
 * holds the whole file. A read is always <i>blocks</i>: the footer first, because only it says where
 * the row groups are, and then the row groups a predicate did not eliminate.
 *
 * <p>That is the gap this fills: an {@code InputFile} backed by selected blocks rather than by a
 * file or a stream. Everything else follows from it.
 *
 * <h2>The loop, and what the throw checks</h2>
 *
 * <p>Reading is a prediction and a verification:
 *
 * <ol>
 *   <li>read the footer, and from it compute exactly which ranges are needed</li>
 *   <li>fetch those ranges — one round trip each, and no more</li>
 *   <li>hand them to {@code parquet-mr} and let it read</li>
 * </ol>
 *
 * <p>Step 1 predicts what step 3 will ask for. A read outside every prefetched range means the
 * prediction was wrong, so {@link #with} throws rather than letting a task silently return the
 * wrong rows. {@code PrefetchedInputFileTest} pins the prediction down: footer plus one row group is
 * all {@code ParquetFileReader} touches to decode that row group — which is why a task can make
 * exactly one network call.
 *
 * <p>At least two ranges are therefore in play on any selective read — the footer and a row group —
 * and they are disjoint, which is why a single contiguous slice will not do. Use {@link #wholeObject}
 * when the whole object is in hand; it is the same view with one range spanning everything, so
 * nothing can be missing and the throw can never fire.
 *
 * <h2>Prefetching rather than reading on demand</h2>
 *
 * <p>Upstream's {@code S3AInputStream} issues a GET from inside {@code read()}, discovering ranges
 * as it decodes. That is not available here: a task on a {@code sparklite} worker cannot block —
 * {@code ObjectStoreClient.getObjectRange} returns a future and tickloom has no thread pool to wait
 * on one — so the ranges must be known and fetched first. Hadoop 3.4 ships the same strategy as
 * {@code S3APrefetchingInputStream}, for latency rather than for want of a thread.
 */
public final class PrefetchedInputFile implements InputFile {

    private final long totalFileSize;
    /** baseOffset -> bytes at that absolute offset. */
    private final NavigableMap<Long, byte[]> ranges = new TreeMap<>();

    private PrefetchedInputFile(long totalFileSize) {
        this.totalFileSize = totalFileSize;
    }

    /** A view over selected ranges, added with {@link #with}. */
    public static PrefetchedInputFile of(long totalFileSize) {
        return new PrefetchedInputFile(totalFileSize);
    }

    /**
     * A view over an object that arrived whole, in one GET — deltalite decoding a data file, or a
     * reader with no footer statistics to skip on.
     *
     * <p>The same type as a selective view, because it is the same thing with one range covering
     * everything: no read can fall outside it, so the throw that guards a selective read can never
     * fire here. The factory name is the promise — every byte is present.
     */
    public static PrefetchedInputFile wholeObject(byte[] data) {
        return of(data.length).with(0, data);
    }

    /** Adds a slice covering {@code [baseOffset, baseOffset + bytes.length)} of the object. */
    public PrefetchedInputFile with(long baseOffset, byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        if (baseOffset < 0 || baseOffset + bytes.length > totalFileSize) {
            throw new IllegalArgumentException(String.format(
                    "slice [%d, %d) lies outside the object (size %d)",
                    baseOffset, baseOffset + bytes.length, totalFileSize));
        }
        ranges.put(baseOffset, bytes);
        return this;
    }

    @Override
    public long getLength() {
        return totalFileSize;
    }

    @Override
    public SeekableInputStream newStream() {
        return new PrefetchedSeekableInputStream(ranges, totalFileSize);
    }

    @Override
    public String toString() {
        return "PrefetchedInputFile(size=" + totalFileSize + ", ranges=" + describe(ranges) + ")";
    }

    private static String describe(NavigableMap<Long, byte[]> ranges) {
        return ranges.entrySet().stream()
                .map(e -> "[" + e.getKey() + ", " + (e.getKey() + e.getValue().length) + ")")
                .collect(Collectors.joining(", "));
    }

    /**
     * Resolves each read to the slice containing the current position. Seeking anywhere is allowed —
     * {@code ParquetFileReader} seeks speculatively — but reading outside every slice throws.
     */
    static final class PrefetchedSeekableInputStream extends SeekableInputStream {

        private final NavigableMap<Long, byte[]> ranges;
        private final long totalFileSize;
        private long position;
        private boolean closed = false;

        PrefetchedSeekableInputStream(NavigableMap<Long, byte[]> ranges, long totalFileSize) {
            this.ranges = ranges;
            this.totalFileSize = totalFileSize;
            this.position = 0;
        }

        @Override
        public long getPos() {
            return position;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (newPos < 0 || newPos > totalFileSize) {
                throw new IOException(String.format(
                        "Seek position %d is outside the object (size %d)", newPos, totalFileSize));
            }
            this.position = newPos;
        }

        /** The slice containing {@code position}, or null if no prefetched slice covers it. */
        private Map.Entry<Long, byte[]> sliceAt(long pos) {
            Map.Entry<Long, byte[]> candidate = ranges.floorEntry(pos);
            if (candidate == null) {
                return null;
            }
            return pos < candidate.getKey() + candidate.getValue().length ? candidate : null;
        }

        private IOException notPrefetched(long pos, int len) {
            return new IOException(String.format(
                    "read of %d byte(s) at offset %d was not prefetched; available ranges: %s. "
                            + "The range calculation that chose what to fetch is wrong.",
                    len, pos, describe(ranges)));
        }

        @Override
        public int read() throws IOException {
            byte[] one = new byte[1];
            int n = read(one, 0, 1);
            return n == -1 ? -1 : one[0] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (closed) {
                throw new IOException("Stream closed");
            }
            if (len == 0) {
                return 0;
            }
            if (position >= totalFileSize) {
                return -1;
            }
            Map.Entry<Long, byte[]> slice = sliceAt(position);
            if (slice == null) {
                throw notPrefetched(position, len);
            }
            byte[] data = slice.getValue();
            int index = (int) (position - slice.getKey());
            int toRead = Math.min(len, data.length - index);
            System.arraycopy(data, index, b, off, toRead);
            position += toRead;
            return toRead;
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            if (!buf.hasRemaining()) {
                return 0;
            }
            byte[] temp = new byte[buf.remaining()];
            int n = read(temp, 0, temp.length);
            if (n > 0) {
                buf.put(temp, 0, n);
            }
            return n;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            long requestedAt = position;
            int total = 0;
            while (total < len) {
                int n = read(bytes, start + total, len - total);
                if (n == -1) {
                    throw new EOFException(String.format(
                            "Reached EOF after %d of %d byte(s) requested at offset %d",
                            total, len, requestedAt));
                }
                total += n;
            }
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                if (read(buf) == -1) {
                    throw new EOFException("Reached EOF before filling buffer");
                }
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
