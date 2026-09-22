package objectstorelite.model;

/**
 * Represents a single shard (data or parity) in memory.
 */
public record Shard(byte[] bytes) {
    public Shard {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes cannot be null");
        }
    }

    public void write(byte[] fromData, int offset, int bytesRead) {
        System.arraycopy(fromData, offset, bytes, 0, Math.min(bytes.length, bytesRead));
    }
}
