package objectstorelite.codec;

import com.tickloom.ProcessId;
import objectstorelite.model.ErasureSet;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32;

/**
 * Maps an object key to an erasure set index or an {@link ErasureSet} directly,
 * identical to MinIO's set hashing.
 *
 * <p>Two algorithms are supported:
 * <ul>
 *     <li>CRCMOD (legacy MinIO) - {@code CRC32(key) % setCount}</li>
 *     <li>SIPMOD (default MinIO) - {@code SipHash-2-4(key, deploymentId) % setCount}</li>
 * </ul>
 */
public final class ErasureSetMapper {
    public enum Algorithm {
        CRCMOD,
        SIPMOD
    }

    private final Algorithm algorithm;
    private final byte[] deploymentId; // 16 bytes used as siphash key
    private final int setCount;
    private final List<ErasureSet> erasureSets;

    public ErasureSetMapper(Algorithm algorithm, byte[] deploymentId, int setCount) {
        this(algorithm, deploymentId, setCount, List.of());
    }

    public ErasureSetMapper(Algorithm algorithm, byte[] deploymentId, List<ErasureSet> erasureSets) {
        this(algorithm, deploymentId, erasureSets.size(), erasureSets);
    }

    private ErasureSetMapper(Algorithm algorithm, byte[] deploymentId, int setCount, List<ErasureSet> erasureSets) {
        this.algorithm = Objects.requireNonNull(algorithm, "algorithm");
        this.deploymentId = deploymentId;
        this.setCount = setCount;
        this.erasureSets = List.copyOf(erasureSets);
    }

    public int indexFor(String objectKey) {
        if (setCount <= 0) {
            return -1;
        }
        return switch (algorithm) {
            case CRCMOD -> crcMod(objectKey, setCount);
            case SIPMOD -> sipMod(objectKey, setCount, deploymentId);
        };
    }

    public ErasureSet setFor(String objectKey) {
        int idx = indexFor(objectKey);
        if (idx < 0 || idx >= erasureSets.size()) {
            throw new IllegalStateException("No erasure set available for index " + idx);
        }
        return erasureSets.get(idx);
    }

    public Algorithm algorithm() {
        return algorithm;
    }

    /**
     * Every node in every erasure set — i.e. the storage nodes, and nothing else.
     *
     * <p>A LIST has no key to hash, so it must fan out. It must not fan out to
     * {@code getAllNodes()}: on a shared bus (the capstone runs brokers and Spark workers
     * alongside storage) that would send internal shard messages to processes with no handler
     * for them, and the listing would silently under-report.
     */
    public List<ProcessId> allNodes() {
        java.util.LinkedHashSet<ProcessId> nodes = new java.util.LinkedHashSet<>();
        for (ErasureSet set : erasureSets) {
            nodes.addAll(set.nodes());
        }
        return List.copyOf(nodes);
    }

    public int setCount() {
        return setCount;
    }

    private static int crcMod(String key, int setCount) {
        CRC32 crc = new CRC32();
        crc.update(key.getBytes(StandardCharsets.UTF_8));
        return (int) (crc.getValue() % setCount);
    }

    private static int sipMod(String key, int setCount, byte[] deploymentId) {
        if (deploymentId == null || deploymentId.length < 16) {
            // fallback to crc when key material is missing
            return crcMod(key, setCount);
        }
        long k0 = bytesToLongLE(deploymentId, 0);
        long k1 = bytesToLongLE(deploymentId, 8);
        long hash = sipHash24(key.getBytes(StandardCharsets.UTF_8), k0, k1);
        return (int) Math.floorMod(hash, (long) setCount);
    }

    private static long bytesToLongLE(byte[] b, int off) {
        return ((long) b[off] & 0xff)
                | (((long) b[off + 1] & 0xff) << 8)
                | (((long) b[off + 2] & 0xff) << 16)
                | (((long) b[off + 3] & 0xff) << 24)
                | (((long) b[off + 4] & 0xff) << 32)
                | (((long) b[off + 5] & 0xff) << 40)
                | (((long) b[off + 6] & 0xff) << 48)
                | (((long) b[off + 7] & 0xff) << 56);
    }

    /**
     * Minimal SipHash-2-4 implementation adapted for key->set hashing.
     */
    private static long sipHash24(byte[] data, long k0, long k1) {
        long v0 = 0x736f6d6570736575L ^ k0;
        long v1 = 0x646f72616e646f6dL ^ k1;
        long v2 = 0x6c7967656e657261L ^ k0;
        long v3 = 0x7465646279746573L ^ k1;

        int end = data.length - (data.length % 8);
        for (int i = 0; i < end; i += 8) {
            long m = bytesToLongLE(data, i);
            v3 ^= m;
            for (int r = 0; r < 2; r++) {
                v0 += v1;
                v1 = rotl(v1, 13);
                v1 ^= v0;
                v0 = rotl(v0, 32);
                v2 += v3;
                v3 = rotl(v3, 16);
                v3 ^= v2;
                v0 += v3;
                v3 = rotl(v3, 21);
                v3 ^= v0;
                v2 += v1;
                v1 = rotl(v1, 17);
                v1 ^= v2;
                v2 = rotl(v2, 32);
            }
            v0 ^= m;
        }

        long b = ((long) data.length) << 56;
        int left = data.length - end;
        switch (left) {
            case 7 -> b |= ((long) data[end + 6] & 0xff) << 48;
            case 6 -> b |= ((long) data[end + 5] & 0xff) << 40;
            case 5 -> b |= ((long) data[end + 4] & 0xff) << 32;
            case 4 -> b |= ((long) data[end + 3] & 0xff) << 24;
            case 3 -> b |= ((long) data[end + 2] & 0xff) << 16;
            case 2 -> b |= ((long) data[end + 1] & 0xff) << 8;
            case 1 -> b |= ((long) data[end] & 0xff);
            default -> {}
        }

        v3 ^= b;
        for (int r = 0; r < 2; r++) {
            v0 += v1;
            v1 = rotl(v1, 13);
            v1 ^= v0;
            v0 = rotl(v0, 32);
            v2 += v3;
            v3 = rotl(v3, 16);
            v3 ^= v2;
            v0 += v3;
            v3 = rotl(v3, 21);
            v3 ^= v0;
            v2 += v1;
            v1 = rotl(v1, 17);
            v1 ^= v2;
            v2 = rotl(v2, 32);
        }
        v0 ^= b;

        v2 ^= 0xff;
        for (int r = 0; r < 4; r++) {
            v0 += v1;
            v1 = rotl(v1, 13);
            v1 ^= v0;
            v0 = rotl(v0, 32);
            v2 += v3;
            v3 = rotl(v3, 16);
            v3 ^= v2;
            v0 += v3;
            v3 = rotl(v3, 21);
            v3 ^= v0;
            v2 += v1;
            v1 = rotl(v1, 17);
            v1 ^= v2;
            v2 = rotl(v2, 32);
        }

        return v0 ^ v1 ^ v2 ^ v3;
    }

    private static long rotl(long x, int b) {
        return (x << b) | (x >>> (64 - b));
    }
}
