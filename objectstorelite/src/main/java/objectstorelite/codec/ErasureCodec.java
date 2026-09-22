package objectstorelite.codec;

import com.backblaze.erasure.ReedSolomon;
import objectstorelite.model.Shard;
import objectstorelite.model.ShardFile;
import objectstorelite.model.ShardMetadata;
import objectstorelite.model.VersionEntry;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Reed-Solomon erasure coding wrapper powered by Backblaze Galois field tables.
 * Supports both in-memory byte arrays and streamed multi-megabyte files (e.g. video files).
 */
public final class ErasureCodec {

    public static final int DEFAULT_BLOCK_SIZE = 1024 * 1024; // 1 MB blocks

    private ErasureCodec() {}

    /**
     * Computes the shard size in bytes for a block of data across N data shards,
     * padding to ensure all shards are equally sized.
     */
    public static int getBytesInShard(int noOfShards, int bytesInBlock) {
        int bytesWithPadding = bytesInBlock + paddingBytes(bytesInBlock, noOfShards);
        return bytesWithPadding / noOfShards;
    }

    public static int paddingBytes(int bytesInBlock, int noOfShards) {
        int remainder = bytesInBlock % noOfShards;
        return remainder == 0 ? 0 : noOfShards - remainder;
    }

    /**
     * Encodes in-memory byte array into (dataShards + parityShards) shards.
     */
    public static byte[][] encode(byte[] data, int dataShards, int parityShards) {
        int totalShards = dataShards + parityShards;
        int shardSize = getBytesInShard(dataShards, data.length);
        byte[][] shards = new byte[totalShards][shardSize];

        for (int i = 0; i < dataShards; i++) {
            int offset = i * shardSize;
            if (offset < data.length) {
                int bytesToCopy = Math.min(shardSize, data.length - offset);
                System.arraycopy(data, offset, shards[i], 0, bytesToCopy);
            }
        }

        ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
        reedSolomon.encodeParity(shards, 0, shardSize);
        return shards;
    }

    /**
     * Decodes in-memory shards into the original byte array.
     * Reconstructs missing data shards if needed using Reed-Solomon decoding.
     */
    public static byte[] decode(byte[][] shards, boolean[] shardPresent, long objectSize,
                                int dataShards, int parityShards) throws IOException {
        int totalShards = dataShards + parityShards;
        int availableCount = 0;
        for (int i = 0; i < totalShards; i++) {
            if (shardPresent[i] && shards[i] != null) {
                availableCount++;
            }
        }

        if (availableCount < dataShards) {
            throw new IOException("Quorum not met. Available shards: " + availableCount
                    + ", required: " + dataShards);
        }

        int shardSize = 0;
        for (int i = 0; i < totalShards; i++) {
            if (shards[i] != null) {
                shardSize = shards[i].length;
                break;
            }
        }

        // Check if all data shards are already present — if so, RS decode is bypassed!
        boolean allDataPresent = true;
        for (int i = 0; i < dataShards; i++) {
            if (!shardPresent[i] || shards[i] == null) {
                allDataPresent = false;
                break;
            }
        }

        if (!allDataPresent) {
            // Allocate empty buffers for missing shards before calling ReedSolomon
            for (int i = 0; i < totalShards; i++) {
                if (shards[i] == null) {
                    shards[i] = new byte[shardSize];
                }
            }
            ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
            reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);
        }

        byte[] reconstructed = new byte[(int) objectSize];
        int bytesWritten = 0;
        for (int i = 0; i < dataShards; i++) {
            int bytesToCopy = (int) Math.min(shardSize, objectSize - bytesWritten);
            if (bytesToCopy <= 0) {
                break;
            }
            System.arraycopy(shards[i], 0, reconstructed, bytesWritten, bytesToCopy);
            bytesWritten += bytesToCopy;
        }

        return reconstructed;
    }

    /**
     * Encodes an input stream (e.g. 62MB video) into shard files on disk across dataShards + parityShards.
     */
    public static List<ShardFile> encodeStreamToFiles(InputStream input, Path outputDir,
                                                      int dataShards, int parityShards,
                                                      int blockSize) throws IOException {
        Files.createDirectories(outputDir);
        int totalShards = dataShards + parityShards;
        OutputStream[] streams = new OutputStream[totalShards];
        List<ShardFile> shardFiles = new ArrayList<>(totalShards);

        for (int i = 0; i < totalShards; i++) {
            Path path = outputDir.resolve("part." + (i + 1));
            streams[i] = new BufferedOutputStream(Files.newOutputStream(path));
            shardFiles.add(new ShardFile(i, path));
        }

        ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
        byte[] buffer = new byte[blockSize];

        try {
            int bytesRead;
            while ((bytesRead = readFullBlock(input, buffer)) > 0) {
                int shardSize = getBytesInShard(dataShards, bytesRead);
                byte[][] shards = new byte[totalShards][shardSize];

                for (int i = 0; i < dataShards; i++) {
                    int offset = i * shardSize;
                    if (offset < bytesRead) {
                        int count = Math.min(shardSize, bytesRead - offset);
                        System.arraycopy(buffer, offset, shards[i], 0, count);
                    }
                }

                reedSolomon.encodeParity(shards, 0, shardSize);

                for (int i = 0; i < totalShards; i++) {
                    streams[i].write(shards[i], 0, shardSize);
                }
            }
        } finally {
            for (OutputStream os : streams) {
                if (os != null) {
                    try { os.close(); } catch (IOException ignored) {}
                }
            }
        }

        return shardFiles;
    }

    /**
     * Decodes shard files on disk back into the target stream, reconstructing any missing shards on the fly.
     */
    public static void decodeFilesToStream(List<Path> shardPaths, OutputStream output, long objectSize,
                                           int dataShards, int parityShards, int blockSize) throws IOException {
        int totalShards = dataShards + parityShards;
        InputStream[] streams = new InputStream[totalShards];
        boolean[] shardPresent = new boolean[totalShards];
        int availableCount = 0;

        for (int i = 0; i < totalShards; i++) {
            Path path = (i < shardPaths.size()) ? shardPaths.get(i) : null;
            if (path != null && Files.exists(path)) {
                streams[i] = new BufferedInputStream(Files.newInputStream(path));
                shardPresent[i] = true;
                availableCount++;
            } else {
                streams[i] = null;
                shardPresent[i] = false;
            }
        }

        if (availableCount < dataShards) {
            throw new IOException("Quorum not met to decode: " + availableCount + " available < " + dataShards + " required");
        }

        ReedSolomon reedSolomon = ReedSolomon.create(dataShards, parityShards);
        long bytesRemaining = objectSize;

        try {
            while (bytesRemaining > 0) {
                int currentBlockBytes = (int) Math.min(blockSize, bytesRemaining);
                int shardSize = getBytesInShard(dataShards, currentBlockBytes);

                byte[][] shards = new byte[totalShards][shardSize];
                for (int i = 0; i < totalShards; i++) {
                    if (shardPresent[i]) {
                        readExact(streams[i], shards[i], shardSize);
                    }
                }

                reedSolomon.decodeMissing(shards, shardPresent, 0, shardSize);

                long blockBytesWritten = 0;
                for (int i = 0; i < dataShards; i++) {
                    int bytesToWrite = (int) Math.min(shardSize, currentBlockBytes - blockBytesWritten);
                    if (bytesToWrite <= 0) break;
                    output.write(shards[i], 0, bytesToWrite);
                    blockBytesWritten += bytesToWrite;
                }

                bytesRemaining -= currentBlockBytes;
            }
        } finally {
            for (InputStream is : streams) {
                if (is != null) {
                    try { is.close(); } catch (IOException ignored) {}
                }
            }
        }
    }

    private static int readFullBlock(InputStream input, byte[] buffer) throws IOException {
        int total = 0;
        while (total < buffer.length) {
            int read = input.read(buffer, total, buffer.length - total);
            if (read == -1) break;
            total += read;
        }
        return total;
    }

    private static void readExact(InputStream stream, byte[] buffer, int length) throws IOException {
        int total = 0;
        while (total < length) {
            int read = stream.read(buffer, total, length - total);
            if (read == -1) {
                Arrays.fill(buffer, total, length, (byte) 0);
                break;
            }
            total += read;
        }
    }
}
