package objectstorelite.codec;

import objectstorelite.model.ShardFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ErasureCodecTest {

    @TempDir
    Path tempDir;

    @Test
    void testPaddingCalculation() {
        assertEquals(0, ErasureCodec.paddingBytes(1024, 4));
        assertEquals(3, ErasureCodec.paddingBytes(1001, 4));
        assertEquals(256, ErasureCodec.getBytesInShard(4, 1024));
        assertEquals(251, ErasureCodec.getBytesInShard(4, 1001));
    }

    @Test
    void testEncodeAndDecodeWithoutLoss() throws IOException {
        byte[] payload = "Hello World! Distributed Systems Design Patterns.".getBytes(StandardCharsets.UTF_8);
        int dataShards = 4;
        int parityShards = 2;
        int totalShards = dataShards + parityShards;

        byte[][] shards = ErasureCodec.encode(payload, dataShards, parityShards);
        assertEquals(totalShards, shards.length);

        boolean[] shardPresent = new boolean[totalShards];
        Arrays.fill(shardPresent, true);

        byte[] decoded = ErasureCodec.decode(shards, shardPresent, payload.length, dataShards, parityShards);
        assertArrayEquals(payload, decoded);
    }

    @Test
    void testDecodeRecoversMissingDataShards() throws IOException {
        byte[] payload = "Testing reconstruction of missing data shards with Reed-Solomon RS(4,2)"
                .repeat(10).getBytes(StandardCharsets.UTF_8);
        int dataShards = 4;
        int parityShards = 2;
        int totalShards = dataShards + parityShards;

        byte[][] shards = ErasureCodec.encode(payload, dataShards, parityShards);

        boolean[] shardPresent = new boolean[totalShards];
        Arrays.fill(shardPresent, true);

        // Delete data shard 0 and data shard 2
        shardPresent[0] = false;
        shardPresent[2] = false;
        shards[0] = null;
        shards[2] = null;

        byte[] decoded = ErasureCodec.decode(shards, shardPresent, payload.length, dataShards, parityShards);
        assertArrayEquals(payload, decoded);
    }

    @Test
    void testDecodeRecoversMissingParityShards() throws IOException {
        byte[] payload = "Testing reconstruction with missing parity shards".getBytes(StandardCharsets.UTF_8);
        int dataShards = 4;
        int parityShards = 2;
        int totalShards = dataShards + parityShards;

        byte[][] shards = ErasureCodec.encode(payload, dataShards, parityShards);

        boolean[] shardPresent = new boolean[totalShards];
        Arrays.fill(shardPresent, true);

        // Delete parity shard 4 and parity shard 5
        shardPresent[4] = false;
        shardPresent[5] = false;
        shards[4] = null;
        shards[5] = null;

        byte[] decoded = ErasureCodec.decode(shards, shardPresent, payload.length, dataShards, parityShards);
        assertArrayEquals(payload, decoded);
    }

    @Test
    void testDecodeFailsWhenLossExceedsParity() {
        byte[] payload = "Payload destined to fail quorum".getBytes(StandardCharsets.UTF_8);
        int dataShards = 4;
        int parityShards = 2;
        int totalShards = dataShards + parityShards;

        byte[][] shards = ErasureCodec.encode(payload, dataShards, parityShards);

        boolean[] shardPresent = new boolean[totalShards];
        Arrays.fill(shardPresent, true);

        // Delete 3 shards (exceeds parityShards=2)
        shardPresent[0] = false;
        shardPresent[1] = false;
        shardPresent[2] = false;
        shards[0] = null;
        shards[1] = null;
        shards[2] = null;

        IOException ex = assertThrows(IOException.class, () ->
                ErasureCodec.decode(shards, shardPresent, payload.length, dataShards, parityShards));
        assertTrue(ex.getMessage().contains("Quorum not met"));
    }

    @Test
    void testStreamEncodeAndDecodeWithLoss() throws IOException {
        byte[] original = "Large streaming block data testing disk sharding with RS(4,2)".repeat(500)
                .getBytes(StandardCharsets.UTF_8);

        Path sourceFile = tempDir.resolve("stream_source.bin");
        Files.write(sourceFile, original);

        Path shardDir = tempDir.resolve("shards");
        List<ShardFile> shardFiles;
        try (InputStream in = Files.newInputStream(sourceFile)) {
            shardFiles = ErasureCodec.encodeStreamToFiles(in, shardDir, 4, 2, 4096);
        }

        assertEquals(6, shardFiles.size());

        List<Path> shardPaths = new ArrayList<>();
        for (ShardFile sf : shardFiles) {
            shardPaths.add(sf.path());
        }

        // Delete shard 1 and shard 3
        Files.delete(shardPaths.get(1));
        Files.delete(shardPaths.get(3));

        Path outputFile = tempDir.resolve("stream_reconstructed.bin");
        try (OutputStream out = Files.newOutputStream(outputFile)) {
            ErasureCodec.decodeFilesToStream(shardPaths, out, original.length, 4, 2, 4096);
        }

        byte[] reconstructed = Files.readAllBytes(outputFile);
        assertArrayEquals(original, reconstructed);
    }
}
