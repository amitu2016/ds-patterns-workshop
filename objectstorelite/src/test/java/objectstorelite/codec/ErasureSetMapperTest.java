package objectstorelite.codec;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.*;

class ErasureSetMapperTest {

    @Test
    void testCrcModDeterminism() {
        ErasureSetMapper mapper = new ErasureSetMapper(ErasureSetMapper.Algorithm.CRCMOD, null, 8);
        int idx1 = mapper.indexFor("bucket/key-1.parquet");
        int idx2 = mapper.indexFor("bucket/key-1.parquet");
        assertEquals(idx1, idx2);
        assertTrue(idx1 >= 0 && idx1 < 8);
    }

    @Test
    void testSipModUsesDeploymentId() {
        byte[] depA = HexFormat.of().parseHex("00112233445566778899aabbccddeeff");
        byte[] depB = HexFormat.of().parseHex("ffeeddccbbaa99887766554433221100");
        ErasureSetMapper mapperA = new ErasureSetMapper(ErasureSetMapper.Algorithm.SIPMOD, depA, 8);
        ErasureSetMapper mapperB = new ErasureSetMapper(ErasureSetMapper.Algorithm.SIPMOD, depB, 8);

        String[] keys = {"photos/2025/01/file.jpg", "docs/report.pdf", "notes.txt", "video.mp4"};
        boolean differs = false;
        for (String key : keys) {
            if (mapperA.indexFor(key) != mapperB.indexFor(key)) {
                differs = true;
                break;
            }
        }
        assertTrue(differs, "Different deployment IDs should change placement for at least one key");
    }

    @Test
    void testSipModMatchesWhenDeploymentSame() {
        byte[] dep = HexFormat.of().parseHex("00112233445566778899aabbccddeeff");
        ErasureSetMapper mapperA = new ErasureSetMapper(ErasureSetMapper.Algorithm.SIPMOD, dep, 8);
        ErasureSetMapper mapperB = new ErasureSetMapper(ErasureSetMapper.Algorithm.SIPMOD, dep, 8);

        assertEquals(mapperA.indexFor("foo/bar"), mapperB.indexFor("foo/bar"));
    }

    @Test
    void testZeroOrNegativeSetCount() {
        ErasureSetMapper mapper = new ErasureSetMapper(ErasureSetMapper.Algorithm.CRCMOD, null, 0);
        assertEquals(-1, mapper.indexFor("key"));
    }
}
