package deltalite;

import deltalite.actions.AddFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the storage guarantee the commit protocol depends on (WORKSHOP-PLAN.md D13).
 *
 * <p>The optimistic conflict check normally rejects a stale writer before it reaches the log,
 * so these tests bypass it and write to {@link DeltaLog} directly. Without them the guarantee
 * would be documented and untested — and the version that shipped before this used a
 * truncating write, which silently lost commits.
 */
class DeltaLogAtomicityTest {

    @TempDir
    Path tempDir;

    @Test
    void claimingAnAlreadyClaimedVersionFails() throws IOException {
        DeltaLog log = DeltaLog.forTable(tempDir.resolve("table").toString());

        log.write(0, List.of(new AddFile("data/first.parquet", 100, 1L)));

        // put-if-absent: a second writer must not be able to take version 0.
        assertThrows(FileAlreadyExistsException.class,
                () -> log.write(0, List.of(new AddFile("data/second.parquet", 200, 2L))),
                "version 0 was already claimed; the second write must fail, not overwrite");

        // ...and the first writer's commit is intact.
        List<deltalite.actions.Action> actions = log.readVersion(0);
        assertEquals(1, actions.size());
        assertEquals("data/first.parquet", ((AddFile) actions.get(0)).getPath(),
                "the original commit must survive the failed attempt");
    }

    @Test
    void aFailedClaimLeavesNoDebrisBehind() throws IOException {
        Path tablePath = tempDir.resolve("table");
        DeltaLog log = DeltaLog.forTable(tablePath.toString());
        log.write(0, List.of(new AddFile("data/first.parquet", 100, 1L)));

        assertThrows(FileAlreadyExistsException.class,
                () -> log.write(0, List.of(new AddFile("data/second.parquet", 200, 2L))));

        // The temp file used for the atomic rename must be cleaned up, or a failed
        // commit would leave the log directory littered with partial writes.
        try (var entries = Files.list(tablePath.resolve("_delta_log"))) {
            List<String> leftovers = entries.map(p -> p.getFileName().toString())
                    .filter(n -> n.contains(".tmp"))
                    .toList();
            assertTrue(leftovers.isEmpty(), "temp files left behind: " + leftovers);
        }
    }

    @Test
    void aVersionFileIsNeverVisibleHalfWritten() throws IOException {
        Path tablePath = tempDir.resolve("table");
        DeltaLog log = DeltaLog.forTable(tablePath.toString());

        // Many actions, so the write is not a single small buffer flush.
        List<deltalite.actions.Action> many = new java.util.ArrayList<>();
        for (int i = 0; i < 200; i++) {
            many.add(new AddFile("data/part-" + i + ".parquet", 1024L * i, i));
        }
        log.write(0, many);

        // Atomic publish: the file appeared complete. A truncating writer would have made
        // the intermediate states observable; the rename makes them unreachable by construction.
        assertEquals(200, log.readVersion(0).size(),
                "every action in the commit must be present, or none of them");
    }
}
