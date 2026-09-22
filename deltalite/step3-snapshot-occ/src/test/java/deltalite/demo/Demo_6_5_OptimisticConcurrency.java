package deltalite.demo;

import deltalite.DeltaTable;
import deltalite.OptimisticTransaction;
import deltalite.util.FileNames;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLIDE: Optimistic Transaction Flow   (demo 6.5)
 * SLIDE: The Concurrent Writers Challenge
 * SLIDE: Conflict Detection and Resolution
 *
 * <p>Step 2 made a multi-file write atomic for one writer. Two writers is a different problem:
 * both read version N, both prepare independently, and both believe they are creating N+1.
 *
 * <p>The pair below separates the two things that make this safe, because they are easy to
 * conflate:
 * <ul>
 *   <li><b>without</b> — the storage layer allows overwriting a version file. The second
 *       writer's commit replaces the first, whose data is still on disk and now unreachable.
 *       No error is raised anywhere.</li>
 *   <li><b>with</b> — claiming a version is a put-if-absent. The loser is told, and can retry
 *       against the version it did not know about.</li>
 * </ul>
 *
 * <p>Conflict <i>detection</i> alone is not enough: two writers can both pass the check and
 * then both write. It is the atomic claim that makes the protocol correct — the check just
 * turns a lost write into a fast, polite failure.
 *
 * <p>TRY IT: set {@code A_RETRIES} to false — the rejection still happens, but nobody acts on it,
 * so A's work is simply lost. Optimistic concurrency does not make conflicts go away; it makes them
 * visible and cheap to handle, and someone still has to do the handling.
 */
class Demo_6_5_OptimisticConcurrency {

    /** TRY IT: set to false — A never retries, and only B's row survives. */
    static final boolean A_RETRIES = true;

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("6.5 (without) · overwritable versions lose a commit silently")
    void withoutAtomicCreate_theFirstCommitIsSilentlyLost() throws IOException {
        Path tablePath = tempDir.resolve("orders-unsafe");
        DeltaTable table = new DeltaTable(tablePath.toString());
        Path logPath = tablePath.resolve("_delta_log");

        System.out.println("\n--- 1. Writer A and writer B both start from the same version ---");
        OptimisticTransaction a = table.startTransaction();
        a.insert(List.of(Map.of("order", "A-1", "amount", "100")));
        OptimisticTransaction b = table.startTransaction();
        b.insert(List.of(Map.of("order", "B-1", "amount", "200")));
        System.out.println("    A read version " + a.getReadVersion() + ", B read version " + b.getReadVersion());
        System.out.println("    both will try to create version " + (a.getReadVersion() + 1));

        System.out.println("\n--- 2. A commits first ---");
        a.commit("A writes");
        long contested = a.getReadVersion() + 1;
        Path versionFile = logPath.resolve(FileNames.deltaFile(contested));
        System.out.println("    " + versionFile.getFileName() + " now names " + countAdds(versionFile) + " data file(s)");

        System.out.println("\n--- 3. B commits to the same version, with no atomic claim ---");
        // Exactly what a plain, truncating write does — the behaviour before CREATE_NEW.
        Files.writeString(versionFile, Files.readString(versionFile).lines()
                .filter(l -> !l.contains("\"add\""))
                .reduce("", (x, y) -> x + y + "\n") + bFakeAddLine());
        System.out.println("    " + versionFile.getFileName() + " now names " + countAdds(versionFile) + " data file(s)");

        System.out.println("\n--- 4. Where did A's order go? ---");
        List<Map<String, String>> visible = table.readAll();
        System.out.println("    rows visible in the table: " + visible);
        boolean aVisible = visible.stream().anyMatch(r -> "A-1".equals(r.get("order")));
        System.out.println("    A's data file is still on disk: " + dataFileCount(tablePath) + " files exist");
        assertTrue(!aVisible, "A's commit was overwritten and its rows are unreachable");

        System.out.println("""

                ── what failed ──
                Nothing threw. A's commit returned successfully, A's Parquet file is still on
                disk, and A's rows are gone from the table — because the log entry that named
                them was replaced.

                A lost update with no error is the worst kind: every writer believes it
                succeeded, and the damage is only visible to whoever reads later.
                """);
    }

    @Test
    @DisplayName("6.5 (with) · the second writer is rejected and retries")
    void withOptimisticConcurrency_theSecondWriterIsRejectedAndRetries() throws IOException {
        Path tablePath = tempDir.resolve("orders-safe");
        DeltaTable table = new DeltaTable(tablePath.toString());

        System.out.println("\n--- 1. Writer A and writer B both start from the same version ---");
        OptimisticTransaction a = table.startTransaction();
        a.insert(List.of(Map.of("order", "A-1", "amount", "100")));
        OptimisticTransaction b = table.startTransaction();
        b.insert(List.of(Map.of("order", "B-1", "amount", "200")));
        System.out.println("    A read version " + a.getReadVersion() + ", B read version " + b.getReadVersion());

        System.out.println("\n--- 2. B commits first and wins ---");
        b.commit("B writes");
        System.out.println("    B created version " + (b.getReadVersion() + 1));

        System.out.println("\n--- 3. A tries to commit against the version it read ---");
        ConcurrentModificationException conflict =
                assertThrows(ConcurrentModificationException.class, () -> a.commit("A writes"));
        System.out.println("    ❌ rejected: " + conflict.getMessage().replace("\"", "").trim());

        if (A_RETRIES) {
            System.out.println("\n--- 4. A retries: re-read, re-apply, commit again ---");
            OptimisticTransaction retry = table.startTransaction();
            System.out.println("    A now reads version " + retry.getReadVersion() + " (B's commit is visible)");
            retry.insert(List.of(Map.of("order", "A-1", "amount", "100")));
            retry.commit("A writes (retry)");
            System.out.println("    A created version " + (retry.getReadVersion() + 1));
        } else {
            System.out.println("\n--- 4. A does not retry — it just gives up ---");
        }

        System.out.println("\n--- 5. What is visible now ---");
        List<Map<String, String>> visible = table.readAll();
        visible.stream().map(r -> r.get("order")).sorted().forEach(o -> System.out.println("      " + o));
        assertEquals(A_RETRIES ? 2 : 1, visible.size(),
                A_RETRIES ? "no writer was lost" : "A never retried, so A's row is not in the table");
        assertEquals(A_RETRIES, visible.stream().anyMatch(r -> "A-1".equals(r.get("order"))));
        assertTrue(visible.stream().anyMatch(r -> "B-1".equals(r.get("order"))), "B committed and stays");

        System.out.println("""

                ── what succeeded ──
                Neither writer took a lock, and neither waited for the other. Both did their
                work in full, and only at the moment of publishing did one discover it had
                been overtaken.

                That is the optimistic bargain: conflicts cost a retry, and the common case —
                writers touching different data — costs nothing at all. It pays off exactly
                when conflicts are rare, which on a data lake they usually are.

                The rejection came from two things working together: a version check that
                noticed the table had moved, and an atomic claim on the version file that
                would have stopped A even if the check had passed.
                """);
    }

    private static String bFakeAddLine() {
        return "{\"type\":\"add\",\"path\":\"data/b-overwrote-a.parquet\",\"partitionValues\":{},"
                + "\"size\":0,\"modificationTime\":0,\"dataChange\":true,\"stats\":{},\"tags\":\"\"}";
    }

    private static long countAdds(Path versionFile) throws IOException {
        return Files.readAllLines(versionFile).stream().filter(l -> l.contains("\"add\"")).count();
    }

    private static int dataFileCount(Path tablePath) throws IOException {
        try (var f = Files.list(tablePath.resolve("data"))) {
            return (int) f.filter(x -> x.toString().endsWith(".parquet")).count();
        }
    }
}
