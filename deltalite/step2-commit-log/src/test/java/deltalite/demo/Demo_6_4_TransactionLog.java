package deltalite.demo;

import deltalite.DeltaTable;
import deltalite.actions.Action;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SLIDE: Transaction Log Solution   (demo 6.4)
 * SLIDE: Action Types - The Building Blocks
 *
 * <p>The <b>with</b> half of the pair opened by demo 6.3. Same transfer, same object store,
 * same two data files — one thing added: an ordered log of what changed.
 *
 * <p>The shift is what "the table" means. In step 1 it meant <i>the files in the directory</i>,
 * so a reader saw a file the instant it was written. Here it means <i>the files named by
 * committed actions</i>, so data can sit on disk fully written and still not be part of the
 * table. Visibility stops being a property of the filesystem and becomes a property of the log.
 *
 * <p>TRY IT: set {@code COMMIT_THE_TRANSFER} to false. The data files are still written and the
 * bytes are still on disk — and the table stays at the opening balances forever, with the log
 * holding only the opening commit. Uncommitted data is not "pending"; it is invisible.
 */
class Demo_6_4_TransactionLog {

    @TempDir
    Path tempDir;

    /** TRY IT: set to false — the data files are written and the table never changes. */
    static final boolean COMMIT_THE_TRANSFER = true;

    private static final int OPENING_BALANCE = 100;
    private static final int TRANSFER = 40;

    @Test
    @DisplayName("6.4 · the log makes a two-file transfer atomic")
    void aTransferIsInvisibleUntilItIsCommitted() throws IOException {
        Path tablePath = tempDir.resolve("accounts");
        DeltaTable accounts = new DeltaTable(tablePath.toString());

        // ---- v0: opening balances
        System.out.println("\n--- 1. Opening balances, committed ---");
        List<Action> opening = new ArrayList<>(accounts.insert(List.of(
                Map.of("account", "alice", "delta", String.valueOf(OPENING_BALANCE)),
                Map.of("account", "bob", "delta", String.valueOf(OPENING_BALANCE)))));
        long openingVersion = accounts.commit(opening);
        System.out.println("    (version 0 was the table's Protocol + Metadata; this is version "
                + openingVersion + ")");
        printState(accounts, "version " + openingVersion);
        assertEquals(2 * OPENING_BALANCE, totalMoney(accounts));

        // ---- the transfer: both files written, neither committed
        System.out.println("\n--- 2. alice transfers " + TRANSFER + " to bob ---");
        List<Action> transfer = new ArrayList<>();
        transfer.addAll(accounts.insert(List.of(Map.of("account", "alice", "delta", String.valueOf(-TRANSFER)))));
        System.out.println("    wrote the debit file");
        transfer.addAll(accounts.insert(List.of(Map.of("account", "bob", "delta", String.valueOf(TRANSFER)))));
        System.out.println("    wrote the credit file");
        System.out.println("    " + parquetFileCount(tablePath) + " data files now exist on disk — none of them committed");

        // ---- the same reader, at the same moment that broke step 1
        System.out.println("\n--- 3. A reader lists the table right now ---");
        printState(accounts, "version " + accounts.currentVersion());
        assertEquals(2 * OPENING_BALANCE, totalMoney(accounts),
                "the debit is on disk but not in the log, so the table has not changed");
        System.out.println("    total money in the bank: " + totalMoney(accounts) + "   ← intact");

        // ---- one atomic commit publishes both files together
        long version = openingVersion;
        if (COMMIT_THE_TRANSFER) {
            System.out.println("\n--- 4. Commit both files in one log entry ---");
            version = accounts.commit(transfer);
            printState(accounts, "version " + version);
            assertEquals(2 * OPENING_BALANCE, totalMoney(accounts), "and still intact afterwards");
        } else {
            System.out.println("\n--- 4. NOT committing — the files stay on disk and stay invisible ---");
            printState(accounts, "still version " + openingVersion);
            assertEquals(2 * OPENING_BALANCE, totalMoney(accounts),
                    "uncommitted data changes nothing, because the log defines the table");
        }

        // ---- the log itself
        System.out.println("\n--- 5. What the log holds ---");
        Path logPath = tablePath.resolve("_delta_log");
        try (var files = Files.list(logPath)) {
            files.filter(f -> f.toString().endsWith(".json")).sorted().forEach(f -> {
                System.out.println("      " + f.getFileName());
                try {
                    for (String line : Files.readAllLines(f)) {
                        if (!line.isBlank()) {
                            System.out.println("          " + abbreviate(line));
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // ---- and because commits are immutable, the old version is still readable
        System.out.println("\n--- 6. The pre-transfer version is still readable ---");
        Map<String, Integer> before = balancesAt(accounts, openingVersion);
        System.out.println("    at version " + openingVersion + ": " + before + "   (before the transfer)");
        assertEquals(OPENING_BALANCE, before.get("alice"),
                "commits are immutable, so an earlier version reconstructs unchanged");
        if (COMMIT_THE_TRANSFER) {
            assertTrue(version > openingVersion,
                    "the transfer created a new version rather than mutating the old one");
        } else {
            assertEquals(openingVersion, version,
                    "with no commit there is no new version — the written files belong to nothing");
        }

        System.out.println("""

                ── what succeeded ──
                The debit file sat on disk, fully written, and the table did not change.
                That is the whole idea: the log decides what is visible, not the directory.

                Both files became visible in a single log write, so the window demo 6.3
                exploited — a reader between two writes — no longer exists. There is no
                "between": there is the state before commit N, and the state after it.

                Note what was NOT needed. No lock, no coordinator, no two-phase protocol.
                One ordered, append-only log over immutable files is enough to make a
                multi-file operation atomic.

                Still missing: two writers committing at the same time both believe they
                are creating the next version. That is demo 6.5.
                """);
    }

    private static int totalMoney(DeltaTable table) throws IOException {
        return table.readAll().stream().mapToInt(r -> Integer.parseInt(r.get("delta"))).sum();
    }

    private static void printState(DeltaTable table, String label) throws IOException {
        Map<String, Integer> balances = new java.util.TreeMap<>();
        for (Map<String, String> row : table.readAll()) {
            balances.merge(row.get("account"), Integer.parseInt(row.get("delta")), Integer::sum);
        }
        System.out.print("    " + label + ": ");
        balances.forEach((a, b) -> System.out.print(a + "=" + b + "  "));
        System.out.println();
    }

    private static Map<String, Integer> balancesAt(DeltaTable table, long version) throws IOException {
        Map<String, Integer> balances = new java.util.TreeMap<>();
        for (Map<String, String> row : table.readAtVersion(version)) {
            balances.merge(row.get("account"), Integer.parseInt(row.get("delta")), Integer::sum);
        }
        return balances;
    }

    private static int parquetFileCount(Path tablePath) throws IOException {
        try (var files = Files.list(tablePath.resolve("data"))) {
            return (int) files.filter(f -> f.toString().endsWith(".parquet")).count();
        }
    }

    private static String abbreviate(String json) {
        return json.length() <= 96 ? json : json.substring(0, 93) + "...";
    }
}
