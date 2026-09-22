package deltalite.demo;

import deltalite.DeltaTable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SLIDE: The Object Store Problem   (demo 6.3)
 * SLIDE: Assignment 1: Basic File Operations
 *
 * <p>This is the <b>without</b> half of the pair. Its <b>with</b> half is demo 6.4 in
 * {@code step2-commit-log}, where the same operation becomes atomic. Nothing here is broken
 * code — this is what an object store gives you, and it is not enough.
 *
 * <p>A transfer is two writes: debit one account, credit another. Each write lands in its own
 * immutable Parquet file, and each is individually atomic. But there is no way to say
 * <i>"these two files take effect together"</i>, so a reader between them sees money that
 * exists nowhere.
 *
 * <p>TRY IT: set {@code CREDIT_FIRST} to true. The anomaly does not disappear, it inverts — the
 * reader briefly sees money that exists twice instead of money that has vanished. Neither order is
 * safe, because ordering was never the problem.
 */
class Demo_6_3_ObjectStoreProblem {

    @TempDir
    Path tempDir;

    /** TRY IT: set to true — the anomaly inverts, money briefly exists twice. */
    static final boolean CREDIT_FIRST = false;

    private static final int OPENING_BALANCE = 100;
    private static final int TRANSFER = 40;

    @Test
    @DisplayName("6.3 · without a log, a reader sees a transfer half-applied")
    void aReaderObservesATransferHalfApplied() throws IOException {
        DeltaTable accounts = new DeltaTable(tempDir.resolve("accounts").toString());

        System.out.println("\n--- 1. Opening balances ---");
        accounts.insert(List.of(
                Map.of("account", "alice", "delta", String.valueOf(OPENING_BALANCE)),
                Map.of("account", "bob", "delta", String.valueOf(OPENING_BALANCE))));
        printBalances(accounts, "after opening");
        assertEquals(2 * OPENING_BALANCE, totalMoney(accounts));

        // ---- the transfer: two writes, no way to bind them together
        System.out.println("\n--- 2. alice transfers " + TRANSFER + " to bob ---");
        String firstAccount  = CREDIT_FIRST ? "bob"   : "alice";
        int    firstDelta    = CREDIT_FIRST ?  TRANSFER : -TRANSFER;
        String secondAccount = CREDIT_FIRST ? "alice" : "bob";
        int    secondDelta   = CREDIT_FIRST ? -TRANSFER :  TRANSFER;

        System.out.println("    write 1 of 2: " + (CREDIT_FIRST ? "credit bob" : "debit alice"));
        accounts.insert(List.of(Map.of("account", firstAccount, "delta", String.valueOf(firstDelta))));

        // ---- a reader arrives here. It is doing nothing wrong.
        System.out.println("\n--- 3. A reader lists the table right now ---");
        printBalances(accounts, "mid-transfer");
        int observed = totalMoney(accounts);
        int drift = observed - 2 * OPENING_BALANCE;
        System.out.printf("    total money in the bank: %d   ← %d has %s%n",
                observed, Math.abs(drift), drift < 0 ? "vanished" : "appeared from nowhere");

        assertEquals(2 * OPENING_BALANCE + firstDelta, observed,
                "with no transaction boundary, one half of the transfer is visible without the other");

        System.out.println("\n    write 2 of 2: " + (CREDIT_FIRST ? "debit alice" : "credit bob"));
        accounts.insert(List.of(Map.of("account", secondAccount, "delta", String.valueOf(secondDelta))));
        printBalances(accounts, "after the transfer completes");
        assertEquals(2 * OPENING_BALANCE, totalMoney(accounts), "it does balance — eventually");

        // ---- and nothing on disk records that those two writes belonged together
        System.out.println("\n--- 4. What the storage layer actually knows ---");
        try (var files = Files.list(tempDir.resolve("accounts").resolve("data"))) {
            files.filter(f -> f.toString().endsWith(".parquet"))   // skip Hadoop's .crc sidecars
                 .sorted()
                 .forEach(f -> System.out.println("      " + f.getFileName()));
        }
        System.out.println("    " + accounts.dataFileCount() + " files. The names carry a UUID and nothing else:");
        System.out.println("    not when they were written, not in what order, not which belong together.");

        System.out.println("""

                ── what failed ──
                Every individual file write was atomic. The table still went wrong.

                Atomicity per file is not atomicity per operation. A transfer spans two
                files, and an object store has no way to express "both or neither" — so a
                reader that lists the directory at the wrong moment sees a bank that has
                lost money. Nothing retried, nothing crashed; the reader simply looked.

                Listing a directory answers "what files exist now". A table needs to answer
                "what did this table look like at a point in time", and no amount of care
                with file names will get you there.

                Demo 6.4 adds one thing — an ordered log of what changed — and the same
                transfer becomes invisible until it is complete.
                """);
    }

    private static int totalMoney(DeltaTable table) throws IOException {
        return table.readAll().stream().mapToInt(r -> Integer.parseInt(r.get("delta"))).sum();
    }

    private static void printBalances(DeltaTable table, String label) throws IOException {
        Map<String, Integer> balances = new java.util.TreeMap<>();
        for (Map<String, String> row : table.readAll()) {
            balances.merge(row.get("account"), Integer.parseInt(row.get("delta")), Integer::sum);
        }
        System.out.print("    " + label + ": ");
        balances.forEach((account, balance) -> System.out.print(account + "=" + balance + "  "));
        System.out.println();
    }
}
