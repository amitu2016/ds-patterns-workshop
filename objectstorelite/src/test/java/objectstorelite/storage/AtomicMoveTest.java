package objectstorelite.storage;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Executable proof of the filesystem semantics the commit protocol depends on.
 *
 * <p>These tests exist because the obvious reading of the API is wrong, and the wrong reading
 * costs you a silently lost commit. See {@code docs/STORAGE-GUARANTEES.md}.
 *
 * <p>Two guarantees are needed to claim a version of a Delta log, and they are easy to conflate:
 * <ul>
 *   <li><b>atomic publish</b> — a reader never sees a half-written file</li>
 *   <li><b>put-if-absent</b> — a second writer cannot take a name that is already taken</li>
 * </ul>
 *
 * <p>The surprise is that {@code ATOMIC_MOVE} gives the first and <b>removes</b> the second.
 * OpenJDK's {@code UnixFileSystem.move()} (in {@code src/java.base/unix/classes}, so this is
 * Linux as well as macOS) delegates straight to POSIX {@code rename(2)} when {@code atomicMove}
 * is set, skipping the exists check its own non-atomic path performs:
 *
 * <pre>
 *   if (flags.atomicMove) {
 *       try {
 *           rename(source, target);          // no exists check, no REPLACE_EXISTING check
 *
 *   // ...versus the non-atomic path in the same method:
 *   if (targetExists) {
 *       if (!flags.replaceExisting)
 *           throw new FileAlreadyExistsException(...);
 * </pre>
 *
 * <p>If a future JDK or platform changes any of this, these tests fail and say so — which is the
 * point of pinning it rather than describing it.
 */
class AtomicMoveTest {

    @Test
    @DisplayName("ATOMIC_MOVE silently overwrites an existing target — it does NOT throw")
    void atomicMoveOverwritesSilently(@TempDir Path tempDir) throws IOException {
        Path source = tempDir.resolve("source.txt");
        Path target = tempDir.resolve("target.txt");
        Files.writeString(source, "new commit");
        Files.writeString(target, "the commit already there");

        // No exception. This is the whole trap.
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);

        assertEquals("new commit", Files.readString(target),
                "the existing file was replaced, and nothing told us");
        assertFalse(Files.exists(source), "the source was consumed by the move");
    }

    @Test
    @DisplayName("plain Files.move DOES throw — asking for ATOMIC_MOVE removes this check")
    void plainMoveRefusesAnExistingTarget(@TempDir Path tempDir) throws IOException {
        Path source = tempDir.resolve("source.txt");
        Path target = tempDir.resolve("target.txt");
        Files.writeString(source, "new commit");
        Files.writeString(target, "the commit already there");

        assertThrows(FileAlreadyExistsException.class, () -> Files.move(source, target));

        assertEquals("the commit already there", Files.readString(target), "target untouched");
        assertTrue(Files.exists(source), "source untouched");
    }

    @Test
    @DisplayName("createLink gives both guarantees: atomic, and fails when the name is taken")
    void createLinkClaimsANameOrFails(@TempDir Path tempDir) throws IOException {
        Path temp = tempDir.resolve("commit.tmp");
        Path version = tempDir.resolve("00000000000000000000.json");
        Files.writeString(temp, "actions for version 0");

        // First writer claims the version.
        Files.createLink(version, temp);
        assertEquals("actions for version 0", Files.readString(version));

        // Second writer, having prepared its own commit, tries to claim the same version.
        Path otherTemp = tempDir.resolve("other.tmp");
        Files.writeString(otherTemp, "a different version 0");

        assertThrows(FileAlreadyExistsException.class, () -> Files.createLink(version, otherTemp),
                "link() must refuse a name that is already taken — this is the put-if-absent "
                        + "that ATOMIC_MOVE does not provide");

        assertEquals("actions for version 0", Files.readString(version),
                "the first writer's commit survives the second writer's attempt");
    }

    @Test
    @DisplayName("the published file is complete — a link never exposes a partial write")
    void linkPublishesTheCompleteFile(@TempDir Path tempDir) throws IOException {
        Path temp = tempDir.resolve("commit.tmp");
        Path version = tempDir.resolve("00000000000000000001.json");

        // Write a payload large enough that it cannot be one buffer flush.
        StringBuilder actions = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            actions.append("{\"type\":\"add\",\"path\":\"data/part-").append(i).append(".parquet\"}\n");
        }
        Files.writeString(temp, actions.toString());

        // Only now does the name exist, and it exists fully formed.
        Files.createLink(version, temp);

        assertEquals(5000, Files.readAllLines(version).size(),
                "every action present, or the name would not exist at all");
    }
}
