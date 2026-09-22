package objectstorelite.storage;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;

/**
 * Provides atomic file publishing through a write-to-temp-then-rename pipeline,
 * mirroring MinIO's {@code xl-storage.go} storage layer.
 *
 * <h2>The Storage Guarantee</h2>
 * <p>This utility guarantees <b>Atomic Publish</b>:
 * <ul>
 *   <li>Data is written to a unique, transient temporary file in the target's parent directory.</li>
 *   <li>Buffers are flushed before the file is published.</li>
 *   <li>The temporary file is atomically renamed into place using POSIX {@code rename(2)}
 *       ({@link StandardCopyOption#ATOMIC_MOVE}).</li>
 *   <li>Concurrent readers will observe either the complete, previous file or the complete,
 *       newly published file — never a truncated, torn, or partially written artifact.</li>
 * </ul>
 *
 * <h2>What This Does NOT Guarantee</h2>
 * <p>This utility does <b>NOT guarantee put-if-absent (mutual exclusion)</b>:
 * <ul>
 *   <li>POSIX {@code rename(2)} (and {@link StandardCopyOption#REPLACE_EXISTING}) overwrites
 *       an existing target file unconditionally.</li>
 *   <li>If two concurrent writers write to the same target path, the second write silently
 *       replaces the first without throwing an exception.</li>
 *   <li>Where mutual exclusion is required without overwrites:
 *       <ul>
 *         <li>On local filesystems, use {@link Files#createLink(Path, Path)}, which fails with {@code EEXIST}
 *             (see {@code deltalite} Steps 2 and 3).</li>
 *         <li>In distributed object stores, use conditional writes ({@code If-None-Match: *}) guarded
 *             by distributed mutual exclusion (such as MinIO's {@code dsync} or AWS DynamoDB locks).</li>
 *       </ul>
 *   </li>
 * </ul>
 */
public final class AtomicFiles {

    private AtomicFiles() {}

    /**
     * Writes byte data to a target path atomically by writing to a temporary file first
     * and renaming it into place.
     *
     * @param target the target destination path
     * @param data   the byte data to write
     * @throws IOException if an I/O error occurs
     */
    public static void writeBytesAtomic(Path target, byte[] data) throws IOException {
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        Path temp = target.resolveSibling(target.getFileName() + ".tmp." + UUID.randomUUID());
        try {
            Files.write(temp, data != null ? data : new byte[0]);
            moveAtomic(temp, target);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /**
     * Writes string content to a target path atomically by writing to a temporary file first
     * and renaming it into place.
     *
     * @param target  the target destination path
     * @param content the string content to write
     * @throws IOException if an I/O error occurs
     */
    public static void writeStringAtomic(Path target, String content) throws IOException {
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        Path temp = target.resolveSibling(target.getFileName() + ".tmp." + UUID.randomUUID());
        try {
            Files.writeString(temp, content != null ? content : "");
            moveAtomic(temp, target);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /**
     * Renames source to target atomically using ATOMIC_MOVE, falling back to non-atomic move
     * on filesystems that do not support atomic moves.
     */
    public static void moveAtomic(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            // Best effort on filesystems without atomic rename (e.g. some network mounts);
            // note that the torn-read window reopens if the fallback is triggered.
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
