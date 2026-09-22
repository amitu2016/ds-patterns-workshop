package objectstorelite.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Tracks object versions in xl.meta similar to MinIO.
 */
public final class ObjectMeta {
    private final List<VersionEntry> versions;

    public ObjectMeta() {
        this.versions = new ArrayList<>();
    }

    public ObjectMeta(List<VersionEntry> versions) {
        this.versions = new ArrayList<>(versions);
    }

    public static ObjectMeta empty() {
        return new ObjectMeta();
    }

    public List<VersionEntry> versions() {
        return Collections.unmodifiableList(versions);
    }

    public void add(VersionEntry entry) {
        versions.add(entry);
    }

    public Optional<VersionEntry> latestVersion() {
        return versions.stream()
                .max(Comparator.comparingLong(VersionEntry::modTimeMillis));
    }

    public Optional<VersionEntry> findVersion(String versionId) {
        return versions.stream()
                .filter(v -> v.versionId().equals(versionId))
                .findFirst();
    }

    public static ObjectMeta read(Path path) throws IOException {
        if (!Files.exists(path)) {
            return empty();
        }
        String raw = Files.readString(path);
        String[] blocks = raw.split("\\n\\n");
        List<VersionEntry> entries = new ArrayList<>();
        for (String block : blocks) {
            String trimmed = block.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            entries.add(VersionEntry.parse(trimmed));
        }
        return new ObjectMeta(entries);
    }

    public void write(Path path) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < versions.size(); i++) {
            builder.append(versions.get(i).serialize());
            if (i < versions.size() - 1) {
                builder.append("\n\n");
            }
        }
        // xl.meta is rewritten in place on every version added, and it is the source of
        // truth for which versions exist and which shards are valid. A direct write leaves it
        // truncated if the process dies mid-write, and a truncated xl.meta makes the object
        // unreadable even though every shard is intact on disk.
        //
        // AtomicFiles writes to a temp file and renames it over the old one. REPLACE_EXISTING is
        // correct here — unlike a Delta log entry, this file is *meant* to be replaced; what must not
        // happen is a reader seeing it half-written. MinIO does the same for the same reason.
        objectstorelite.storage.AtomicFiles.writeStringAtomic(path, builder.toString());
    }
}
