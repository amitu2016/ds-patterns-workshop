package deltalite;

import deltalite.actions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a snapshot of the Delta table at a specific version over object storage.
 * Contains the state of the table, including the active files and metadata.
 */
public class Snapshot {

    private final DeltaLog deltaLog;
    private final long version;
    private final List<Action> actions;
    private Metadata metadata;
    private Protocol protocol;
    private final Map<String, AddFile> activeFiles = new HashMap<>();

    public Snapshot(DeltaLog deltaLog, long version, List<Action> actions) {
        this.deltaLog = deltaLog;
        this.version = version;
        this.actions = new ArrayList<>(actions != null ? actions : List.of());
        initializeState();
    }

    private void initializeState() {
        for (Action action : actions) {
            if (action instanceof AddFile addFile) {
                activeFiles.put(addFile.getPath(), addFile);
            } else if (action instanceof RemoveFile removeFile) {
                activeFiles.remove(removeFile.getPath());
            } else if (action instanceof Metadata m) {
                this.metadata = m;
            } else if (action instanceof Protocol p) {
                this.protocol = p;
            }
        }
    }

    public DeltaLog getDeltaLog() {
        return deltaLog;
    }

    public long getVersion() {
        return version;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Protocol getProtocol() {
        return protocol;
    }

    public List<Action> getActions() {
        return actions;
    }

    public List<AddFile> getAllFiles() {
        return new ArrayList<>(activeFiles.values());
    }

    public Map<String, AddFile> getActiveFiles() {
        return new HashMap<>(activeFiles);
    }
}
