# deltalite → Delta Lake

The bridge from this model to real source. Every model class belongs in this table.

| deltalite | Delta Lake | upstream source |
|---|---|---|
| `deltalite.DeltaLog` | `org.apache.spark.sql.delta.DeltaLog` | `core/src/main/scala/org/apache/spark/sql/delta/DeltaLog.scala` |
| `deltalite.Snapshot` | `org.apache.spark.sql.delta.Snapshot` | `core/src/main/scala/org/apache/spark/sql/delta/Snapshot.scala` |
| `deltalite.OptimisticTransaction` | `org.apache.spark.sql.delta.OptimisticTransaction` | `core/src/main/scala/org/apache/spark/sql/delta/OptimisticTransaction.scala` |
| `deltalite.actions.*` | `org.apache.spark.sql.delta.actions.*` | `core/src/main/scala/org/apache/spark/sql/delta/actions/actions.scala` |
| `deltalite.store.Storage` | `org.apache.spark.sql.delta.storage.LogStore` | `core/src/main/scala/org/apache/spark/sql/delta/storage/LogStore.scala` |
| `deltalite.store.ObjectStoreStorage` | `io.delta.storage.S3LogStore` | `storage-s3-dynamodb/src/main/java/io/delta/storage/S3DynamoDBLogStore.java` |

## Deliberate divergences

| What | Why |
|---|---|
| **Async `Storage`** | Real Delta's `LogStore` is synchronous because Spark's execution engine is thread-per-task blocking. `Storage` in Step 4 is asynchronous (`TickCompletableFuture`) because on a network object store, blocking OS threads is an anti-pattern. Demos drive execution via `cluster.tickUntilComplete(...)` (WORKSHOP-PLAN D14). |
| **Local FS: `Files.createLink`** | Delta on local filesystems uses a racy `exists()` pre-check before overwriting. `deltalite` Steps 2–3 use `Files.createLink` (atomic, fails with `EEXIST`), giving true mutual exclusion (WORKSHOP-PLAN D13). |
| **Object Store: `If-None-Match: *`** | AWS S3 historically lacked conditional PUTs, forcing Delta on S3 to rely on DynamoDB (`S3DynamoDBLogStore`) for mutual exclusion. MinIO and modern S3 support `If-None-Match: *`, allowing Delta to commit directly to object storage with native put-if-absent semantics. |
| **Sequential Replay Without `Futures.allOf`** | Log replay operates sequentially across versions 0..N using tickloom continuations (`whenComplete`) and in-memory replay over loaded bytes, ensuring strict version ordering without custom future combinators. |

## Going deeper

- Upstream Delta Lake: [github.com/delta-io/delta](https://github.com/delta-io/delta)
- Substrate: [tickloom](https://github.com/unmeshjoshi/tickloom) — the tick loop everything runs on

---

## Storage Guarantees Across the 4 Steps

Delta Lake requires two fundamental guarantees from its underlying storage engine:
1. **Atomic Publish**: A commit or data file is either completely visible or completely invisible to readers. Readers never see a torn, truncated, or partially written file.
2. **Put-If-Absent (Mutual Exclusion)**: Exactly one writer can successfully claim version $N$ in the log. If two writers attempt to commit version $N$ concurrently, one must succeed and the other must fail (e.g. with `ConcurrentModificationException` / `412 Precondition Failed`).

Here is how each step in `deltalite` realizes (or deliberately lacks) these guarantees:

| Step | Environment | Atomic Publish | Put-If-Absent | Mechanism |
|---|---|---|---|---|
| **Step 1: Plain Files** | Local Disk | ❌ None | ❌ None | Direct writes overwrite files in-place; torn reads and lost updates occur. |
| **Step 2: Commit Log** | Local Disk | ✅ Atomic Publish | ✅ Put-If-Absent | Temp file written, published via `Files.createLink(version, temp)` (`EEXIST` on collision). |
| **Step 3: Snapshot & OCC** | Local Disk | ✅ Atomic Publish | ✅ Put-If-Absent | `Files.createLink` + in-memory log replay for snapshot isolation. |
| **Step 4: Object Store** | Distributed Network | ✅ Atomic Publish | ✅ Put-If-Absent | Temp-and-rename (`AtomicFiles`) inside storage nodes; coordinator checks conditional PUT (`If-None-Match: *`). |

### The Local Filesystem Nuance (Steps 2 & 3 vs Real Delta)

In real Delta (`HDFSLogStore.writeInternal`), Delta relies on Hadoop's `FileContext.rename(temp, target, Options.Rename.NONE)` which is atomic and throws on HDFS. However, Java's `java.nio.file.Files` has no such primitive:
- `CREATE_NEW` claims the name but allows concurrent readers to see the file being written (torn read).
- `Files.move(..., ATOMIC_MOVE)` publishes atomically, but POSIX `rename(2)` overwrites existing files unconditionally, silently destroying the previous commit.

Real Delta works around this on local disks with a racy check:
```scala
if (!overwrite && fc.util.exists(path)) {
  // This is needed for the tests to throw error with local file system
  throw DeltaErrors.fileAlreadyExists(path.toString)
}
```
`deltalite` uses `Files.createLink`: hard-linking a completed temp file into place is atomic and fails immediately with `EEXIST` if the target already exists.

### The Object Store Nuance (Step 4: S3 vs MinIO)

In an object store, objects are addressed by keys, not POSIX inodes:
1. **Atomic Shard Persistence**: On disk inside each storage node, shards and metadata are written to unique temp files and renamed into place via `AtomicFiles.writeBytesAtomic` (`moveAtomic`).
2. **Conditional Commit (`If-None-Match: *`)**: S3 commits do not rename files from a temporary bucket into a log directory (which would require a non-atomic copy + delete). Instead, Delta writes the commit file directly to `_delta_log/<version>.json` via an HTTP PUT with header `If-None-Match: *`.
3. **The TOCTOU Race & Distributed Locking**:
   - In production MinIO (`cmd/dsync`), conditional PUTs require acquiring a distributed quorum lock across the erasure set nodes before checking existence and committing shards.
   - On AWS S3, before S3 supported conditional writes in August 2024, Delta Lake required an external coordinator like DynamoDB (`S3DynamoDBLogStore`) to provide mutual exclusion.
   - In `objectstorelite`, `StorageNode` executes a coordinator check-then-write sequence documented with an educational architecture note on distributed locking and TOCTOU races.

## Storage guarantees

> Full cross-system comparison — HDFS, MinIO, S3, Delta and here — is in
> [docs/STORAGE-GUARANTEES.md](../docs/STORAGE-GUARANTEES.md).
