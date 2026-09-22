# objectstorelite → MinIO

The bridge from this model to real MinIO source. Every model class belongs in this table.

| objectstorelite | MinIO | Upstream Concept |
|---|---|---|
| `ErasureSetMapper` | `cmd/erasure-sets.go` (`hashOrder`, `CRCMOD`, `SIPMOD`) | Deterministic mapping of object key to erasure set index |
| `ErasureSet` | `cmd/erasure-sets.go` (`erasureSets`) | Fixed-size stripe of $N = K + M$ storage disks/nodes |
| `ErasureCodec` | `cmd/erasure-encode.go` / `klauspost/reedsolomon` | Galois Field $GF(2^8)$ Reed-Solomon matrix encoding & reconstruction |
| `ObjectMeta` | `cmd/xl-storage-format-v2.go` (`xlMetaV2`) | Object metadata container storing version histories |
| `VersionEntry` | `cmd/xl-storage-format-v2.go` (`xlMetaV2Version`) | Version entry tracking payload size, shard files, or delete markers |
| `StorageNode` | `cmd/xl-storage.go` (`xlStorage`) + `cmd/erasure-sets.go` | Storage node serving local disk shards and coordinating erasure sets / RS |
| `ObjectStoreClient` | MinIO Client SDK / AWS S3 SDK | Thin S3 client sending high-level object requests (`PUT`, `GET`, `DELETE`) |

## Deliberate divergences

| What | MinIO | objectstorelite | Why |
|---|---|
| **`If-None-Match` is check-then-write, not atomic** | Real put-if-absent is made atomic one of two ways, and objectstorelite does neither. S3 partitions a key→blob index by key and makes each partition a consensus group, so the existence check and the write are a single CAS in one log entry — no lock, no interval. MinIO has no such central authority, so `cmd/dsync` takes a distributed lock across the erasure set first, which is a lock and therefore needs lease expiry. Here the coordinator checks existence and writes as two asynchronous steps, leaving a TOCTOU window: two concurrent PUTs for the same absent key can both pass the check. Demo 6.5 passes because tickloom is deterministic, not because the race is closed. Delta's whole commit protocol rests on this operation, so the gap is worth knowing — see the note in `StorageNode.handlePutObject`. |---|---|
| **Substrate** | Go goroutines, network RPCs, channel pools | `tickloom` (`Process`, `ClusterClient`, `MessageBus`) | Collapses concurrency into deterministic, reproducible tick-driven simulation |
| **No Hash Ring** | Erasure Sets | Erasure Sets | Neither uses consistent hash rings; fixed-size sets avoid full-cluster key churn |
| **Async Primitive** | Go channels | `TickCompletableFuture` | Native tickloom non-blocking future, never blocking OS threads with `.join()` |
| **Storage Layout** | POSIX file system with direct I/O | Standard Java `Path` / `Files` | Portable across operating systems in tests and demos |

## Going deeper

- Upstream source: [MinIO Github](https://github.com/minio/minio)
- Substrate: [tickloom](https://github.com/unmeshjoshi/tickloom) — the tick loop everything runs on

## Storage guarantees

> Full cross-system comparison — HDFS, MinIO, S3, Delta and here — is in
> [docs/STORAGE-GUARANTEES.md](../docs/STORAGE-GUARANTEES.md).

## Fan-out must target storage nodes, never `getAllNodes()`

`LIST` has no key to hash, so it fans out — but to `mapper.allNodes()` (the union of the erasure
sets), not to every process on the bus. The capstone runs brokers and Spark workers on the same
`MessageBus`, and fanning out to all of them sends internal `ListKeys` messages to processes with no
handler. The symptom is quiet: `No handler found` on stderr, and a coordinator that waits for
responses which never come.

The same rule produced the explicit `ErasureSet` the capstone passes at construction — without it
`StorageNode` defaults to `new ErasureSet(0, getAllNodes())` and stripes shards onto brokers. Both
are instances of one rule: **on a shared bus, address the nodes that belong to your subsystem.**
