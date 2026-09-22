# objectstorelite — an erasure-coded object store

Models **MinIO**. Class names, package layout, and responsibilities mirror upstream MinIO and Backblaze Reed-Solomon coding, so the real source is recognizable after reading this. The bridge is [MAPPING.md](MAPPING.md).

## Start here

| Demo | Concept | What the room sees |
|------|---------|-------------------|
| `5.1` | Object key → erasure set | CRCMOD vs SIPMOD set mapping, determinism, and salt isolation |
| `5.2` | **Why Erasure Coding** | *without*: 3× replication costs 3.0× disk · *with*: RS(4,2) cuts cost to 1.5× while tolerating 2 node failures |
| `5.3` | Reed-Solomon Encode and Reconstruct | Real 62MB video (`BigBuckBunny_320x180.mp4`): lose any 2 of 6 nodes → 100% bit-exact reconstruction in ~60ms; lose 3 nodes → quorum fails safely |
| `5.4` | Shard layout & versioned metadata | On-disk `part.1` shards + `xl.meta` append-only version entries and delete markers |

Run any demo from the repo root: `make demo-<id>`. Full index: [../DEMOS.md](../DEMOS.md).

## Architecture & Client API

Built on [tickloom](https://github.com/unmeshjoshi/tickloom):
- Storage nodes extend `Replica`.
- `ObjectStoreClient` extends `ClusterClient`, returning native `TickCompletableFuture` instances (matching `QuorumKVClient` and `PaxosLogClient` in `distrib-patterns-workshop`).
- Never blocks OS threads or calls `.join()`.
- Client API (`putObject`, `getObject`, `getObjectRange`, `getObjectSize`, `deleteObject`) provides range reads needed by downstream systems (`parquetlite`, `sparklite`, `deltalite`).
