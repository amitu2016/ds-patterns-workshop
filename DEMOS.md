# DEMOS

Every demo maps to one slide. Run any of them from the repo root:

```bash
make demo-2.2
```

Demos are discovered by class name — `Demo_2_2_EpochFencing` in Java, `TestDemo_2_4_Scheduler` in Go
— so nothing needs registering here to be runnable. `make demos` lists what exists today.

**Status legend:** ⚠️ new = demo must be written · **covered by** = the slide has no demo of its
own and is answered by another.

| ID | Slide | System | What the room sees | Status |
|---|---|---|---|---|
| 1.1 | Assignment 1: Node registration with Zookeeper | `kafkalite` | Ephemeral znode appears; kill session; znode vanishes | ✅ **built** |
| 1.2 | Assignment 2: Node registration with etcd | `kubelite` | Kubelet POSTs → /registry/nodes/<n> in etcd | ✅ **built** |
| 1.3 | Consistent Core Interface & Cluster Primitives | `kubelite` | ListWatch streams Added/Modified/Deleted | ✅ **built** |
| 1.5 | What's common? (gossip) | `cassandralite` | Gossip converges; round count vs cluster size | ✅ **built** |
| 1.6 | Failure detection | `cassandralite` | φ rises as heartbeats stop; node marked down | ✅ **built** |
| 2.1 | One of the brokers is chosen as the Controller | `kafkalite` | Three brokers elect; all agree | ✅ **built** |
| 2.2 | **Two controllers at once** | `kafkalite` | *without*: stale LeaderAndIsr accepted, brokers diverge · *with*: epoch 1 rejected | ✅ **built** |
| 2.3 | ReplicaSet Controller Detailed Flow | `kubelite` | Delete a pod; controller recreates it | ✅ **built** |
| 2.4 | Scheduling Algorithm Step-by-Step | `kubelite` | Filter eliminates a NotReady node, score table per node, bind, then re-score as load shifts | ✅ **built** |
| 3.1 | **What breaks when a node is added** | `kafkalite` | %3→%4: how many of 10,000 keys move | ✅ **built** |
| 3.2 | Fixing the partition count | `kafkalite` | 12 partitions / 3 nodes; add node 4 → only P4,P8,P12 move | ✅ **built** |
| 3.3 | Topic Creation uses fixed number of partitions | `kafkalite` | Replica assignment across brokers | ✅ **built** |
| 3.4 | Parquet Data Organization: Row Groups & Column Chunks | `parquetlite` | Footer dump: row groups, column chunks, min/max stats | ✅ **built** |
| 3.5 | Parquet + Distributed Processing | `parquetlite` | Pushdown: row groups skipped; **exact bytes read** before/after | ✅ **built** |
| 4.1 | Parquet to RDD Mapping | `sparklite` | One row group → one RDD partition | ✅ **built** |
| 4.2 | End-to-End Spark + Parquet Execution | `sparklite` | DAG → stages → tasks across workers | ✅ **built** |
| 4.3 | Spark's Parquet Integration | `sparklite` | Data locality: task scheduled where the row group lives | ✅ **built** |
| 5.1 | Object key → erasure set | `objectstorelite` | CRCMOD vs SIPMOD set mapping | ✅ **built** |
| 5.2 | **Why Erasure Coding** | `objectstorelite` | *without*: 3× replication, lose 2 of 3 → gone, 3× cost | ✅ **built** |
| 5.3 | Reed-Solomon Encode and Reconstruct | `objectstorelite` | *with*: RS(4,2), delete any 2 of 6 → reconstructs, 1.5× cost | ✅ **built** |
| 5.4 | Shard layout & versioned metadata | `objectstorelite` | On-disk shards + xl.meta-style version entries | ✅ **built** |
| 6.1 | **Writes above the high-watermark** | `kafkalite` | *without*: read offset 1, leader dies, value vanishes · *with*: blocked at HW | ✅ **built** |
| 6.2 | Consistency - HighWaterMark | `kafkalite` | HW advances across ticks as replicas fetch over MessageBus | ✅ **built** |
| 6.3 | **The Object Store Problem** | `deltalite/step1` | A transfer spans two files; a reader between them sees money vanish | ✅ **built** |
| 6.4 | Transaction Log Solution | `deltalite/step2` | Files written but uncommitted stay invisible; one log entry publishes both | ✅ **built** |
| 6.5 | **Optimistic Transaction Flow** | `deltalite/step3` | *without*: overwritable version loses a commit silently · *with*: second writer rejected, retries | ✅ **built** |
| 6.6 | Time Travel Queries | `deltalite/step4` | Read table as of version N | ✅ **built** |

## The without/with pattern

Failure-mode demos come in pairs — two methods in **one** class, so the diff between them is the
lesson:

```java
@Test void withoutEpochFencing_staleControllerOverwritesAssignments() { … }
@Test void withEpochFencing_staleControllerIsRejected()               { … }
```

## Demos narrate

A demo that only asserts teaches nothing on a screen share. Print the story — offsets, counts, bytes
skipped, shards lost. `showStandardStreams` is on for exactly this reason.

## TRY IT markers

Each demo carries at least one one-line perturbation:

```java
// TRY IT: change to 4 and re-run. How many of the 10,000 keys change partition?
static final int NUM_PARTITIONS = 3;
```

Rationale and the full build plan: [WORKSHOP-PLAN.md](WORKSHOP-PLAN.md) Part 5.
