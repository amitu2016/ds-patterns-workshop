# kafkalite → Apache Kafka

The bridge from this model to real source. Every model class belongs in this table.

Upstream reference: [apache/kafka](https://github.com/apache/kafka) (ZooKeeper-mode classes;
removed in Kafka 4.0 but the clearest place to see these primitives in isolation).

## Classes

| kafkalite | Apache Kafka | Notes |
|---|---|---|
| `kafkalite.zookeeper.ZookeeperClient` | `kafka.zk.KafkaZkClient` | Real KafkaZkClient is ~2000 lines and serves *every* subsystem — brokers, topics, controller, ISR, configs, ACLs. Ours grows the same way on purpose. |
| `kafkalite.cluster.Broker` | `kafka.cluster.Broker` | Value at `/brokers/ids/<id>`. Upstream also carries multiple listener endpoints and a rack id. |
| `kafkalite.server.BrokerServer` | `kafka.server.KafkaServer` | `KafkaServer#startup`'s broker registration happens in `onStart()`. See divergences. |
| `kafkalite.common.Config` | `kafka.server.KafkaConfig` | Upstream has several hundred settings; we keep the handful the workshop touches. |
| `kafkalite.common.ZKStringSerializer` | `kafka.utils.ZKStringSerializer` | znode data is UTF-8 JSON, so `zkCli.sh` can read it. |
| `kafkalite.common.JsonSerDes` | `kafka.zk.ZkData` | znode payloads only. Inter-broker messages use tickloom's `MessageCodec`. |
| `kafkalite.zookeeper.ZkEventQueue` | `kafka.controller.ControllerEventManager` | See below — the load-bearing one. Consumed by `ZkController`. |
| `kafkalite.controller.ZkController` | `kafka.controller.KafkaController` | Elects on `/controller`, increments `/controller_epoch`, owns `/brokers/ids` watch, pushes metadata. |
| `kafkalite.controller.ControllerExistsException` | `kafka.common.ControllerExistsException` | Thrown when `/controller` already exists during election race. |
| `kafkalite.cluster.UpdateMetadataRequest` | `kafka.api.UpdateMetadataRequest` | Pushes membership and partition state to brokers; carries `controllerEpoch`. |
| `kafkalite.cluster.LeaderAndIsrRequest` | `kafka.api.LeaderAndIsrRequest` | Instructs brokers on partition leadership and ISR; carries `controllerEpoch`. |
| `kafkalite.cluster.LeaderAndReplicas` | `kafka.controller.LeaderAndIsr` | Pairs a `TopicAndPartition` with its `PartitionStateInfo`. |
| `kafkalite.cluster.PartitionStateInfo` | `kafka.controller.PartitionStateInfo` | Leader broker ID and replica list for a partition. |
| `kafkalite.common.TopicAndPartition` | `kafka.common.TopicAndPartition` | Topic name and partition number pair. |
| `kafkalite.zookeeper.PartitionInfo` | `kafka.zk.TopicZNode` | Raw partition-to-replicas assignment stored in ZooKeeper. |
| `kafkalite.common.ReplicaAssigner` | `kafka.admin.AdminUtils` | Round-robin replica assigner spreading leaders and replicas across brokers. |
| `kafkalite.admin.CreateTopicCommand` | `kafka.admin.TopicCommand` | Admin command discovering brokers, generating replica assignments, and saving to ZK. |
| `kafkalite.log.Log` | `kafka.log.UnifiedLog` / `LogSegment` | On-disk append-only binary commit log using `FileChannel` with monotonic offsets and recovery. |
| `kafkalite.partition.Partition` | `kafka.cluster.Partition` | Tracks leader/follower role, ISR replica progress, and monotonically advances `highWatermark`. |
| `kafkalite.cluster.ReplicaManager` | `kafka.server.ReplicaManager` | Coordinates local partition instances and handles `makeLeader` / `makeFollower`. |
| `kafkalite.api.ProduceRequest` / `ProduceResponse` | `org.apache.kafka.common.requests.Produce*` | Inter-broker / client produce request and response. |
| `kafkalite.api.FetchRequest` / `FetchResponse` | `org.apache.kafka.common.requests.Fetch*` | Inter-broker replica fetch and consumer read requests carrying High Watermark. |
| `kafkalite.api.FetchIsolation` | `org.apache.kafka.common.IsolationLevel` | `LOG_END` (for replication fetchers) vs `HIGH_WATERMARK` (for consumers). |

## ZooKeeper paths — identical to upstream

| path | holds |
|---|---|
| `/brokers/ids/<id>` | **ephemeral** — broker registration and liveness |
| `/brokers/topics/<topic>` | persistent — partition→replica assignment |
| `/controller` | **ephemeral** — the elected controller |
| `/controller_epoch` | **persistent** — monotonic epoch counter for epoch fencing |

## Who watches what — preserved exactly

This asymmetry is the subject of Topics 2 and 3, so it is not blurred:

| | watches `/brokers/ids` & `/brokers/topics` | learns membership & partitions from |
|---|---|---|
| the **controller** | ✅ yes — `BrokerChangeHandler`, `TopicChangeHandler` | ZooKeeper directly |
| every **other broker** | ❌ no | the controller's `UpdateMetadata` & `LeaderAndIsr` over the network |

`BrokerServer` therefore only *registers itself*; its `aliveBrokers` list is filled by
`updateMetadata(...)` and its `partitionAssignments` map is filled by `handleLeaderAndIsr(...)` over the network. One node reads cluster state from the
Consistent Core; everyone else is told.

## Two constraints of the harness, worth knowing before writing a demo

**1. ZooKeeper is invisible to tickloom's fault injection.** `ClusterEvents.partition(...)`,
`reconnect(...)` and `delay(...)` act on the *simulated* network. ZooKeeper connections are real
TCP sockets that never travel over it, so no network fault can sever a broker from the Consistent
Core. Use `endsZooKeeperSessionOf(...)` for that. Broker-to-broker traffic (`LeaderAndIsr`,
`UpdateMetadata`) *does* cross the simulated network, so `partition(...)` is the right tool there.

**2. Startup is a lifecycle, not a call.** `BrokerServer.onStart()` registers in ZooKeeper and
starts the controller. tickloom calls it from `Cluster.start()`, once every process has been
constructed, so declaring `.servers(...)` is enough to have a registered cluster and no demo calls
`startup()`.

Registration is synchronous, which nothing else in this repo can be: `zk.registerSelf()` talks to a
*real* ZooKeeper over a real socket (D5), and its client blocks. Everything that crosses the
simulated network has to return a future instead.

⚠️ *This replaces an earlier design, recorded because the trap is easy to fall back into.*
Registration used to run in `onInit()`, which `Process`'s **constructor** invokes — before subclass
field initialisers run, so anything a subclass assigned was silently clobbered and `BrokerServer`
could hold no field initialisers at all. It worked around that by returning an incomplete future and
registering on the first `onTick()`. tickloom later added `start()`/`onStart()`, called after
construction, which removes the hazard rather than working around it. Overriding `onInit()` in a
`Process` subclass is still a trap; there is now no reason to.

## Deliberate divergences

| What | Why |
|---|---|
| **Broker threading is collapsed.** Real `KafkaServer` runs a `SocketServer` acceptor/processor pool, a `KafkaRequestHandlerPool` of workers, and per-broker replica fetcher threads. Here everything runs on one tickloom tick loop. | Removes accidental complexity, and makes concurrency demos reproducible instead of lucky. The *controller's* event model stays faithful — see next row. |
| **`ZkEventQueue` is not a 1:1 port**, but the pattern is upstream's. Modern Kafka's controller has exactly this shape: ZooKeeper watches only *enqueue* a `ControllerEvent`, and a single `ControllerEventThread` drains it. | That redesign happened because ZK callbacks mutating controller state directly caused concurrency bugs. Our tick loop plays the part of that single thread. |
| **Wire format is JSON over tickloom**, not Kafka's binary protocol. | The protocol is not what this course teaches; the coordination is. |
| **`Broker` is a class, not a record.** | Jackson needs a no-arg constructor to read it back out of a znode. |
| **ZooKeeper itself is real** (embedded, not modelled). | It *is* the Consistent Core being taught. |
| **Direct disk append (`FileChannel`) vs tickloom `Storage`.** | `tickloom`'s `Storage` interface is designed for single-file or key-value abstractions, and its asynchronous completion model adds significant callback noise when managing multiple partition segment files per broker. Direct `FileChannel` append-only logs keep the code clean, readable, synchronous, and faithful to Kafka's segment layout without obscuring the High Watermark and replication protocol. |

## Going deeper

- `kafka.zk.KafkaZkClient` — the class this module is shaped after
- `kafka.controller.ControllerEventManager` — the queue pattern `ZkEventQueue` mirrors
- [tickloom](https://github.com/unmeshjoshi/tickloom) — the tick loop; `Process.onTick()` is where our ZooKeeper events are drained

## Offset conventions — a divergence that bites integrators

| | kafkalite | Apache Kafka |
|---|---|---|
| first offset | **1** | 0 |
| high-watermark | **last committed offset (inclusive)** | offset of the *next* record (exclusive) |
| `read(start, HW)` | `[start .. HW]` inclusive; rejects `start <= 0` | `[start, HW)` half-open |

A reader written to Kafka's convention gets **nothing** here — `Partition.read` returns an empty
list for `startOffset <= 0`, silently. The capstone's `KafkaRDDLite` hit this twice: once reading
from offset 0, and again with `if (hw > start)`, which skips a partition holding exactly one record
because inclusive ranges make `start == hw` a valid single-record range.
