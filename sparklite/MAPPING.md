# sparklite → Apache Spark

The bridge from this model to real source. Every model class belongs in this table.

| sparklite | Apache Spark | upstream source |
|---|---|---|
| `sparklite.rdd.RDDLite` | `org.apache.spark.rdd.RDD` | `core/src/main/scala/org/apache/spark/rdd/RDD.scala` |
| `sparklite.rdd.Partition` | `org.apache.spark.Partition` | `core/src/main/scala/org/apache/spark/Partition.scala` |
| `sparklite.rdd.ParallelCollectionRDDLite` | `org.apache.spark.rdd.ParallelCollectionRDD` | `core/src/main/scala/org/apache/spark/rdd/ParallelCollectionRDD.scala` |
| `sparklite.rdd.MapRDDLite` | `org.apache.spark.rdd.MapPartitionsRDD` | `core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala` |
| `sparklite.rdd.FilterRDDLite` | `org.apache.spark.rdd.MapPartitionsRDD` | `core/src/main/scala/org/apache/spark/rdd/MapPartitionsRDD.scala` |
| `sparklite.parquet.ParquetRDDLite` | `org.apache.spark.sql.execution.datasources.FileScanRDD` | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FileScanRDD.scala` |
| `sparklite.parquet.ParquetPartition` | `org.apache.spark.sql.execution.datasources.FilePartition` | `sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/FilePartition.scala` |
| `sparklite.scheduler.DAGScheduler` | `org.apache.spark.scheduler.DAGScheduler` | `core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala` |
| `sparklite.scheduler.TaskScheduler` | `org.apache.spark.scheduler.TaskSchedulerImpl` | `core/src/main/scala/org/apache/spark/scheduler/TaskSchedulerImpl.scala` |
| `sparklite.scheduler.Stage` | `org.apache.spark.scheduler.Stage` | `core/src/main/scala/org/apache/spark/scheduler/Stage.scala` |
| `sparklite.scheduler.ResultStage` | `org.apache.spark.scheduler.ResultStage` | `core/src/main/scala/org/apache/spark/scheduler/ResultStage.scala` |
| `sparklite.scheduler.Task` | `org.apache.spark.scheduler.Task` | `core/src/main/scala/org/apache/spark/scheduler/Task.scala` |
| `sparklite.scheduler.RDDLiteTask` | `org.apache.spark.scheduler.ResultTask` | `core/src/main/scala/org/apache/spark/scheduler/ResultTask.scala` |
| `sparklite.scheduler.JobWaiter` | `org.apache.spark.scheduler.JobWaiter` | `core/src/main/scala/org/apache/spark/scheduler/JobWaiter.scala` |
| `sparklite.scheduler.LocalScheduler` | `org.apache.spark.scheduler.local.LocalSchedulerBackend` | `core/src/main/scala/org/apache/spark/scheduler/local/LocalSchedulerBackend.scala` |
| `sparklite.protocol.TaskLocality` | `org.apache.spark.scheduler.TaskLocality` | `core/src/main/scala/org/apache/spark/scheduler/TaskLocality.scala` |
| `sparklite.worker.SparkLiteWorker` | `org.apache.spark.executor.CoarseGrainedExecutorBackend` | `core/src/main/scala/org/apache/spark/executor/CoarseGrainedExecutorBackend.scala` |
| `sparklite.context.SparkLiteDriver` | `org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend` | `core/src/main/scala/org/apache/spark/scheduler/cluster/CoarseGrainedSchedulerBackend.scala` |
| `sparklite.context.SparkLiteContext` | `org.apache.spark.SparkContext` | `core/src/main/scala/org/apache/spark/SparkContext.scala` |

## Deliberate divergences

| What | Why |
|---|---|
| Tickloom substrate (`Process`, `MessageBus`) | Real Spark relies on Akka / Netty RPC and multi-threaded event loops. `sparklite` runs deterministically on tickloom's discrete tick loop, allowing test reproduction without race conditions or thread sleeps. |
| `TickCompletableFuture` (No threads / `CompletableFuture`) | Real Spark manages thread pools and execution contexts. `sparklite` runs entirely on tickloom's non-blocking `TickCompletableFuture` and discrete tick progression, eliminating all `CompletableFuture`, thread pools, and blocking `.join()` calls. |
| 1 Row Group = 1 RDD Partition | Apache Spark groups multiple small splits together (`maxPartitionBytes`, default 128MB). `sparklite` strictly maps 1 Parquet Row Group to 1 Partition to illuminate the physical file layout to RDD mapping directly on a slide share. |
| `parquetlite` explicit range reads | Real Spark reads Parquet via Hadoop FileSystem abstractions (`HadoopInputFile`). `sparklite` delegates columnar range reads directly through `parquetlite`'s two-phase range reader over `objectstorelite`. |
| Java serialization for tasks | Spark uses Java/Kryo serialization to transmit closures over RPC. `sparklite` serializes `RDDLiteTask` using `SerializerUtil` over `MessageBus` payloads, preserving closure encapsulation. |
| ResultStage without ShuffleMapStage | Focuses purely on narrow transformations (filter, map) and data locality without introducing shuffle partitions or external shuffle services. |
| `RDDLite.compute` returns a `TickCompletableFuture`, not an `Iterator` | Every Spark `compute` returns an `Iterator[T]` synchronously, because an executor has a thread pool and can block on the read. tickloom has no threads to block, so a partition that reads over the network must hand back a future the scheduler's tick loop drives. The visible consequence is that `ParallelCollectionRDDLite`'s future is always already completed while `ParquetRDDLite`'s is not — the in-memory RDD's asynchrony is pure ceremony. |
| `clientType()` / `setClient()` / `ClientRegistry` | Upstream's executor **pulls**: `compute` names a process-wide pool at compile time (`KafkaDataConsumer.acquire`, `FileSystem.get`) which constructs the client on a miss. Nothing walks a lineage injecting clients and there is no `clientType()`. tickloom clients can only be created by `Cluster`, so a client can only be *handed* to a worker — sparklite pushes, and a pusher must be told what to push. `ClientRegistry` plays the part DNS, the NameNode and Kafka's Metadata request play upstream: it resolves a name to a connection, prepopulated rather than queried. The `@transient` field that fails loudly when unset **is** upstream — `RDD.sc` throws "This RDD lacks a SparkContext" on an executor. Full reasoning in [docs/SPARKLITE-CLIENT-INJECTION.md](../docs/SPARKLITE-CLIENT-INJECTION.md). |
| `TaskResultCallback` has no upstream counterpart | Upstream's `DAGScheduler` event loop calls `JobWaiter.taskSucceeded` directly. Here worker replies arrive through tickloom's `RequestWaitingList`, so `TaskResultCallback` adapts one to the other and holds no aggregation of its own. The waiting list owns *when* a task is considered lost (per-task expiry); `JobWaiter` owns *what* the job returns. |

## Going deeper

- Upstream source: [Apache Spark on GitHub](https://github.com/apache/spark)
- Substrate: [tickloom](https://github.com/unmeshjoshi/tickloom) — the tick loop everything runs on

