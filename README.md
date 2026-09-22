# Design Patterns of Distributed Systems — workshop code

Small, faithful, runnable models of the systems the workshop is about. Every demo you see on
screen is in here, and every one of them runs on your laptop in seconds.

This checkout holds: Cassandra, Delta Lake, Kafka, MinIO, Parquet, Spark and Kubernetes

Companion code for the *Design Patterns of Distributed Systems* workshop, and for
[*Patterns of Distributed Systems*](https://martinfowler.com/books/patterns-distributed.html).

## Run it

```bash
make demos          # list every demo that exists in this checkout
make demo-2.2       # run one — "Two controllers at once"
make test           # run everything
```

Requirements: **JDK 21** and, if `kubelite` is present, **Go 1.23**. No Docker, no cluster, no
cloud account. ZooKeeper and etcd run embedded, in-process.

First run downloads dependencies and takes a couple of minutes. After that a demo takes seconds —
the simulated clock means nothing waits on real time.

## What you are looking at

Each directory models one real system and is named for it: `cassandralite`, `deltalite`, `kafkalite`, `objectstorelite`, `parquetlite`, `sparklite`, `kubelite`.

Two rules shaped all of them.

**Fidelity.** Class names, package layout and responsibilities mirror the real thing — *including*
the large classes. `ZookeeperClient` is big because `kafka.zk.KafkaZkClient` is big. Each system has
a `MAPPING.md` giving the class-by-class bridge to real upstream source, plus the places this model
deliberately diverges and why. **That file is the most useful thing here.** Open one next to the
real source it names and see how much you recognise.

**Everything runs.** No stubs, no exercises to complete, no TODOs. Where a failure mode is the
lesson, there is code that **misbehaves on purpose** — you will find pairs like:

```java
@Test void withoutEpochFencing_staleControllerOverwritesAssignments() { … }
@Test void withEpochFencing_staleControllerIsRejected()               { … }
```

Run the first, watch it break, then run the second. The diff between the two methods is the lesson.

## Poke at it

Demos print their story — offsets, counts, bytes transferred, which node did what. Several carry a
`TRY IT` comment naming a constant worth changing:

```java
// TRY IT: change ROW_GROUP_SIZE to 1024 or 2048 to see the RDD partition count change!
```

Change it, re-run, and you will see the new behaviour — results are never cached.

`DEMOS.md` indexes every demo against the slide it belongs to.

## If something goes red

Most of this is deterministic and will not flake. Two things are not, because they run against a
*real* embedded ZooKeeper and etcd on wall-clock time rather than the simulated clock:

- `Demo_6_1_HighWatermark`
- `TestListWatch_RetryBehavior` in `kubelite`

If either fails, re-run it. If something else fails, that is worth reporting.

## Reading order

If you want to go further after the workshop, start with the smallest thing that surprised you.
`cassandralite` is about 16 files and covers gossip and failure detection. `kafkalite` is the
biggest and has the most recognisable upstream. Each one is meant to be readable in an afternoon —
that is the whole point of the `-lite` in the name.
