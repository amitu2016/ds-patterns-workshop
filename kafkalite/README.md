# kafkalite — a partitioned log

Models **Apache Kafka**. Class names, package layout and class responsibilities mirror upstream, so the real
source is recognisable after reading this. The bridge is [MAPPING.md](MAPPING.md).

## Start here

| Demo | Concept | Open |
|------|---------|------|
| `1.1` | Broker registration + ephemeral liveness = the lease | [`ZookeeperClient`](src/main/java/kafkalite/zookeeper/ZookeeperClient.java) · [`BrokerServer`](src/main/java/kafkalite/server/BrokerServer.java) |
| `2.1` _(next)_ | ZooKeeper callbacks → tick loop | [`ZkEventQueue`](src/main/java/kafkalite/zookeeper/ZkEventQueue.java) |

Run any demo from the repo root: `make demo-<id>`. Full index: [../DEMOS.md](../DEMOS.md).

## What this is, and isn't

A **structural** model, not a reimplementation. Where we diverge from Apache Kafka deliberately — collapsed
threading, omitted features, simplified wire formats — it is recorded in MAPPING.md rather than
left for you to discover.
