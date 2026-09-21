# cassandralite → Apache Cassandra

The bridge from this model to real source. Every model class belongs in this table.

| cassandralite | Apache Cassandra | upstream source |
|---|---|---|
| `cassandralite.gossip.Gossiper` | `org.apache.cassandra.gms.Gossiper` | `src/java/org/apache/cassandra/gms/Gossiper.java` |
| `cassandralite.gossip.EndpointState` | `org.apache.cassandra.gms.EndpointState` | `src/java/org/apache/cassandra/gms/EndpointState.java` |
| `cassandralite.gossip.HeartBeatState` | `org.apache.cassandra.gms.HeartBeatState` | `src/java/org/apache/cassandra/gms/HeartBeatState.java` |
| `cassandralite.gossip.ApplicationState` | `org.apache.cassandra.gms.ApplicationState` | `src/java/org/apache/cassandra/gms/ApplicationState.java` |
| `cassandralite.gossip.VersionedValue` | `org.apache.cassandra.gms.VersionedValue` | `src/java/org/apache/cassandra/gms/VersionedValue.java` |
| `cassandralite.gossip.GossipDigest` | `org.apache.cassandra.gms.GossipDigest` | `src/java/org/apache/cassandra/gms/GossipDigest.java` |
| `cassandralite.protocol.CassandraLiteProtocol.GossipDigestSyn` | `org.apache.cassandra.gms.GossipDigestSyn` | `src/java/org/apache/cassandra/gms/GossipDigestSyn.java` |
| `cassandralite.protocol.CassandraLiteProtocol.GossipDigestAck` | `org.apache.cassandra.gms.GossipDigestAck` | `src/java/org/apache/cassandra/gms/GossipDigestAck.java` |
| `cassandralite.protocol.CassandraLiteProtocol.GossipDigestAck2` | `org.apache.cassandra.gms.GossipDigestAck2` | `src/java/org/apache/cassandra/gms/GossipDigestAck2.java` |
| `cassandralite.heartbeat.FailureDetector` | `org.apache.cassandra.gms.IFailureDetector` | `src/java/org/apache/cassandra/gms/IFailureDetector.java` |
| `cassandralite.heartbeat.PhiAccrualFailureDetector` | `org.apache.cassandra.gms.FailureDetector` | `src/java/org/apache/cassandra/gms/FailureDetector.java` |
| `cassandralite.heartbeat.ArrivalWindow` | `org.apache.cassandra.gms.ArrivalWindow` | `src/java/org/apache/cassandra/gms/ArrivalWindow.java` |
| `cassandralite.heartbeat.ServerState` | `org.apache.cassandra.gms.IFailureDetectionEventListener` | `src/java/org/apache/cassandra/gms/IFailureDetectionEventListener.java` |

## Deliberate divergences

| What | Why |
|---|---|
| Tickloom substrate (`Replica`, `MessageBus`) | Real Cassandra runs scheduled background executor threads (`ScheduledThreadPoolExecutor`) firing roughly once every 1,000ms, alongside a Netty messaging service. `cassandralite` collapses all concurrency onto tickloom's discrete tick loop, allowing deterministic reproduction of gossip rounds, seed discovery, and message routing without thread sleeps or network sockets. |
| 3-Way Handshake with JSON serialization | Real Cassandra encodes gossip verbs using custom low-level binary streaming (`IVersionedSerializer`). `cassandralite` faithfully implements the 3-way handshake (`GossipDigestSyn` → `GossipDigestAck` → `GossipDigestAck2`) while serializing state as JSON records over `MessageBus` payloads for maximum inspection and legibility. |
| Discrete Tick &phi; Accrual | Real Cassandra measures inter-arrival times using monotonic nanosecond clocks (`System.nanoTime()`). `cassandralite` measures intervals in discrete ticks, which eliminates wall-clock flakiness in automated testing and makes failure conviction reproducible down to the exact tick. |
| Collapsed Failure Event Listeners | Real Cassandra propagates state events across multiple internal listeners (`IEndpointStateChangeSubscriber`, `IFailureDetectionEventListener`). `cassandralite` integrates failure detector evaluations directly into the gossiper's tick lifecycle. |

## Going deeper

- Upstream source: [Apache Cassandra on GitHub](https://github.com/apache/cassandra)
- CASSANDRA-2597: [Phi Accrual Failure Detector implementation math](https://issues.apache.org/jira/browse/CASSANDRA-2597)
- Substrate: [tickloom](https://github.com/unmeshjoshi/tickloom) — the tick loop everything runs on
