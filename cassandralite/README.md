# cassandralite — peer-to-peer membership

Models **Apache Cassandra**. Class names, package layout and class responsibilities mirror upstream, so the real
source is recognisable after reading this. The bridge is [MAPPING.md](MAPPING.md).

## Start here

| Demo | Concept | Open |
|------|---------|------|
| [1.5](src/test/java/cassandralite/demo/Demo_1_5_GossipConvergence.java) | Gossip convergence across $N$ nodes | `make demo-1.5` |
| [1.6](src/test/java/cassandralite/demo/Demo_1_6_FailureDetection.java) | &phi; Accrual failure detection & recovery | `make demo-1.6` |

Run any demo from the repo root: `make demo-<id>`. Full index: [../DEMOS.md](../DEMOS.md).

## What this is, and isn't

A **structural** model, not a reimplementation. Where we diverge from Apache Cassandra deliberately — collapsed
threading, omitted features, simplified wire formats — it is recorded in MAPPING.md rather than
left for you to discover.
