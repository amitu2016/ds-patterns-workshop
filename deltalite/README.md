# deltalite — a transactional table format

Models **Delta Lake**. Class names, package layout and class responsibilities mirror upstream, so the real
source is recognisable after reading this. The bridge is [MAPPING.md](MAPPING.md).

## Start here

| Demo | Concept | Open |
|------|---------|------|
| _none yet_ | | |

Run any demo from the repo root: `make demo-<id>`. Full index: [../DEMOS.md](../DEMOS.md).

## What this is, and isn't

A **structural** model, not a reimplementation. Where we diverge from Delta Lake deliberately — collapsed
threading, omitted features, simplified wire formats — it is recorded in MAPPING.md rather than
left for you to discover.
