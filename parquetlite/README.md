# parquetlite — Columnar File Format & Object Store Range I/O

`parquetlite` models **Apache Parquet** (`parquet-format` and `parquet-mr`). Class names, metadata layouts, and range reading patterns mirror upstream, so the real big-data engines (Spark, DuckDB, Trino, Arrow DataFusion) are immediately recognizable.

> **System Classification**: Unlike `kafkalite`, `kubelite`, or `objectstorelite`, which are distributed systems running processes over tickloom's `MessageBus`, `parquetlite` is an **I/O Format & Codec Library**. It provides the columnar file format, footer metadata indexing, and byte-range reading primitives that downstream distributed systems (`sparklite` and `deltalite`) build upon.

---

## Start Here

| Demo | Concept | Command |
|---|---|---|
| **3.4** | Parquet Data Organization: Row Groups & Column Chunks | `make demo-3.4` |
| **3.5** | Parquet + Distributed Processing: Predicate Pushdown over Object Storage | `make demo-3.5` |

Full index with slide mapping: [../DEMOS.md](../DEMOS.md).

---

## Key Architecture: Explicit Two-Phase Range Reads

Instead of masking object storage behind an opaque streaming `InputStream`, `parquetlite` demonstrates the **Explicit Two-Phase Range Read** pattern used by modern cloud query engines:

1. **Footer Read (Phase 1)**:
   - Reads the last 8 bytes of the S3 object to get the little-endian footer length $L$ and magic bytes `PAR1`.
   - Reads the exact metadata range `[fileSize - 8 - L .. fileSize - 1]` without downloading row group data.
   - Parses `FileMetaData`, `BlockMetaData` (row groups), and `ColumnChunkMetaData` in memory.
2. **Predicate Pushdown & Row Group Pruning (Phase 2)**:
   - Compares query filter predicates (e.g. `age >= 60`) against per-column `min`/`max` statistics stored in the footer.
   - Skips entire row groups where the predicate is impossible, transferring **0 bytes** of data over the network for those row groups.
3. **Targeted Data Range Reads (Phase 3)**:
   - Issues byte-range requests (`GET Range: bytes=start-end`) for *only* the surviving row groups and queried columns.

---

## Source Mapping

See [MAPPING.md](MAPPING.md) for direct class mappings to `parquet-format` (Thrift definitions) and `parquet-mr` (Java implementation).
