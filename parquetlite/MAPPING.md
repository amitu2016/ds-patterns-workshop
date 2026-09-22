# parquetlite → Apache Parquet Mapping

The bridge from `parquetlite` to Apache Parquet source code (`parquet-format` and `parquet-mr`).

| parquetlite | Apache Parquet (`parquet-format` / `parquet-mr`) | Upstream Source / Responsibility |
|---|---|---|
| `TableSchema` | `org.apache.parquet.schema.MessageType` | Schema definition, primitive types, required/optional fields |
| `TableRecord` | `org.apache.parquet.example.data.Group` | In-memory row/record representation |
| `PrefetchedInputFile` | `org.apache.parquet.io.InputFile` + `S3APrefetchingInputStream` | The only in-memory `InputFile`: selected ranges via `of`/`with`, or a whole object via `wholeObject` |
| `PrefetchedInputFile.PrefetchedSeekableInputStream` | `org.apache.parquet.io.SeekableInputStream` | Random access across the fetched ranges; throws outside them |
| `ByteRange` | An S3 `Range` GET header | The unit a reader asks for: `[offset, offset + length)` of an object |
| `RowGroupFilter` | `org.apache.parquet.filter2.compat.RowGroupFilter` | Evaluates predicates against row group min/max `Statistics` |
| `ParquetFooterReader` | `org.apache.parquet.hadoop.ParquetFileReader.readFooter` | Reads and dumps `FileMetaData`, `BlockMetaData`, and `ColumnChunkMetaData` |
| `ObjectStoreParquetReader` | Vectorized S3 Reader (DuckDB / Trino / Spark) | Names the ranges a read needs: size -> footer length -> footer -> filter -> target row groups |

---

## One name, two meanings: `BlockMetaData`

Parquet's spec calls the unit a **row group**; parquet-mr's Java class calls it
`BlockMetaData`, and `ParquetMetadata.getBlocks()` returns one per row group. Code here uses the
spec's word (`RowGroupFilter`, `rowGroupRange`) even where the type says otherwise.

The class name is not arbitrary, and the reason matters for `sparklite`. A row group was designed
to **be** one HDFS block: `ParquetWriter.withRowGroupSize(...)` writes the config key
`parquet.block.size`, whose default is 128MB — HDFS's default block size. Align them and one task
reads one block, entirely from one DataNode, which is what makes Parquet-on-HDFS local at all.

So the 1:1 chain *row group → RDD partition → task* is not a coincidence of this model; it is the
layout Parquet was shaped for. Two cautions follow. A Parquet "block" is **not** an HDFS block —
they coincide only when sized to, and this repo writes 512-byte row groups so demos produce several
from tiny data. And on object storage the alignment buys nothing, because there are no blocks and no
placement to be local to (see `sparklite`'s demo 4.3).

## Deliberate Divergences

| What | Why |
|---|---|
| **Explicit Two-Phase Range Reads instead of opaque streaming** | S3 stream wrappers hide network requests behind `InputStream.read()`. Explicit range reads make every byte transferred and skipped visible to students on screen. |
| **The object's size is fetched, never assumed** | A reader arriving at an existing object has no local copy to measure, and `ParquetFileReader` needs the length before it can find the footer at the tail. Upstream that is `FileSystem.getFileStatus` — a `HEAD Object` on S3, issued by `HadoopInputFile.fromPath` before any read; here it is `ObjectStoreClient.getObjectSize`. It costs a round trip and zero body bytes, which is why demo 3.5 counts it as a request but not as traffic — and why newer readers drop it for a suffix range (`Range: bytes=-8`), whose `Content-Range` header carries the total. |
| **`ObjectStoreParquetReader` returns ranges; it never fetches** | Upstream hides I/O behind `SeekableInputStream.read()`, so a reader discovers what it needs while decoding. `footerLengthHintRange` → `footerRange` → `readFooter` exposes both round trips a footer read genuinely costs, and `rowGroupRange` names what a task fetches. Callers do the fetching, so nothing in `src/main` performs I/O — which is what lets a sparklite worker, unable to block on a `TickCompletableFuture`, share this code with a driver that can. |
| **Prefetch rather than read-on-demand** | Upstream's `S3AInputStream` issues a GET per read as `ParquetFileReader` decodes. `PrefetchedInputFile` requires every byte in memory first and throws if asked for one that was not fetched. That is Hadoop 3.4's `S3APrefetchingInputStream` strategy, which upstream ships alongside the lazy one — and here it is required rather than optional, because tickloom has no executor thread pool to block. The throw is load-bearing: an unprefetched read means the range calculation was wrong. |
| **No lazy `InputFile` over the object store** | An earlier `ObjectStoreInputFile`/`RangeFetcher` pair modelled `S3AInputStream` directly. It was removed: the callback existed only to hide the fact that nothing in `src/main` can block (`tickloom-testkit` is `testImplementation` for every subproject), and it needed `preloadFooter` to stop the lazy stream re-fetching the footer on every open. Explicit ranges remove the need instead of hiding it. |
