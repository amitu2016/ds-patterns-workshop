package parquetlite.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import parquetlite.filter.RowGroupFilter;

import java.io.IOException;

/**
 * Reads and inspects Parquet footer metadata from local files or byte slices.
 */
public class ParquetFooterReader {

    /**
     * Reads footer metadata from a local filesystem path.
     */
    public static ParquetMetadata readFooter(java.nio.file.Path path) throws IOException {
        Configuration conf = new Configuration();
        HadoopInputFile inputFile = HadoopInputFile.fromPath(new Path(path.toUri()), conf);
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            return reader.getFooter();
        }
    }

    /**
     * Reads footer metadata from an {@link InputFile} (such as {@link parquetlite.io.PrefetchedInputFile}, or a {@code HadoopInputFile} over a local file).
     */
    public static ParquetMetadata readFooter(InputFile inputFile) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            return reader.getFooter();
        }
    }

    /**
     * Generates a detailed, human-readable terminal dump of the Parquet footer.
     */
    public static String formatFooterDump(ParquetMetadata metadata) {
        StringBuilder sb = new StringBuilder();
        FileMetaData fileMeta = metadata.getFileMetaData();

        sb.append(String.format("📄 PARQUET FILE FOOTER SUMMARY%n"));
        sb.append(String.format("   Created By:       %s%n", fileMeta.getCreatedBy()));
        sb.append(String.format("   Total Row Groups: %d%n", metadata.getBlocks().size()));
        sb.append(String.format("   Schema:           %s%n", fileMeta.getSchema().getName()));

        for (int i = 0; i < fileMeta.getSchema().getFieldCount(); i++) {
            sb.append(String.format("      - Field [%d]: %-6s %-7s %s%n",
                    i,
                    fileMeta.getSchema().getFieldName(i),
                    fileMeta.getSchema().getType(i).asPrimitiveType().getPrimitiveTypeName(),
                    fileMeta.getSchema().getType(i).getRepetition().name().toLowerCase()));
        }

        sb.append(String.format("%n ROW GROUPS & COLUMN CHUNKS:%n"));
        for (int i = 0; i < metadata.getBlocks().size(); i++) {
            BlockMetaData block = metadata.getBlocks().get(i);
            sb.append(String.format("   ┌── Row Group [%d]: rows=%d, startingPos=%d, size=%d bytes%n",
                    i, block.getRowCount(), block.getStartingPos(), block.getCompressedSize()));

            for (ColumnChunkMetaData col : block.getColumns()) {
                Statistics<?> stats = col.getStatistics();
                String statsStr = "none";
                if (stats != null && stats.hasNonNullValue()) {
                    String min = RowGroupFilter.cleanString(stats.minAsString());
                    String max = RowGroupFilter.cleanString(stats.maxAsString());
                    statsStr = String.format("min=%s, max=%s, nulls=%d", min, max, stats.getNumNulls());
                }

                sb.append(String.format("   │   ├── Column [%-6s]: type=%-7s offset=%-6d size=%-5d stats=[%s]%n",
                        col.getPath().toDotString(),
                        col.getPrimitiveType().getPrimitiveTypeName(),
                        col.getStartingPos(),
                        col.getTotalSize(),
                        statsStr));
            }
            sb.append(String.format("   └──%n"));
        }

        return sb.toString();
    }
}
