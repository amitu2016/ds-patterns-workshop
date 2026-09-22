package parquetlite.filter;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.util.ArrayList;
import java.util.List;

/**
 * Evaluates predicates against Parquet row group column chunk {@link Statistics}.
 * Used to skip row groups without reading any data pages (predicate pushdown / row group pruning).
 */
public class RowGroupFilter {

    /**
     * Finds column chunk metadata for a specific column within a row group.
     */
    public static ColumnChunkMetaData findColumnChunk(BlockMetaData rowGroup, String columnName) {
        for (ColumnChunkMetaData column : rowGroup.getColumns()) {
            if (column.getPath().toDotString().equals(columnName)) {
                return column;
            }
        }
        return null;
    }

    /**
     * Checks if a row group can be skipped for an integer predicate: {@code column >= minValue}.
     * If the maximum value in the column chunk is strictly less than {@code minValue},
     * no record in this row group can possibly match, so it can be skipped.
     */
    public static boolean canSkipMinInteger(BlockMetaData rowGroup, String columnName, int minValue) {
        ColumnChunkMetaData col = findColumnChunk(rowGroup, columnName);
        if (col == null || col.getStatistics() == null || !col.getStatistics().hasNonNullValue()) {
            return false; // Conservative: keep if no stats
        }
        Statistics<?> stats = col.getStatistics();
        if (stats.genericGetMax() instanceof Number maxNum) {
            return maxNum.intValue() < minValue;
        }
        return false;
    }

    /**
     * Checks if a row group can be skipped for an integer predicate: {@code column <= maxValue}.
     */
    public static boolean canSkipMaxInteger(BlockMetaData rowGroup, String columnName, int maxValue) {
        ColumnChunkMetaData col = findColumnChunk(rowGroup, columnName);
        if (col == null || col.getStatistics() == null || !col.getStatistics().hasNonNullValue()) {
            return false;
        }
        Statistics<?> stats = col.getStatistics();
        if (stats.genericGetMin() instanceof Number minNum) {
            return minNum.intValue() > maxValue;
        }
        return false;
    }

    /**
     * Filters row groups for a point lookup key (e.g. primary key search).
     *
     * @return List of row group indices that might contain the key.
     */
    public static List<Integer> filterForPointLookup(List<BlockMetaData> rowGroups, String columnName, String searchKey) {
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < rowGroups.size(); i++) {
            BlockMetaData rowGroup = rowGroups.get(i);
            ColumnChunkMetaData col = findColumnChunk(rowGroup, columnName);
            if (col == null || col.getStatistics() == null || !col.getStatistics().hasNonNullValue()) {
                candidates.add(i);
                continue;
            }
            String minStr = cleanString(col.getStatistics().minAsString());
            String maxStr = cleanString(col.getStatistics().maxAsString());

            // Key must be >= min and <= max
            boolean mightMatch = searchKey.compareTo(minStr) >= 0 && searchKey.compareTo(maxStr) <= 0;
            if (mightMatch) {
                candidates.add(i);
            }
        }
        return candidates;
    }

    /**
     * Filters row groups for a range query [startKey, endKey].
     */
    public static List<Integer> filterForStringRange(List<BlockMetaData> rowGroups, String columnName, String startKey, String endKey) {
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < rowGroups.size(); i++) {
            BlockMetaData rowGroup = rowGroups.get(i);
            ColumnChunkMetaData col = findColumnChunk(rowGroup, columnName);
            if (col == null || col.getStatistics() == null || !col.getStatistics().hasNonNullValue()) {
                candidates.add(i);
                continue;
            }
            String minStr = cleanString(col.getStatistics().minAsString());
            String maxStr = cleanString(col.getStatistics().maxAsString());

            // No overlap if endKey < minStr OR startKey > maxStr
            boolean noOverlap = (endKey != null && endKey.compareTo(minStr) < 0) ||
                               (startKey != null && startKey.compareTo(maxStr) > 0);
            if (!noOverlap) {
                candidates.add(i);
            }
        }
        return candidates;
    }

    /**
     * Filters row groups where integer column >= minValue.
     */
    public static List<Integer> filterForMinInteger(List<BlockMetaData> rowGroups, String columnName, int minValue) {
        List<Integer> candidates = new ArrayList<>();
        for (int i = 0; i < rowGroups.size(); i++) {
            if (!canSkipMinInteger(rowGroups.get(i), columnName, minValue)) {
                candidates.add(i);
            }
        }
        return candidates;
    }

    /**
     * Decodes hex strings returned by Parquet minAsString() / maxAsString() if applicable.
     */
    public static String cleanString(String statValue) {
        if (statValue == null) return null;
        if (statValue.startsWith("0x")) {
            try {
                String hex = statValue.substring(2);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < hex.length(); i += 2) {
                    int val = Integer.parseInt(hex.substring(i, Math.min(i + 2, hex.length())), 16);
                    sb.append((char) val);
                }
                return sb.toString();
            } catch (Exception e) {
                return statValue;
            }
        }
        return statValue;
    }
}
