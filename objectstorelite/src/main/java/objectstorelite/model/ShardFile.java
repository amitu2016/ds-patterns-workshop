package objectstorelite.model;

import java.nio.file.Path;

/**
 * References an on-disk shard file with its shard index.
 */
public record ShardFile(int shardIndex, Path path) {
}
