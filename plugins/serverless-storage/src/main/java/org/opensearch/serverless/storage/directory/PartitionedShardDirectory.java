/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import java.util.List;
import java.util.Optional;

/**
 * A {@link ShardDirectory} that fans a shard out to one of several backing {@link ShardDirectory}
 * partitions by consistent-hashing {@code (indexUuid, shardId)}, instead of holding every entry in
 * one instance. This is the tested foundation the target design's "tens of directory nodes,
 * DynamoDB-style" (rfc-serverless-metadata-plane.md &sect;8/&sect;9/&sect;11) would be built from:
 * a fixed shard key always resolves to the same partition, so a client that agrees on the same
 * partition count and ring can compute where to look without asking anyone.
 *
 * <p>What this class is explicitly <em>not</em>: the partitions here are plain in-process {@link
 * ShardDirectory} instances (typically {@link InMemoryShardDirectory}s), not connections to
 * separate directory-node processes over the network. There is no replication (losing one
 * partition's process loses every entry it held, with no fallback), no dynamic membership (the
 * partition count is fixed at construction -- see {@link ConsistentHashRing}'s own caveat about
 * resizing), and no cross-node protocol at all. Turning this into the real target design means
 * replacing each in-process partition with a client that talks to one of several remote directory
 * nodes over the network, while reusing this class's partition-selection logic unchanged.
 */
public final class PartitionedShardDirectory implements ShardDirectory {

    private static final int VIRTUAL_NODES_PER_PARTITION = 64;

    private final List<ShardDirectory> partitions;
    private final ConsistentHashRing ring;

    /**
     * Creates a directory that fans shards out across the given partitions by consistent hashing.
     *
     * @param partitions the backing directory partitions, must be non-empty
     */
    public PartitionedShardDirectory(List<ShardDirectory> partitions) {
        if (partitions.isEmpty()) {
            throw new IllegalArgumentException("must have at least one partition");
        }
        this.partitions = List.copyOf(partitions);
        this.ring = new ConsistentHashRing(this.partitions.size(), VIRTUAL_NODES_PER_PARTITION);
    }

    /** Which partition index a given shard's entries live in -- exposed for tests/metrics, not routing logic. */
    int partitionIndexFor(String indexUuid, int shardId) {
        return ring.bucketFor(ringKey(indexUuid, shardId));
    }

    private ShardDirectory partitionFor(String indexUuid, int shardId) {
        return partitions.get(partitionIndexFor(indexUuid, shardId));
    }

    private static String ringKey(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    @Override
    public Optional<ShardDirectoryEntry> lookup(String indexUuid, int shardId) {
        return partitionFor(indexUuid, shardId).lookup(indexUuid, shardId);
    }

    @Override
    public void report(String indexUuid, int shardId, ShardDirectoryEntry entry) {
        partitionFor(indexUuid, shardId).report(indexUuid, shardId, entry);
    }

    @Override
    public void drop(String indexUuid, int shardId) {
        partitionFor(indexUuid, shardId).drop(indexUuid, shardId);
    }

    @Override
    public void dropIfMatches(String indexUuid, int shardId, ShardDirectoryEntry expectedEntry) {
        partitionFor(indexUuid, shardId).dropIfMatches(indexUuid, shardId, expectedEntry);
    }
}
