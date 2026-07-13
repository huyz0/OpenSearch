/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

/**
 * Everything a split-target reader shard needs to schedule its own {@link
 * PartitionRewriteSchedulerTask}, bundled into one value so background partition rewrite can be
 * threaded through as a single optional (nullable) constructor parameter -- same shape as {@link
 * org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig}. {@code null} where this
 * type is accepted means "background partition rewrite is not configured for this shard" (either
 * the node setting that controls the interval is unset/zero, or this shard was never a split
 * target at all).
 */
public record PartitionRewriteSchedulerConfig(TimeValue interval, ShardStateStore shardStateStore, BlobContainerManifestStore manifestStore,
    ObjectStoreCommitMaterializer materializer, ObjectStoreCommitPublisher commitPublisher,
    BlobContainerShardPartitionStore partitionStore) {

    /**
     * Creates a config bundling everything needed to schedule background partition rewrite for one shard.
     *
     * @param interval        delay between successive rewrite ticks
     * @param shardStateStore store used to read the shard's live head
     * @param manifestStore   store used to read the manifest at the shard's currently published generation
     * @param materializer    materializes the pre-rewrite manifest's segments into a real Lucene directory
     * @param commitPublisher publishes the filtered result as a new commit manifest
     * @param partitionStore  reads (and, on success, clears) this shard's {@link ShardPartitionDescriptor}
     */
    public PartitionRewriteSchedulerConfig {
    }

    /** Delay between successive rewrite ticks. */
    @Override
    public TimeValue interval() {
        return interval;
    }

    /** Store used to read the shard's live head. */
    @Override
    public ShardStateStore shardStateStore() {
        return shardStateStore;
    }

    /** Store used to read the manifest at the shard's currently published generation. */
    @Override
    public BlobContainerManifestStore manifestStore() {
        return manifestStore;
    }

    /** Materializes the pre-rewrite manifest's segments into a real Lucene directory. */
    @Override
    public ObjectStoreCommitMaterializer materializer() {
        return materializer;
    }

    /** Publishes the filtered result as a new commit manifest. */
    @Override
    public ObjectStoreCommitPublisher commitPublisher() {
        return commitPublisher;
    }

    /** Reads (and, on success, clears) this shard's {@link ShardPartitionDescriptor}. */
    @Override
    public BlobContainerShardPartitionStore partitionStore() {
        return partitionStore;
    }
}
