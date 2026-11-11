/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.IOException;

/**
 * Where a shard's data lives in the store, and the write log and segment publisher over it, keyed by the index
 * incarnation. Split out of {@link MetadataPlane}, which keeps the public entry points.
 */
final class ShardDataPaths {

    private final BlobStore blobStore;
    private final BlobPath base;
    private final DescriptorStore descriptors;

    ShardDataPaths(BlobStore blobStore, BlobPath base, DescriptorStore descriptors) {
        this.blobStore = blobStore;
        this.base = base;
        this.descriptors = descriptors;
    }

    /**
     * Returns the write-ahead log for one shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the WAL store
     */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, int shardId) throws IOException {
        return walStore(indexName, uuidOf(indexName), shardId);
    }

    /**
     * Returns the write-ahead log for one shard, for a caller that already knows the index's uuid.
     *
     * @param indexName the index
     * @param uuid the index's uuid
     * @param shardId the shard number
     * @return the WAL store
     */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, String uuid, int shardId) {
        return new org.opensearch.serverless.store.WalStore(blobStore, RegisterMap.shardData(base, indexName, uuid, shardId));
    }

    /**
     * Returns where one shard's bytes live.
     *
     * <p>Resolving the uuid costs a descriptor read, which is why every caller on a hot path takes the
     * overload that supplies one. This exists for the maintenance paths, where a read is already happening
     * and one more is not the cost that matters.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the container path
     * @throws IOException if the descriptor cannot be read
     */
    public BlobPath shardData(String indexName, int shardId) throws IOException {
        return RegisterMap.shardData(base, indexName, uuidOf(indexName), shardId);
    }

    String uuidOf(String indexName) throws IOException {
        return descriptors.get(indexName)
            .orElseThrow(() -> new IOException("no such index: " + indexName + "; its storage cannot be located without its uuid"))
            .uuid();
    }

    /**
     * Returns the segment publisher for one shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the publisher
     */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, int shardId) throws IOException {
        return segmentPublisher(indexName, uuidOf(indexName), shardId);
    }

    /**
     * Returns the segment publisher for one shard, for a caller that already knows the index's uuid.
     *
     * @param indexName the index
     * @param uuid the index's uuid
     * @param shardId the shard number
     * @return the publisher
     */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, String uuid, int shardId) {
        return new org.opensearch.serverless.store.SegmentPublisher(blobStore, RegisterMap.shardData(base, indexName, uuid, shardId));
    }
}
