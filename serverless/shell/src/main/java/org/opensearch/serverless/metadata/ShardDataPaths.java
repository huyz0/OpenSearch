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
    private final org.opensearch.serverless.bucket.BucketHeads bucketHeads;
    private final org.opensearch.serverless.bucket.BucketCommits bucketCommits;

    ShardDataPaths(
        BlobStore blobStore,
        BlobPath base,
        DescriptorStore descriptors,
        org.opensearch.serverless.bucket.BucketHeads bucketHeads,
        org.opensearch.serverless.bucket.BucketCommits bucketCommits
    ) {
        this.blobStore = blobStore;
        this.base = base;
        this.descriptors = descriptors;
        this.bucketHeads = bucketHeads;
        this.bucketCommits = bucketCommits;
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
        final var publisher = new org.opensearch.serverless.store.SegmentPublisher(
            blobStore,
            RegisterMap.shardData(base, indexName, uuid, shardId)
        );
        return publisher.commitRegister(new MemberAwareRegister(publisher.commitRegister(), indexName, uuid, shardId));
    }

    /**
     * A shard's commit register, resolved on first use: its bucket's entry for a member, its own register otherwise.
     *
     * <p>Resolved lazily so making a publisher reads nothing; the answer comes from the descriptor the bucket resolver
     * already caches, and is kept for the publisher's life, which ends when the shard closes, as it does on a move.
     */
    private final class MemberAwareRegister implements org.opensearch.serverless.store.CommitRegister {
        private final org.opensearch.serverless.store.CommitRegister own;
        private final String indexName;
        private final String uuid;
        private final int shardId;
        private volatile org.opensearch.serverless.store.CommitRegister resolved;

        MemberAwareRegister(org.opensearch.serverless.store.CommitRegister own, String indexName, String uuid, int shardId) {
            this.own = own;
            this.indexName = indexName;
            this.uuid = uuid;
            this.shardId = shardId;
        }

        private org.opensearch.serverless.store.CommitRegister resolve() throws IOException {
            org.opensearch.serverless.store.CommitRegister r = resolved;
            if (r == null) {
                final var member = bucketHeads.bucketOf(indexName, shardId, uuid);
                r = member.isPresent() ? bucketCommits.member(member.get().bucketId(), member.get().indexUuid()) : own;
                resolved = r;
            }
            return r;
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> read() throws IOException {
            return resolve().read();
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwap(
            long expectedGeneration,
            org.opensearch.core.common.bytes.BytesReference value
        ) throws IOException {
            return resolve().compareAndSwap(expectedGeneration, value);
        }

        @Override
        public String toString() {
            return resolved == null ? "unresolved[" + indexName + "#" + shardId + "]" : resolved.toString();
        }
    }
}
