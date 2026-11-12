/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.bucket;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Answers bucket membership from index descriptors, and which members a node holds open from that node's own shards.
 *
 * <p>A descriptor names its bucket, so every node answers alike from what it reads anyway to route the shard. The answer
 * is kept for an hour per index incarnation, because a node checks the heads of what it holds every few minutes and a
 * descriptor read per member per check would cost more than the bucket saves. An index's bucket does not change for an
 * incarnation except by promotion out of it, which {@link #forget}s it; a recreated index is told apart by its uuid
 * wherever a caller knows it. Nodes sharing one metadata plane (in one process, as tests run them) each register their open
 * shards under their own id.
 */
public final class DescriptorBucketHeads implements BucketHeads {

    /** Reads a descriptor the way routing does, from a recent read when there is one. */
    @FunctionalInterface
    public interface Descriptors {
        /**
         * Returns an index's descriptor.
         *
         * @param indexName the index
         * @return its descriptor, or empty if there is none
         * @throws IOException if it cannot be read
         */
        Optional<IndexDescriptor> describe(String indexName) throws IOException;
    }

    /** How long a member's bucket is answered from an earlier read. */
    public static final long MEMBERSHIP_CACHE_MILLIS = 3_600_000L;

    private record Known(String uuid, String bucketId, long readAtMillis) {
    }

    private final Descriptors descriptors;
    private final LongSupplier clock;
    private final Map<String, Supplier<Collection<ShardId>>> openByNode = new ConcurrentHashMap<>();
    private final Map<String, Known> known = new ConcurrentHashMap<>();

    /**
     * Creates a resolver.
     *
     * @param descriptors where descriptors are read
     * @param clock the clock the cache ages by
     */
    public DescriptorBucketHeads(Descriptors descriptors, LongSupplier clock) {
        this.descriptors = descriptors;
        this.clock = clock;
    }

    /**
     * Records an index's bucket from a descriptor a caller has just read, so the head operation that follows costs no
     * second read.
     *
     * @param descriptor the descriptor
     */
    public void note(IndexDescriptor descriptor) {
        known.put(descriptor.name(), new Known(descriptor.uuid(), descriptor.bucket(), clock.getAsLong()));
    }

    /**
     * Drops what is known of an index's bucket, for an index deleted or moved out of its bucket.
     *
     * @param indexName the index
     */
    public void forget(String indexName) {
        known.remove(indexName);
    }

    /**
     * Registers what a node holds open.
     *
     * @param nodeId the node
     * @param openShards its open shards, read on each release of a member
     * @return this
     */
    public DescriptorBucketHeads register(String nodeId, Supplier<Collection<ShardId>> openShards) {
        openByNode.put(nodeId, openShards);
        return this;
    }

    @Override
    public Optional<Member> bucketOf(String indexName, int shardId) throws IOException {
        return bucketOf(indexName, shardId, null);
    }

    @Override
    public Optional<Member> bucketOf(String indexName, int shardId, String indexUuid) throws IOException {
        if (shardId != 0 || indexName.startsWith("#")) {
            return Optional.empty();
        }
        final long now = clock.getAsLong();
        Known entry = known.get(indexName);
        if (entry == null
            || now - entry.readAtMillis() >= MEMBERSHIP_CACHE_MILLIS
            || (indexUuid != null && indexUuid.equals(entry.uuid()) == false)) {
            final Optional<IndexDescriptor> descriptor = descriptors.describe(indexName);
            if (descriptor.isEmpty()) {
                known.remove(indexName);
                return Optional.empty();
            }
            entry = new Known(descriptor.get().uuid(), descriptor.get().bucket(), now);
            known.put(indexName, entry);
        }
        if (entry.bucketId() == null || (indexUuid != null && indexUuid.equals(entry.uuid()) == false)) {
            return Optional.empty();
        }
        return Optional.of(new Member(entry.bucketId(), entry.uuid()));
    }

    @Override
    public boolean othersOpenOn(String bucketId, String nodeId, String exceptIndexName) throws IOException {
        final Supplier<Collection<ShardId>> open = openByNode.get(nodeId);
        if (open == null) {
            return false;
        }
        for (ShardId shardId : open.get()) {
            final String name = shardId.getIndexName();
            if (shardId.id() == 0 && name.equals(exceptIndexName) == false) {
                final Optional<Member> member = bucketOf(name, 0, shardId.getIndex().getUUID());
                if (member.isPresent() && member.get().bucketId().equals(bucketId)) {
                    return true;
                }
            }
        }
        return false;
    }
}
