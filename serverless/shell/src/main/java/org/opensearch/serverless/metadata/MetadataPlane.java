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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.membership.BlobLeaseMembership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * The whole truth layer behind one object: descriptors, shard-heads and node leases.
 *
 * <p>Nothing here consults a cluster-manager, and nothing here is published. Index creation is one
 * put-if-absent; shard activation is one compare-and-swap; membership is a listing. The register map is
 * in {@link RegisterMap}, and the reason it is a map rather than a single object is in that class's
 * documentation.
 *
 * <p><b>Decision D5:</b> everything here is exercised against {@code FsBlobContainer} only. Passing
 * there says nothing about whether a provider's conditional write is genuinely linearizable, which is
 * R11 and the thing the entire safety argument rests on. No durability claim is made for S3, GCS or
 * Azure until that conformance suite runs.
 */
public final class MetadataPlane {

    private final DescriptorStore descriptors;
    private final ShardHeadStore heads;
    private final BlobLeaseMembership membershipField;
    private final boolean nodeLeaseLiveness;
    private final BlobStore blobStore;
    private final BlobPath base;

    /**
     * Creates a metadata plane over a blob store.
     *
     * @param blobStore the backing store
     * @param base the deployment's base path within it
     * @param clock source of wall-clock millis
     * @param leaseTtlMillis lease duration for shard-heads and node leases
     */
    public MetadataPlane(BlobStore blobStore, BlobPath base, LongSupplier clock, long leaseTtlMillis) {
        this(blobStore, base, clock, leaseTtlMillis, false);
    }

    /**
     * Creates a metadata plane, optionally with §7's batched liveness.
     *
     * <p>With {@code nodeLeaseLiveness}, a shard-head is held for as long as its owner's node lease is,
     * so a node renews once rather than once per shard. Phase 8 measured the difference this makes:
     * the per-shard renewal was the entire steady-state write cost.
     *
     * @param blobStore the backing store
     * @param base the deployment's base path within it
     * @param clock source of wall-clock millis
     * @param leaseTtlMillis lease duration
     * @param nodeLeaseLiveness true to derive shard liveness from node leases
     */
    public MetadataPlane(BlobStore blobStore, BlobPath base, LongSupplier clock, long leaseTtlMillis, boolean nodeLeaseLiveness) {
        this.nodeLeaseLiveness = nodeLeaseLiveness;
        this.descriptors = new DescriptorStore(blobStore.blobContainer(RegisterMap.indices(base)));
        final BlobLeaseMembership leases = new BlobLeaseMembership(
            blobStore.blobContainer(RegisterMap.members(base)),
            clock,
            leaseTtlMillis
        );
        final LivenessOracle oracle = nodeLeaseLiveness ? (nodeId, ephemeralId) -> {
            try {
                final var lease = leases.read(nodeId);
                // An ephemeral id that has moved on means the process that took the shard is gone, even
                // though a node with the same name is back. It does not inherit the claim.
                return lease.isPresent()
                    && lease.get().isExpiredAt(clock.getAsLong()) == false
                    && (ephemeralId == null || ephemeralId.equals(lease.get().ephemeralId()));
            } catch (java.io.IOException e) {
                // Unreadable is not "dead": refusing to acquire is the safe answer, since the cost is a
                // retry and the cost of the alternative is two writers.
                return true;
            }
        } : null;
        this.heads = new ShardHeadStore(blobStore.blobContainer(RegisterMap.shards(base)), clock, leaseTtlMillis, oracle);
        this.membershipField = leases;
        this.blobStore = blobStore;
        this.base = base;
    }

    /**
     * Reports whether shard liveness is derived from node leases.
     *
     * @return true when §7's batching is in effect
     */
    public boolean usesNodeLeaseLiveness() {
        return nodeLeaseLiveness;
    }

    /**
     * Returns the write-ahead log for one shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the WAL store
     */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, int shardId) {
        return new org.opensearch.serverless.store.WalStore(blobStore, RegisterMap.shardData(base, indexName, shardId));
    }

    /**
     * Returns the segment publisher for one shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the publisher
     */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, int shardId) {
        return new org.opensearch.serverless.store.SegmentPublisher(blobStore, RegisterMap.shardData(base, indexName, shardId));
    }

    /**
     * Creates an index. One put-if-absent, whose cost does not depend on how many indices already exist.
     *
     * @param descriptor the index to create
     * @return the descriptor register's generation
     * @throws IndexAlreadyExistsException if the name is taken
     * @throws IOException if the write fails
     */
    public long createIndex(IndexDescriptor descriptor) throws IOException {
        return descriptors.create(descriptor);
    }

    /**
     * Deletes an index and every shard-head belonging to it.
     *
     * <p>Heads first, then the descriptor. The order matters and the reason is worth stating: deleting
     * the descriptor first would leave heads that no reader can interpret, since the shard count needed
     * to enumerate them lives in the descriptor. Orphaned heads with no descriptor are the kind of
     * garbage that is discovered years later.
     *
     * @param indexName the index to delete
     * @return true if the index existed
     * @throws IOException if the delete fails
     */
    public boolean deleteIndex(String indexName) throws IOException {
        final long generation = descriptors.generationOf(indexName);
        final Optional<IndexDescriptor> descriptor = descriptors.get(indexName);
        if (descriptor.isEmpty()) {
            return false;
        }
        // Order the delete against any concurrent lifecycle change before removing anything. If the
        // descriptor moved underneath us, someone else is mid-operation and this delete loses.
        if (descriptors.deleteIfUnchanged(indexName, generation) == false) {
            return false;
        }
        // Heads only after the descriptor is ordered away: the shard count needed to enumerate them
        // lives in the descriptor, and it is bounded by IndexDescriptor.MAX_SHARDS, so this is a
        // bounded loop rather than a listing.
        heads.deleteAllFor(indexName, descriptor.get().numberOfShards());
        return true;
    }

    /**
     * Reads an index descriptor.
     *
     * @param indexName the index
     * @return the descriptor, or empty
     * @throws IOException if the read fails
     */
    public Optional<IndexDescriptor> describe(String indexName) throws IOException {
        return descriptors.get(indexName);
    }

    /**
     * Attempts to take ownership of a shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the acquiring node
     * @param ephemeralId the acquiring node's ephemeral id
     * @return the outcome, carrying the winner's head either way
     * @throws IOException if the register cannot be read or written
     */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId) throws IOException {
        final Acquisition acquisition = heads.acquire(indexName, shardId, nodeId, ephemeralId);
        if (acquisition.acquired()) {
            // Record the claim so this node can find it again without reading the world. Written after
            // the compare-and-swap, never before: if the process dies in between, the node simply does
            // not see the shard on its next read and re-acquires, which self-heals. Writing it first
            // would instead advertise a claim that was never won.
            blobStore.blobContainer(RegisterMap.assignments(base, nodeId))
                .writeBlob(RegisterMap.assignmentBlob(indexName, shardId), new java.io.ByteArrayInputStream(new byte[0]), 0L, false);
        }
        return acquisition;
    }

    /**
     * Forgets a node's claim on a shard. Idempotent, and safe to skip — a stale entry costs one wasted
     * head read, because every entry is verified before it is believed.
     *
     * @param nodeId the node
     * @param indexName the index
     * @param shardId the shard number
     * @throws IOException if the delete fails
     */
    public void forgetAssignment(String nodeId, String indexName, int shardId) throws IOException {
        blobStore.blobContainer(RegisterMap.assignments(base, nodeId))
            .deleteBlobsIgnoringIfNotExists(java.util.List.of(RegisterMap.assignmentBlob(indexName, shardId)));
    }

    /**
     * Reads everything one node needs in order to serve.
     *
     * <p>This is the loop that replaces cluster-state publication. It returns only the indices this node
     * owns a shard of, so a node's residency tracks its working set rather than the size of the world.
     *
     * @param nodeId the node to read truth for
     * @return that node's descriptors and shard assignments
     * @throws IOException if the object store cannot be read
     */
    public Truth truthFor(String nodeId) throws IOException {
        final Map<String, IndexDescriptor> hosted = new LinkedHashMap<>();
        final List<ShardAssignment> assignments = new ArrayList<>();

        // One listing of this node's own claims, rather than a sweep of every index in the deployment.
        // Phase 9 measured the difference: the old form cost 2N+1 operations in the population, on the
        // steady-state path, on every node -- the exact O(population) shape this design exists to remove.
        for (String claim : blobStore.blobContainer(RegisterMap.assignments(base, nodeId)).listBlobs().keySet()) {
            final int separator = claim.lastIndexOf(RegisterMap.SHARD_SEPARATOR);
            if (separator < 0) {
                continue;
            }
            final String indexName = claim.substring(0, separator);
            final int shard;
            try {
                shard = Integer.parseInt(claim.substring(separator + RegisterMap.SHARD_SEPARATOR.length()));
            } catch (NumberFormatException e) {
                continue;
            }

            // The claim is only a hint. The head decides, so a stale entry costs one read and is then
            // ignored -- which is what makes it safe for the listing to lag reality.
            final Optional<ShardHead> head = heads.read(indexName, shard);
            if (head.isEmpty() || nodeId.equals(head.get().ownerNodeId()) == false) {
                continue;
            }
            final Optional<IndexDescriptor> descriptor = hosted.containsKey(indexName)
                ? Optional.of(hosted.get(indexName))
                : descriptors.get(indexName);
            if (descriptor.isEmpty()) {
                // The index was deleted underneath us. Not an error: the shard is going away too.
                continue;
            }
            // The term comes from the head, which is the one monotonic number every node touching this
            // shard agrees on. Never a per-node counter -- see s1-findings.md.
            assignments.add(new ShardAssignment(indexName, shard, head.get().term()));
            hosted.put(indexName, descriptor.get());
        }
        return new Truth(hosted.values(), assignments);
    }

    /**
     * Returns the backing blob store, for components that address shard data directly.
     *
     * @return the blob store
     */
    public BlobStore blobStore() {
        return blobStore;
    }

    /**
     * Returns the deployment's base path.
     *
     * @return the base path
     */
    public BlobPath basePath() {
        return base;
    }

    /**
     * Returns the descriptor store.
     *
     * @return the descriptor store
     */
    public DescriptorStore descriptors() {
        return descriptors;
    }

    /**
     * Returns the shard-head store.
     *
     * @return the shard-head store
     */
    public ShardHeadStore heads() {
        return heads;
    }

    /**
     * Returns the lease-backed membership source.
     *
     * @return the membership source
     */
    public BlobLeaseMembership membership() {
        return membershipField;
    }
}
