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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Takes a shard for a node and records the claim, and reports what the store says a node owns. Split out of
 * {@link MetadataPlane}, which keeps the public entry points.
 */
final class ShardActivation {

    private final BlobStore blobStore;
    private final BlobPath base;
    private final ShardHeadStore heads;
    private final DescriptorStore descriptors;
    private final MetadataPlane plane;

    ShardActivation(BlobStore blobStore, BlobPath base, ShardHeadStore heads, DescriptorStore descriptors, MetadataPlane plane) {
        this.blobStore = blobStore;
        this.base = base;
        this.heads = heads;
        this.descriptors = descriptors;
        this.plane = plane;
    }

    /**
     * Attempts to take ownership of a shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the acquiring node
     * @param ephemeralId the acquiring node's ephemeral id
     * @return the outcome, carrying the winner's head either way
     * @throws IOException if the register cannot be read or created
     */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId) throws IOException {
        return activate(indexName, shardId, nodeId, ephemeralId, null);
    }

    /**
     * Attempts to take ownership of a shard of one incarnation of an index.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the acquiring node
     * @param ephemeralId the acquiring node's ephemeral id
     * @param indexUuid the uuid of the index being activated
     * @return the outcome, carrying the winner's head either way
     * @throws IOException if the register cannot be read or created
     */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId, String indexUuid) throws IOException {
        return activate(indexName, shardId, nodeId, ephemeralId, indexUuid, false);
    }

    /**
     * Attempts to take ownership of a shard of one incarnation of an index, saying which write log the taker writes.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the acquiring node
     * @param ephemeralId the acquiring node's ephemeral id
     * @param indexUuid the uuid of the index being activated
     * @param writesNodeLog whether the acquiring node writes its node's log rather than the shard's own; see
     *     {@link ShardHead#withShardLogQuietSince}
     * @return the outcome, carrying the winner's head either way
     * @throws IOException if the register cannot be read or created
     */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId, String indexUuid, boolean writesNodeLog)
        throws IOException {
        final Acquisition acquisition = heads.acquire(indexName, shardId, nodeId, ephemeralId, indexUuid, writesNodeLog);
        if (acquisition.acquired()) {
            // Seal the log here, at the swap, rather than leaving it to whoever opens the shard.
            //
            // The seal is what stops a predecessor that has not yet noticed it lost this shard from
            // having its later appends replayed as acknowledged history. It used to be taken when the
            // shard was opened, which is several object-store round trips and a whole shard creation
            // after the moment ownership actually moved -- and every one of the predecessor's appends
            // inside that window fell in front of the cutoff and replayed. Taking it here narrows the
            // window to the one blob write below: from the swap that transferred ownership to the seal
            // that records where the predecessor's history ended.
            //
            // The open path still seals, and must: a shard released locally and reopened at the same
            // term never passes through here. That second seal cannot loosen this one, because seals
            // merge by taking the lowest recorded position per term -- see WalStore#sealAt -- so the
            // tighter bound taken here is the one every later recovery inherits. The cost is one extra
            // seal per acquisition, on a path that runs at failover rather than per write.
            //
            // Only when the uuid is known, because that is what locates the shard's bytes. A caller
            // that acquires without one -- the overload that skips the incarnation check -- gets the
            // old behaviour, with the open path's seal as the bound.
            // Not for a head whose shard log is quiet: every term of it is closed already, and fencing them again is a
            // listing per term to find so.
            if (acquisition.head().indexUuid() != null && acquisition.head().shardLogQuiet() == false) {
                try {
                    plane.walStore(indexName, acquisition.head().indexUuid(), shardId).fenceOlderTerms(acquisition.head().term());
                } catch (IOException e) {
                    // A head won and a log unsealed is worse than no head at all: this node would go on
                    // to open the shard and seal late, which is the window this exists to close. Give
                    // the head back at the term just won and let the caller see the failure, exactly as
                    // the unwritten claim below does.
                    try {
                        heads.release(indexName, shardId, nodeId, acquisition.head().term());
                    } catch (IOException release) {
                        e.addSuppressed(release);
                    }
                    throw e;
                }
            }
            // Record the claim so this node can find it again without reading the world. Written after
            // the compare-and-swap, never before: if the process dies in between, the node simply does
            // not see the shard on its next read and re-acquires, which self-heals. Writing it first
            // would instead advertise a claim that was never won.
            try {
                blobStore.blobContainer(RegisterMap.assignments(base, nodeId))
                    .writeBlob(RegisterMap.assignmentBlob(indexName, shardId), new java.io.ByteArrayInputStream(new byte[0]), 0L, false);
            } catch (IOException e) {
                // A head won and a claim not written is a shard nobody serves: this node never lists it,
                // and every other node's acquire finds it held by a live owner for as long as the lease
                // lasts. Give the head back at the term this acquisition won, so it is acquirable again
                // -- by anyone, this node included -- and let the caller see the failure.
                try {
                    heads.release(indexName, shardId, nodeId, acquisition.head().term());
                } catch (IOException release) {
                    e.addSuppressed(release);
                }
                throw e;
            }
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
        return truthFor(nodeId, null);
    }

    /**
     * Lists every node that has, or once had, claims on shards -- live or long gone.
     *
     * @return node ids
     * @throws IOException if the store cannot be listed
     */
    public List<String> nodesWithClaims() throws IOException {
        return new ArrayList<>(blobStore.blobContainer(base.add("assignments")).children().keySet());
    }

    /**
     * Lists the shards a node has claimed -- one listing, no head reads. A claim is only a hint: the head decides,
     * and a caller acting on one reads the head first.
     *
     * @param nodeId the node
     * @return index and shard number of each claim
     * @throws IOException if the claims cannot be listed
     */
    public List<Map.Entry<String, Integer>> claimsOf(String nodeId) throws IOException {
        final List<Map.Entry<String, Integer>> claims = new ArrayList<>();
        for (String claim : blobStore.blobContainer(RegisterMap.assignments(base, nodeId)).listBlobs().keySet()) {
            final int separator = claim.lastIndexOf(RegisterMap.SHARD_SEPARATOR);
            if (separator < 0) {
                continue;
            }
            try {
                final String indexName = claim.substring(0, separator);
                final int shard = Integer.parseInt(claim.substring(separator + RegisterMap.SHARD_SEPARATOR.length()));
                if (Names.isValidIndexOrAlias(indexName)) {
                    claims.add(Map.entry(indexName, shard));
                }
            } catch (NumberFormatException e) {
                // Not a claim this system writes.
            }
        }
        return claims;
    }

    /**
     * Reads what one incarnation of a node owns, as the heads say.
     *
     * <p>A head is inherited only when its owner's ephemeral id is this incarnation's. A node restarted
     * with the same id used to inherit every head its previous incarnation held -- and serve them at the
     * old term while every peer read those heads as dead and took them at the next. The heads a restart
     * leaves behind are re-acquired through the ordinary path, which bumps the term and seals the log.
     *
     * @param nodeId the node
     * @param ephemeralId this incarnation's ephemeral id, or null to inherit by node id alone
     * @return the descriptors to know about and the shards to serve
     * @throws IOException if a register cannot be read
     */
    public Truth truthFor(String nodeId, String ephemeralId) throws IOException {
        final Map<String, IndexDescriptor> hosted = new LinkedHashMap<>();
        final List<ShardAssignment> assignments = new ArrayList<>();

        // One listing of this node's own claims, rather than a sweep of every index in the deployment.
        // Measured: a sweep cost 2N+1 operations in the population, on the
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
            if (ephemeralId != null
                && head.get().ownerEphemeralId() != null
                && ephemeralId.equals(head.get().ownerEphemeralId()) == false) {
                // A previous incarnation's head. Not ours to serve at its term; see the javadoc.
                continue;
            }
            if (Names.isValidIndexOrAlias(indexName) == false) {
                // A stray object in this node's assignments container -- a console upload, a scratch file --
                // used to abort the whole read with the validator's exception, and with it every shard this
                // node serves. Skipped, and named once in the log.
                MetadataPlane.LOGGER.warn("ignoring [{}] in the assignments of {}: not an index name this system writes", claim, nodeId);
                continue;
            }
            final Optional<IndexDescriptor> descriptor;
            try {
                descriptor = hosted.containsKey(indexName) ? Optional.of(hosted.get(indexName)) : descriptors.get(indexName);
            } catch (IllegalArgumentException e) {
                // Belt and braces behind the name check above: whatever the descriptor store refuses about
                // this one claim is that claim's problem, not the resync's.
                MetadataPlane.LOGGER.warn("ignoring the claim [{}] of {}: {}", claim, nodeId, e.getMessage());
                continue;
            }
            if (descriptor.isEmpty()) {
                // The index was deleted underneath us. Not an error: the shard is going away too.
                continue;
            }
            if (head.get().indexUuid() != null && head.get().indexUuid().equals(descriptor.get().uuid()) == false) {
                // The index was deleted and recreated underneath us; the head is the old incarnation's.
                continue;
            }
            // The term comes from the head, which is the one monotonic number every node touching this
            // shard agrees on. Never a per-node counter, which can make the update be silently ignored.
            assignments.add(new ShardAssignment(indexName, shard, head.get().term()));
            hosted.put(indexName, descriptor.get());
        }
        return new Truth(hosted.values(), assignments);
    }
}
