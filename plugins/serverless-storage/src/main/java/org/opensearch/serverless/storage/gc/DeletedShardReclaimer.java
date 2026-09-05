/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;

import java.io.IOException;

/**
 * Reclaims one deleted shard's object-store footprint, or explains why it must not.
 *
 * <h2>The hole this closes</h2>
 *
 * Deleting an index used to reclaim nothing at all from object storage. The index-removal hook released the
 * clone pin, deregistered the WAL entry, deleted the <em>local disk cache</em> and cancelled a scheduler
 * task -- and left every manifest, every bundle, the shard-head register and the pin register in place
 * forever. Garbage collection could not clean up after it either: GC is per-shard and owned by the shard's
 * reader engine, so once the index is gone there is no engine left to ever sweep it. For the shape of usage
 * this plugin is built for -- many short-lived indices -- that is 100% of the storage footprint, retained
 * permanently, growing monotonically.
 *
 * <h2>Why this cannot simply be a recursive delete</h2>
 *
 * A zero-copy clone's segments physically live in its source's bundles. Deleting a source's prefix because
 * its index was deleted would destroy a live clone's data -- which is safe today only by accident, because
 * nothing deletes anything. So the reclaim is conditional on the same signal the sweep already trusts: a
 * live durable pin. A clone pins every hop of its lineage ({@code ShardCloner#clone}), a snapshot pins what
 * it names, and PITR pins its window; so "no live pin on this shard" is precisely "nothing outside this
 * shard is depending on its bytes". That is a stronger check than scanning every other index's lineage
 * records for a reference, and a far cheaper one -- it is a single register read.
 *
 * <p>Expired pins do not block: an expiry is the mechanism by which an abandoned operation stops holding
 * storage, and honouring it here is the same reading {@code DurablePinRegistry#getPinnedManifestIds} already
 * takes. A pin register that cannot be read blocks -- an unreadable shard is never assumed to be free.
 */
public final class DeletedShardReclaimer {

    private DeletedShardReclaimer() {}

    /** What one reclaim attempt did, so a caller can log the difference between "nothing to do" and "refused". */
    public enum Outcome {
        /** The shard's whole prefix was deleted. */
        RECLAIMED,
        /** At least one live pin still names a generation of this shard, so something still depends on its bytes. */
        SKIPPED_PINNED
    }

    /**
     * Deletes everything under one deleted shard's own container, unless something still pins it.
     *
     * <p>Uses the container's own recursive delete rather than enumerating manifests and bundles: the index
     * is gone, so there is no surviving reader of the head register, the pin register, the lineage record or
     * the sweep state either, and deleting them one prefix at a time would be several listings to accomplish
     * exactly the same thing. This is the one moment in this plugin's life cycle where "delete all of it" is
     * the correct instruction rather than a dangerous one -- guarded by the pin check above it.
     *
     * <p>Idempotent: the index-removal hook this is called from fans out across nodes and may run more than
     * once, and deleting an already-deleted prefix is not an error.
     *
     * @param indexUuid the deleted index.
     * @param shardId the shard within it.
     * @param shardContainer that shard's own container.
     * @param nowMillis the instant pin expiry is judged against.
     * @return whether the prefix was deleted or deliberately left alone.
     * @throws IOException if the pin register or the delete itself fails -- the caller decides whether that
     *                     is worth retrying, but nothing partial is left in a state that needs repair, since
     *                     every blob here is unreachable once the index is gone.
     */
    public static Outcome reclaimShard(String indexUuid, int shardId, BlobContainer shardContainer, long nowMillis) throws IOException {
        for (PinRecord pin : new BlobContainerDurablePinRegistry(shardContainer).getPins(indexUuid, shardId)) {
            if (pin.isLiveAt(nowMillis)) {
                return Outcome.SKIPPED_PINNED;
            }
        }
        shardContainer.delete();
        return Outcome.RECLAIMED;
    }
}
