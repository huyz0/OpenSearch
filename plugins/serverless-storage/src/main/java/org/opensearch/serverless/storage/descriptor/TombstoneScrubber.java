/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Reclaims tombstones once they are older than the retention window.
 *
 * <h2>Why a tombstone can be reclaimed at all</h2>
 *
 * A tombstone exists so that a node partitioned during a delete consults the descriptor, finds the deletion
 * recorded, and drops its local shard data instead of keeping it. It is not what stops a deleted index being
 * served: absence already does that, since a gated shard is only ever opened through a successful descriptor
 * lookup. So the window a tombstone has to cover is the longest a node can be partitioned and still rejoin
 * carrying that data, and past that it is dead weight that would otherwise accumulate for the life of the
 * cluster.
 *
 * <h2>The tombstone's own content is the only authority</h2>
 *
 * Nothing here consults an index of what to delete. The worklist is the keyspace, so there is nothing that
 * can fall out of step with it and nothing it can lose track of, which is what makes "no missed cleanup" a
 * property rather than a hope. Each candidate is read and reclaimed only if its own recorded deletion time
 * is older than the window.
 *
 * <p>That also means an unknown age is never old enough. {@link IndexDescriptor#deletedAtMillis()} returns
 * zero both for a live descriptor and for a tombstone written before that field existed, and the obvious
 * {@code deletedAt < cutoff} would read zero as the epoch and delete every such tombstone on the first pass.
 * Zero is refused explicitly here for that reason.
 *
 * <h2>What this costs, and when it is not the right mechanism</h2>
 *
 * One listing of the tombstone space plus one read per tombstone examined, per pass. That is affordable at a
 * modest population and it is not the cheapest way to do this on a store that can expire objects by age on
 * its own: an object-store lifecycle rule over the tombstone prefix does the same job for no requests at all,
 * and is the better answer where it is available. This exists for stores that have no such facility, and as
 * the thing that still reclaims when a lifecycle rule was never configured.
 */
public final class TombstoneScrubber {

    private static final Logger logger = LogManager.getLogger(TombstoneScrubber.class);

    private final Function<BlobPath, BlobContainer> containers;
    private final BlobPath basePath;
    private final LongSupplier clock;

    public TombstoneScrubber(Function<BlobPath, BlobContainer> containers, BlobPath basePath, LongSupplier clock) {
        this.containers = containers;
        this.basePath = basePath;
        this.clock = clock;
    }

    /**
     * Reclaims every tombstone older than {@code retentionMillis}, and reports how many went.
     *
     * <p>Failures are per tombstone and swallowed. One unreadable or undeletable entry is not a reason to
     * abandon the rest of the pass, and anything skipped is simply seen again next time, because the pass
     * derives its worklist from the store rather than from a cursor it has to keep correct.
     */
    public int scrubOnce(long retentionMillis) {
        long cutoff = clock.getAsLong() - retentionMillis;
        BlobContainer tombstones = tombstones();

        List<String> candidates;
        try {
            candidates = new ArrayList<>(tombstones.listBlobs().keySet());
        } catch (IOException | RuntimeException e) {
            logger.warn("could not list tombstones to reclaim; will retry on the next pass", e);
            return 0;
        }

        int reclaimed = 0;
        for (String name : candidates) {
            try {
                if (reclaimable(tombstones, name, cutoff)) {
                    tombstones.deleteBlobsIgnoringIfNotExists(List.of(name));
                    reclaimed++;
                }
            } catch (IOException | RuntimeException e) {
                logger.debug("could not reclaim tombstone [{}]; leaving it for a later pass", name, e);
            }
        }
        if (reclaimed > 0) {
            logger.info("reclaimed [{}] tombstones deleted before [{}]", reclaimed, cutoff);
        }
        return reclaimed;
    }

    /**
     * Whether one tombstone is past the window, decided from the object itself.
     *
     * <p>Re-read immediately before the delete rather than carried over from the listing, so a name deleted,
     * recreated and deleted again between the two is seen as the young tombstone it now is. That narrows the
     * race to the gap between this read and the delete below; it does not close it, and closing it would need
     * a conditional delete the blob store does not offer. Losing it costs a tombstone reclaimed early for an
     * index deleted twice within that gap, whose consequence is stale shard data left on one node rather than
     * an index returning, because absence alone already prevents the shard being opened.
     */
    private boolean reclaimable(BlobContainer tombstones, String name, long cutoff) throws IOException {
        Optional<BlobRegister> register = tombstones.readRegister(name);
        if (register.isEmpty()) {
            return false;
        }

        IndexDescriptor tombstone;
        try {
            tombstone = BlobDescriptorBackend.decode(register.get().value());
        } catch (IOException e) {
            // Unreadable, which after S6 means written before the stored format carried a version. Its age
            // is unknowable, so it is left alone: refusing to reclaim what cannot be dated is the same rule
            // the zero check below applies, and deleting it would be reclaiming on the strength of a parse
            // failure.
            logger.debug("tombstone [{}] is not readable, so its age is unknown and it is kept", name, e);
            return false;
        }

        if (tombstone.exists()) {
            // A live descriptor under the tombstone prefix is not something this understands, and guessing
            // would mean deleting a record of an index that still exists.
            return false;
        }
        if (tombstone.deletedAtMillis() == 0L) {
            return false;
        }
        return tombstone.deletedAtMillis() < cutoff;
    }

    /**
     * The tombstone space, addressed as a child container rather than by prefix.
     *
     * <p>Not interchangeable with {@code listBlobsByPrefix("tombstones/")} on the root, which is the obvious
     * way and works only on an object store. There a key is a flat string and the slash is just a character;
     * on a filesystem repository it is a directory separator, and that listing iterates one level and returns
     * nothing at all. Addressing the child directly is what {@code DescriptorEnumerator} already does for the
     * descriptor space, and it is right on both.
     */
    private BlobContainer tombstones() {
        String prefix = BlobDescriptorBackend.TOMBSTONE_PREFIX;
        return containers.apply(basePath.add(prefix.substring(0, prefix.length() - 1)));
    }
}
