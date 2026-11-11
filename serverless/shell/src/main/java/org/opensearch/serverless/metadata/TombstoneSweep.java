/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Sweeps {@link DescriptorStore}'s tombstones once their quarantine has passed: claims each one in its bucket, then
 * deletes it, so a name is reused only after every node has seen its index gone.
 */
final class TombstoneSweep {

    private final DescriptorStore store;

    TombstoneSweep(DescriptorStore store) {
        this.store = store;
    }

    /**
     * Removes tombstones that have outlived their quarantine.
     *
     * <p><b>Why they cannot stay forever.</b> A tombstone guards against one thing: a compare-and-swap
     * carrying a generation read before the delete landing on the recreated name, whose generations would
     * otherwise restart at the numbers the old one had. That swap is an operation in flight, and no
     * operation is in flight for hours. Past that, a tombstone is a register every listing reads and every
     * prefix pattern counts — a tenant creating and deleting a daily index poisoned {@code logs-*} after
     * five hundred days with one live index under it.
     *
     * <p><b>Found through markers, not by walking the population.</b> Every delete drops a marker into its
     * hour's bucket ({@link RegisterMap#tombstones}), so this lists the buckets, skips any too young to
     * hold an expired name, and reads only the names in the rest. It used to list every descriptor and
     * read each one, on every node, hourly: at a million indices that was a million reads per node per
     * hour to find the few hundred names deleted that hour. The cost is now one listing of the buckets,
     * plus a read and a claim per name deleted in the buckets this node owns.
     *
     * <p><b>Deletion is conditional on the claimed generation.</b> A deadline checked before sending a
     * request cannot prevent a slow backing store from applying it after a name was recreated. Only
     * stores that passed the conditional-delete probe may reap old tombstones. Other stores retain
     * them: recreation can still swap over them, but prefix enumeration may cost more as they accumulate.
     *
     * <p><b>Safe to run on more than one node.</b> Two sweeps of the same bucket contend on the claim and
     * one of them loses it; which is what lets {@code ownsBucket} be a hint drawn from membership rather
     * than an election.
     *
     * <p>A tombstone written before the delete time was recorded reads as infinitely old. One written
     * before markers existed has none and is never found; those are left where they are.
     *
     * @param nowMillis the current time, in the clock the tombstones were stamped with
     * @param quarantineMillis how long a tombstone must have stood
     * @return the names whose tombstones were removed
     * @throws IOException if listing, reading or deleting fails
     */
    public List<String> sweepTombstones(long nowMillis, long quarantineMillis) throws IOException {
        return sweepTombstones(nowMillis, quarantineMillis, bucket -> true);
    }

    /**
     * Removes tombstones that have outlived their quarantine, from the buckets this caller owns.
     *
     * @param nowMillis the current time, in the clock the tombstones were stamped with
     * @param quarantineMillis how long a tombstone must have stood
     * @param ownsBucket whether this caller should sweep a bucket; see {@link #sweepTombstones(long, long)}
     * @return the names whose tombstones were removed
     * @throws IOException if listing, reading or deleting fails
     */
    public List<String> sweepTombstones(long nowMillis, long quarantineMillis, Predicate<String> ownsBucket) throws IOException {
        // A deadline checked before a remote request does not bound when its delete is applied.
        // Retain tombstones unless the backing store has proved it enforces the expected generation.
        if (store.conditionalDelete() == false) {
            return List.of();
        }
        final List<String> removed = new ArrayList<>();
        for (String bucket : new ArrayList<>(store.blobStore.blobContainer(store.tombstonesPath).children().keySet())) {
            final long start = Tombstones.tombstoneBucketStart(bucket);
            // Only a bucket whose oldest possible name has passed the quarantine; younger ones are not
            // listed at all. Not waiting for the whole bucket keeps the quarantine exact rather than up to
            // a bucket late, and costs at most one more listing of it, since each name is still checked.
            if (start < 0L || start + quarantineMillis > nowMillis || ownsBucket.test(bucket) == false) {
                continue;
            }
            sweepBucket(bucket, start, nowMillis, quarantineMillis, removed);
        }
        return removed;
    }

    /** What a sweep did with one marker. */
    enum Reaped {
        /** The tombstone is claimed and waiting on the batched delete. */
        CLAIMED,
        /** Nothing for this marker to do: the name is live, gone, or deleted again later. */
        STALE,
        /** Not now: inside its quarantine, or another sweep's claim. */
        KEEP
    }

    void sweepBucket(String bucket, long bucketStart, long nowMillis, long quarantineMillis, List<String> removed) throws IOException {
        final BlobContainer markers = store.blobStore.blobContainer(store.tombstonesPath.add(bucket));
        final int page = store.markerPage;
        boolean kept = false;
        // In pages rather than one listing held whole: an hour's bucket at a thousand deletes a second is
        // millions of names. There is no resuming a listing through BlobContainer, but there need not be --
        // every page deletes the markers it finished, so listing the first page again lists the next ones.
        while (true) {
            final List<org.opensearch.common.blobstore.BlobMetadata> listed = markers.listBlobsByPrefixInSortedOrder(
                "",
                page,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
            final List<String> finished = new ArrayList<>();
            final Map<String, Long> claimed = new LinkedHashMap<>();
            long oldestClaim = 0L;
            for (org.opensearch.common.blobstore.BlobMetadata marker : listed) {
                final String blobName = marker.name();
                // Deletes are batched, but a claim only licenses a delete for the deadline, so the batch is
                // flushed well inside it rather than whenever it happens to fill.
                if (claimed.isEmpty() == false
                    && (claimed.size() >= 1000
                        || store.clock.getAsLong() >= oldestClaim + DescriptorStore.TOMBSTONE_REAP_DEADLINE_MILLIS / 2)) {
                    kept |= flushClaims(claimed, oldestClaim, finished, removed) == false;
                }
                final long claimAt = store.clock.getAsLong();
                switch (reap(blobName, bucketStart, nowMillis, quarantineMillis, claimAt, claimed)) {
                    case CLAIMED -> {
                        if (claimed.size() == 1) {
                            oldestClaim = claimAt;
                        }
                    }
                    case STALE -> finished.add(blobName);
                    case KEEP -> kept = true;
                }
            }
            kept |= flushClaims(claimed, oldestClaim, finished, removed) == false;
            if (finished.isEmpty() == false) {
                markers.deleteBlobsIgnoringIfNotExists(finished);
            }
            // A short page was the last. A page that finished nothing holds only markers kept for later,
            // and listing again would return that same page.
            if (listed.size() < page || finished.isEmpty()) {
                break;
            }
        }
        // An emptied bucket goes too, or a filesystem store would keep a directory an hour forever. Listed
        // again first, so a marker a badly skewed clock dropped in meanwhile is not taken with it.
        if (kept == false && markers.listBlobs().isEmpty()) {
            markers.delete();
        }
    }

    /**
     * Deletes the claimed tombstones if the claims are still inside the deadline.
     *
     * @return false if they were not, and the markers are kept for a later sweep
     */
    boolean flushClaims(Map<String, Long> claimed, long oldestClaim, List<String> finished, List<String> removed) throws IOException {
        if (claimed.isEmpty()) {
            return true;
        }
        try {
            if (store.clock.getAsLong() >= oldestClaim + DescriptorStore.TOMBSTONE_REAP_DEADLINE_MILLIS) {
                // Too late to be sure no create has taken a claim over. Left claimed; a later sweep finds the
                // marker, sees the claim has aged past the handoff, and claims it afresh.
                return false;
            }
            boolean all = true;
            for (Map.Entry<String, Long> claim : claimed.entrySet()) {
                if (store.container.deleteRegisterIfUnchanged(claim.getKey(), claim.getValue())) {
                    removed.add(claim.getKey());
                    finished.add(claim.getKey());
                } else {
                    // A newer claim or lifecycle change still needs its discovery marker.
                    all = false;
                }
            }
            return all;
        } finally {
            claimed.clear();
        }
    }

    Reaped reap(String blobName, long bucketStart, long nowMillis, long quarantineMillis, long claimAt, Map<String, Long> claimed)
        throws IOException {
        final Optional<BlobRegister> register = store.container.readRegister(blobName);
        if (register.isEmpty() || Tombstones.isTombstone(register.get().value()) == false) {
            // Already removed, recreated since, or a delete whose swap lost after its marker was written.
            return Reaped.STALE;
        }
        final org.opensearch.core.common.bytes.BytesReference value = register.get().value();
        final long deletedAt = Tombstones.tombstoneDeletedAt(value);
        if (deletedAt >= bucketStart + DescriptorStore.TOMBSTONE_BUCKET_MILLIS) {
            // Recreated and deleted again later. That delete dropped its own marker in its own bucket.
            return Reaped.STALE;
        }
        if (nowMillis - deletedAt < quarantineMillis) {
            // Stamped by a clock running ahead of this one. Its quarantine is not this sweep's to shorten.
            return Reaped.KEEP;
        }
        final long reapingAt = Tombstones.tombstoneReapingAt(value);
        if (reapingAt != 0L && claimAt < reapingAt + DescriptorStore.TOMBSTONE_REAP_HANDOFF_MILLIS) {
            // Another sweep's claim, not yet abandoned.
            return Reaped.KEEP;
        }
        final BlobRegisterCasResult swapped = store.container.compareAndSwapRegister(
            blobName,
            register.get().generation(),
            Tombstones.tombstone(deletedAt, claimAt)
        );
        if (swapped.applied()) {
            claimed.put(blobName, swapped.currentGeneration());
            return Reaped.CLAIMED;
        }
        return Reaped.KEEP;
    }
}
