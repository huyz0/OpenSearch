/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The seals of a shard's write log: the cutoffs takeovers recorded before fences replaced them, for logs written then.
 */
final class WalSeals {

    private WalSeals() {}

    /**
     * Seals the log against a predecessor that has not yet stopped writing, and returns the cutoff to
     * replay under.
     *
     * <p><b>Why the snapshot has to be durable.</b> A cutoff held only in memory protects the one recovery
     * that took it. If that node then dies before publishing — which is exactly the case where the log
     * still matters — the next successor takes its own snapshot, by which time the zombie's records have
     * been sitting in the log looking like ordinary history for as long as it took. Writing the snapshot
     * down makes it survive the successor that took it, which is the whole point: the first node to take
     * over records where legitimate history ended, and every node after it inherits that answer.
     *
     * <p>Seals are merged by taking the <em>lowest</em> recorded position for each term, because the
     * earliest seal was taken closest to the moment ownership actually moved and is therefore the tightest
     * true bound. Re-sealing writes the merged answer, so one blob carries it and the older ones are then
     * dropped; a crash between the write and the drop leaves both, and merging them gives the same answer.
     *
     * <p>Seals live in their own directory so that {@link WalStore#replayable(Map)} — which reads term directories
     * — cannot mistake one for a term, and {@code dropOlderTerms} cannot delete one while pruning terms.
     * {@link WalStore#deleteAll} does remove them, which is right: that is the log being destroyed, not truncated.
     *
     * @param sealsContainer where seals are kept
     * @param here the log's position now: the highest record name of each term
     * @param term the term this node is taking the shard over at
     * @return the cutoff to replay under
     * @throws IOException if reading or writing the seals fails
     */
    static Map<Long, String> seal(BlobContainer sealsContainer, Map<Long, String> here, long term) throws IOException {
        // One listing doing the three jobs that used to cost three: which seals to merge, the highest term
        // any of them was written at, and which of them this one supersedes.
        //
        // <b>Taking the supersede set from this listing rather than from a fresh one after the write is a
        // correctness fix, not only a cheaper way to get the same answer.</b> The delete used to be driven
        // by a listing taken *after* the seal below was written, which could therefore see a seal that a
        // concurrent lower-term sealer landed after the merge above had already read the container -- and
        // delete it, unmerged. Seals merge by taking the lowest recorded position per term, so the seal
        // dropped that way was the tighter bound, and losing it widens the cutoff: a zombie's records
        // between the two bounds replay as though they had been acknowledged. Bounding the delete by what
        // was read is what makes "everything this seal supersedes" mean "everything this seal has already
        // accounted for". A concurrent seal is now neither merged nor deleted; it stays, and the next
        // sealer merges it.
        final List<String> sealNames = WalLayout.sealNamesIn(sealsContainer.listBlobs().keySet());
        final Map<Long, String> sealed = WalLayout.mergeSeals(sealsContainer, sealNames);
        final long sealedBelow = WalLayout.highestSealedTerm(sealNames);
        final Map<Long, String> cutoff = new java.util.HashMap<>();
        for (Map.Entry<Long, String> entry : here.entrySet()) {
            if (entry.getKey() >= sealedBelow) {
                // No seal speaks for this term: it is the last sealer's own term, or later. Its records
                // are the ones that sealer legitimately wrote after taking over, and they must replay.
                cutoff.put(entry.getKey(), entry.getValue());
                continue;
            }
            // A seal does speak for this term, and it is authoritative even when it says nothing. A term
            // that a seal taken above it does not mention had no records when that seal was written, so
            // anything under it now appeared afterwards -- which is the steady state of this system, not
            // an edge case: a publish truncates the log, so the usual state at takeover is an empty one,
            // and falling back to the successor's own listing here would hand a zombie's later appends
            // straight back. The empty string excludes every name.
            final String earlier = sealed.getOrDefault(entry.getKey(), "");
            cutoff.put(entry.getKey(), earlier.compareTo(entry.getValue()) > 0 ? entry.getValue() : earlier);
        }

        // A seal speaks only for terms strictly below the sealing term. The sealer's own term can appear
        // in `here` -- a shard released locally and reopened at the same term through ensureOpen seals
        // again at that term, and its container already holds records -- and writing that entry down was
        // a latent loss: the records the reopened writer went on to append after this seal, all of them
        // acknowledged, would be bounded by *this* listing if a later sealer's seal-write succeeded but
        // its delete of this one did not, because seals are merged by taking the minimum per term. The
        // sealer's own term is bounded only by a later sealer's listing, which is the one true bound; it
        // still replays in full here, through the cutoff returned below.
        final StringBuilder body = new StringBuilder();
        for (Map.Entry<Long, String> entry : new TreeMap<>(cutoff).entrySet()) {
            if (entry.getKey() >= term) {
                continue;
            }
            body.append(entry.getKey()).append(' ').append(entry.getValue()).append('\n');
        }
        final byte[] bytes = body.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        final String name = String.format(java.util.Locale.ROOT, "seal-%020d", term);
        sealsContainer.writeBlob(name, new ByteArrayInputStream(bytes), bytes.length, false);

        // Only what was read above, so nothing that arrived since is dropped unmerged. One delete call for
        // all of them: the container batches, and a seal per takeover meant a request per takeover.
        final List<String> superseded = new ArrayList<>(sealNames);
        superseded.removeIf(other -> other.compareTo(name) >= 0);
        if (superseded.isEmpty() == false) {
            sealsContainer.deleteBlobsIgnoringIfNotExists(superseded);
        }
        return cutoff;
    }

    /**
     * Reads the cutoff recorded by every node that has taken this shard over, merged.
     *
     * @param seals where seals are kept
     * @return the lowest recorded position for each sealed term, empty if the log has never been sealed
     * @throws IOException if reading fails
     */
    static Map<Long, String> sealed(BlobContainer seals) throws IOException {
        return WalLayout.mergeSeals(seals, WalLayout.sealNamesIn(seals.listBlobs().keySet()));
    }
}
