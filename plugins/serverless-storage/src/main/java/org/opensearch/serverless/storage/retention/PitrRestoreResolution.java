/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Which manifest generation answers for a point in time.
 *
 * <p>The missing half of point-in-time recovery. {@link PitrRetentionPolicy} decides which generations must
 * survive so that any instant inside the window <em>can</em> be answered; nothing decided which generation
 * answers a given instant, so the data was kept and could not be asked for. This is that function.
 *
 * <p><b>The newest manifest created at or before the instant.</b> A manifest is a commit, and it answers for
 * every instant from its own creation until the next one exists -- so the state of the shard at time T is the
 * state the most recent commit at or before T published. Restoring to a generation created *after* T would
 * include writes the caller asked to be rid of, which is the whole reason they named a time.
 *
 * <p><b>Granularity is the commit, not the instant</b>, and that is a property of the design rather than of
 * this function. Between two commits there is nothing durable to restore to: the writes in that gap live in
 * the WAL, and {@code WalRecord} carries {@code (indexUuid, shardId, primaryTerm, seqNo)} and no timestamp,
 * so replay can stop at a sequence number but not at a wall-clock second. Sub-commit precision needs a
 * decision about that record format first. A caller asking for 14:32:05 gets the last commit at or before
 * 14:32:05, and the response says which.
 *
 * <p>Pure: no I/O, no clock read, no registry. {@code nowMillis} is never consulted -- only the instant the
 * caller asked for -- which is what makes this as testable as {@link PitrRetentionPolicy}, whose shape it
 * deliberately mirrors.
 */
public final class PitrRestoreResolution {

    private PitrRestoreResolution() {}

    /**
     * The generation that answers for {@code instantMillis}, or empty when the shard has none that old.
     *
     * @param manifestsForOneShard all known manifests belonging to exactly one (index, shard).
     * @param instantMillis        the instant to resolve, in epoch millis.
     * @return the newest manifest created at or before {@code instantMillis}.
     */
    public static Optional<CommitManifest> newestAtOrBefore(List<CommitManifest> manifestsForOneShard, long instantMillis) {
        CommitManifest best = null;
        for (CommitManifest candidate : manifestsForOneShard) {
            if (candidate.createdAtMillis() > instantMillis) {
                continue;
            }
            if (best == null || isNewer(candidate, best)) {
                best = candidate;
            }
        }
        return Optional.ofNullable(best);
    }

    /**
     * The oldest instant this shard can be restored to, for the message a caller gets when they ask for one
     * earlier than that.
     *
     * <p>Named rather than left to the caller to infer, because "there is nothing that old" and "here is how
     * far back you can go" are the same answer and only one of them is useful. Empty when the shard has no
     * manifests at all, which is a different thing and reads differently.
     */
    public static OptionalLong oldestInstant(List<CommitManifest> manifestsForOneShard) {
        long oldest = Long.MAX_VALUE;
        for (CommitManifest manifest : manifestsForOneShard) {
            oldest = Math.min(oldest, manifest.createdAtMillis());
        }
        return oldest == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(oldest);
    }

    /**
     * Later of two manifests, by creation time and then by where they sit in the commit order.
     *
     * <p>The tie-break is not decoration. Manifests are stamped from a wall clock at a resolution coarser
     * than the rate a shard can commit at, so two manifests sharing a millisecond is ordinary rather than
     * exceptional, and picking either one would make the answer depend on iteration order -- which is the
     * kind of thing that is right in every test and wrong once in production. Within a term the higher
     * generation is later; across terms the higher term is later, because a term only advances when a new
     * writer takes over.
     */
    private static boolean isNewer(CommitManifest candidate, CommitManifest incumbent) {
        if (candidate.createdAtMillis() != incumbent.createdAtMillis()) {
            return candidate.createdAtMillis() > incumbent.createdAtMillis();
        }
        if (candidate.primaryTerm() != incumbent.primaryTerm()) {
            return candidate.primaryTerm() > incumbent.primaryTerm();
        }
        return candidate.generation() > incumbent.generation();
    }
}
