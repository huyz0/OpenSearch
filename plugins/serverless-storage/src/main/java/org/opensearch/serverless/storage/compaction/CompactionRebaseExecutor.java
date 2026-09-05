/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.Optional;

/**
 * Implements the compaction service's rebase-on-conflict publication protocol
 * (rfc-serverless-opensearch.md &sect;7.4): a compactor's manifest publish is a CAS on the
 * shard's head; if a writer (or another compactor) published a newer generation meanwhile, the
 * compactor rebases &mdash; recomputes its new head against the actual current one and retries
 * &mdash; up to a bounded number of attempts, after which it gives up (the active owner of the
 * shard merges there anyway, so a hot shard simply never needs the compaction service).
 * Compaction is therefore always safe: at worst, wasted work, never a lost or corrupted update.
 *
 * <p>This class has no opinion on what a "compaction" is; {@link CompactionPublisher} is the
 * caller's merge-result-to-manifest logic, and {@link CompactionPolicy} is the separate decision
 * of whether a shard is worth compacting in the first place.
 */
public final class CompactionRebaseExecutor {

    private final ShardStateStore shardStateStore;
    private final int maxAttempts;

    /**
     * Creates an executor that rebases against the given store, up to a bounded number of attempts.
     *
     * @param shardStateStore store used to read and CAS the shard's live head
     * @param maxAttempts     maximum number of rebase-and-retry attempts before giving up (must be &gt;= 1)
     */
    public CompactionRebaseExecutor(ShardStateStore shardStateStore, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
        }
        this.shardStateStore = shardStateStore;
        this.maxAttempts = maxAttempts;
    }

    /**
     * Runs the rebase-on-conflict publish protocol: computes a new head via {@code publisher} and attempts to
     * CAS it onto the shard's live head, retrying against the freshly-read head on every conflict until either
     * the CAS succeeds, the publisher abandons, or {@code maxAttempts} is exhausted.
     *
     * @param indexUuid UUID of the index the shard belongs to
     * @param shardId   id of the shard within the index
     * @param publisher computes the new head to attempt to publish, given the shard's actual current head
     * @return the outcome of the publish attempt: published, abandoned by the publisher, or exhausted retries
     * @throws IOException if reading the shard's live head or manifest fails
     */
    public RebaseResult publish(String indexUuid, int shardId, CompactionPublisher publisher) throws IOException {
        try {
            return publishAndRetry(indexUuid, shardId, publisher);
        } finally {
            // A publisher that holds resources across the retry loop -- LuceneMergeCompactionPublisher
            // keeps a temporary filesystem directory holding a whole shard's merged segments, so it
            // can reuse the merge when a retry's source manifest turns out to be unchanged -- is
            // released here rather than by each of this method's callers. Closing it is meaningless
            // before the loop ends (that is precisely the window the cache exists for) and every
            // caller would otherwise have to remember to do it; one of them not remembering leaves a
            // shard-sized directory on disk. Idempotent, so a caller that also closes is fine.
            if (publisher instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) publisher).close();
                } catch (Exception closeFailure) {
                    throw new IOException(
                        "failed to release compaction publisher resources for " + indexUuid + "/" + shardId,
                        closeFailure
                    );
                }
            }
        }
    }

    private RebaseResult publishAndRetry(String indexUuid, int shardId, CompactionPublisher publisher) throws IOException {
        int attempts = 0;
        while (attempts < maxAttempts) {
            attempts++;

            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
            if (current.isEmpty()) {
                // The shard has no head at all (never activated, or somehow deactivated
                // mid-compaction) -- there is nothing to rebase against.
                return RebaseResult.abandoned(attempts);
            }

            Optional<ShardHead> newHead = publisher.computeNewHead(current.get().head());
            if (newHead.isEmpty()) {
                return RebaseResult.abandoned(attempts);
            }

            CasResult result = shardStateStore.compareAndSet(indexUuid, shardId, Optional.of(current.get().version()), newHead.get());
            if (result == CasResult.SUCCESS) {
                return RebaseResult.published(attempts);
            }
            // VERSION_CONFLICT: someone else published meanwhile. Loop -- re-read the new
            // current head and let the publisher recompute against it. Whatever this losing
            // attempt's own publisher call already durably uploaded (a bundle, for
            // LuceneMergeCompactionPublisher's real implementation) is now unreferenced by any
            // manifest -- this executor has no delete permission of its own and makes no attempt
            // to clean it up; see LuceneMergeCompactionPublisher's own javadoc for why that's
            // deliberate and how it's eventually reclaimed.
        }
        return RebaseResult.exhausted(attempts);
    }
}
