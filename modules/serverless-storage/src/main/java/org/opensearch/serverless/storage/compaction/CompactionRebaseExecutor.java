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

    public CompactionRebaseExecutor(ShardStateStore shardStateStore, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1, got " + maxAttempts);
        }
        this.shardStateStore = shardStateStore;
        this.maxAttempts = maxAttempts;
    }

    public RebaseResult publish(String indexUuid, int shardId, CompactionPublisher publisher) throws IOException {
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
            // current head and let the publisher recompute against it.
        }
        return RebaseResult.exhausted(attempts);
    }
}
