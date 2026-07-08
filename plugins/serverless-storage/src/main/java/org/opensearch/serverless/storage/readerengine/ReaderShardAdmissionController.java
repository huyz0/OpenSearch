/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.core.index.shard.ShardId;

import java.util.concurrent.Semaphore;

/**
 * A coarse, node-shared cap on how many reader shards may be open at once
 * (rfc-serverless-opensearch.md &sect;18 risk #3, "reader heap under many shards" -- segment
 * metadata, terms index, and points index per open reader are the expensive parts, and nothing
 * today stops shard density from growing until the node runs out of heap).
 *
 * <p><b>This is not &sect;7's target admission-control design</b>, and is deliberately labeled as
 * such rather than claimed to be it: &sect;7 describes per-*refresh* admission control against an
 * actual heap/cache byte budget, deferring and retrying a refresh that would exceed it while the
 * shard keeps serving slightly stale data -- that needs the lazy, block-cache-backed remote
 * `Directory` view &sect;7 also describes, which does not exist yet (today's
 * {@code ObjectStoreReaderEngine} fully materializes a manifest up front, a documented, separate
 * tradeoff -- see its own class javadoc). This class is a simpler, coarser stand-in reachable
 * without that prerequisite: a fixed cap on the *count* of concurrently open reader engines on
 * one node, checked once at open time, not weighed against actual memory usage or reconsidered on
 * refresh. It bounds the same risk, just less precisely and less gracefully (a rejected open
 * fails outright, rather than degrading to stale-but-serving).
 */
public final class ReaderShardAdmissionController {

    private final int maxConcurrentReaderShards;
    private final Semaphore permits;

    public ReaderShardAdmissionController(int maxConcurrentReaderShards) {
        if (maxConcurrentReaderShards <= 0) {
            throw new IllegalArgumentException("maxConcurrentReaderShards must be > 0, got " + maxConcurrentReaderShards);
        }
        this.maxConcurrentReaderShards = maxConcurrentReaderShards;
        this.permits = new Semaphore(maxConcurrentReaderShards);
    }

    /**
     * Admits one more open reader shard, or throws if this node has already reached its cap.
     * Every successful call must be matched by exactly one {@link #release()} once that shard's
     * engine closes -- {@link ObjectStoreReaderEngine} owns that pairing, not callers of this
     * method directly.
     */
    public void acquire(ShardId shardId) {
        if (permits.tryAcquire() == false) {
            throw new IllegalStateException(
                "node has reached its reader-shard admission limit ("
                    + maxConcurrentReaderShards
                    + " concurrently open reader shards); cannot open reader shard "
                    + shardId
                    + " until another reader shard on this node closes"
            );
        }
    }

    public void release() {
        permits.release();
    }

    /** Exposed for tests/metrics, not part of the admission contract itself. */
    int availablePermits() {
        return permits.availablePermits();
    }
}
