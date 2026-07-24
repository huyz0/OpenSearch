/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scheduling;

import java.util.concurrent.Semaphore;

/**
 * A node-shared cap on how many CPU/IO-heavy background rewrite ticks -- {@code
 * CompactionSchedulerTask}'s Lucene merges and {@code PartitionRewriteSchedulerTask}'s split
 * partition rewrites -- may run concurrently across every shard on the node.
 *
 * <p>Each of those tasks is scheduled independently per shard, jittered only to avoid <em>every</em>
 * shard ticking in lockstep after a coordinated event (see {@link JitteredScheduling}) -- nothing
 * otherwise stops many shards' ticks from landing close together and all deciding, independently,
 * that they are compaction/rewrite candidates. Without a shared cap, a node hosting many shards
 * could end up running an unbounded number of real Lucene merges at once, each materializing
 * segments into a directory and doing sustained CPU/disk work, competing for the same node
 * resources that every other shard's indexing and search also needs.
 *
 * <p>Deliberately a <b>skip-this-tick</b> gate, not a queue or a blocking wait: both scheduled tasks
 * already treat a skipped tick as no worse than a no-op (their own thresholds/candidacy checks are
 * re-evaluated fresh on the very next tick), so refusing admission here is just one more reason a
 * tick can decide "not now" -- exactly like an empty candidate set or a transient read failure,
 * never a caller-visible error.
 */
public final class RewriteAdmissionController {

    private final int maxConcurrentRewrites;
    private final Semaphore permits;

    /**
     * Creates a node-shared cap on concurrent compaction/rewrite ticks.
     *
     * @param maxConcurrentRewrites the maximum number of compaction/rewrite ticks this node will run at once.
     */
    public RewriteAdmissionController(int maxConcurrentRewrites) {
        if (maxConcurrentRewrites <= 0) {
            throw new IllegalArgumentException("maxConcurrentRewrites must be > 0, got " + maxConcurrentRewrites);
        }
        this.maxConcurrentRewrites = maxConcurrentRewrites;
        this.permits = new Semaphore(maxConcurrentRewrites);
    }

    /**
     * Attempts to admit one more concurrent rewrite tick, non-blocking. Every {@code true} return
     * must be matched by exactly one {@link #release()} once that tick's work (success, failure, or
     * skip) has finished.
     *
     * @return whether a permit was acquired; {@code false} means the node is already at its cap this tick.
     */
    public boolean tryAcquire() {
        return permits.tryAcquire();
    }

    /** Releases the permit acquired by a matching {@link #tryAcquire()} that returned {@code true}. */
    public void release() {
        permits.release();
    }

    /** Exposed for tests/metrics, not part of the admission contract itself. */
    int availablePermits() {
        return permits.availablePermits();
    }

    /** Exposed for tests/metrics, not part of the admission contract itself. */
    int maxConcurrentRewrites() {
        return maxConcurrentRewrites;
    }
}
