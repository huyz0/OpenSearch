/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import org.opensearch.serverless.nodelog.NodeLogBlob.ShardKey;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Confirms that this node still owns the shards whose first records go into an epoch, before the epoch's next blob is
 * written: one check per shard new to the epoch, a few at a time when an executor is set.
 */
final class NodeLogAdmissions {

    /** Remembers that a shard passed its check for an epoch, so its later records in it are not checked again. */
    @FunctionalInterface
    interface Admitted {
        void mark(long epoch, ShardKey shard, long term);
    }

    private final NodeLogCommitter.ShardAdmission admission;
    private final NodeLogCommitTimings timings;
    private final Admitted admitted;

    private volatile java.util.concurrent.Executor admissionExecutor;
    private volatile int admissionConcurrency = 1;

    NodeLogAdmissions(NodeLogCommitter.ShardAdmission admission, NodeLogCommitTimings timings, Admitted admitted) {
        this.admission = Objects.requireNonNull(admission, "admission");
        this.timings = timings;
        this.admitted = admitted;
    }

    /**
     * Checks the shards new to an epoch up to this many at a time, on the given executor and the flusher itself.
     *
     * @param concurrency how many checks may run at once; one or less checks them one after another
     * @param executor where the extra checks run; null checks them one after another
     */
    void configure(int concurrency, java.util.concurrent.Executor executor) {
        this.admissionExecutor = executor;
        this.admissionConcurrency = Math.max(1, concurrency);
    }

    /**
     * Checks every shard new to the epoch, a few at a time when an executor is set. One after another, a window carrying
     * the first records of thousands of shards -- a node's first writes after taking them, or after its epoch changed --
     * waited one head read per shard before its PUT: at 20 ms a read, a minute for three thousand shards, with every write
     * on the node waiting behind it and the write limiter refusing them. Each check is independent and all of them still
     * finish before the PUT, so running them side by side changes only how long the window waits.
     */
    void admitAll(long epoch, List<NodeLogWindow.Key> keys, NodeLogWindow window) {
        final java.util.concurrent.Executor executor = admissionExecutor;
        final int parallelism = Math.min(admissionConcurrency, keys.size());
        if (executor == null || parallelism <= 1) {
            for (NodeLogWindow.Key key : keys) {
                admitOne(epoch, key, window);
            }
            return;
        }
        final java.util.concurrent.ConcurrentLinkedQueue<NodeLogWindow.Key> queue = new java.util.concurrent.ConcurrentLinkedQueue<>(keys);
        final java.util.concurrent.ConcurrentHashMap<ShardKey, IOException> failed = new java.util.concurrent.ConcurrentHashMap<>();
        // Counted by checks, not by helpers: a helper the pool runs late finds the queue empty and the flusher has not
        // waited for it, so a busy pool slows the checks down to the flusher's own pace and never holds the window longer.
        final java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(keys.size());
        final Runnable drain = () -> {
            for (NodeLogWindow.Key key = queue.poll(); key != null; key = queue.poll()) {
                try {
                    final IOException failure = check(epoch, key);
                    if (failure != null) {
                        failed.put(key.shard(), failure);
                    }
                } finally {
                    done.countDown();
                }
            }
        };
        for (int i = 0; i < parallelism - 1; i++) {
            try {
                executor.execute(drain);
            } catch (RuntimeException rejected) {
                break;
            }
        }
        // This thread drains too, so the checks finish even when the executor runs none of them.
        drain.run();
        boolean interrupted = false;
        while (true) {
            try {
                done.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        window.failures.putAll(failed);
    }

    private void admitOne(long epoch, NodeLogWindow.Key key, NodeLogWindow window) {
        final IOException failure = check(epoch, key);
        if (failure != null) {
            window.failures.put(key.shard(), failure);
        }
    }

    /** Checks one shard and marks it admitted; returns the refusal, or null. */
    private IOException check(long epoch, NodeLogWindow.Key key) {
        try {
            timings.admissionChecks.increment();
            admission.admit(key.shard(), key.term(), epoch);
            admitted.mark(epoch, key.shard(), key.term());
            return null;
        } catch (IOException | RuntimeException e) {
            return new IOException("refused to log to " + key.shard() + " at term " + key.term(), e);
        }
    }
}
