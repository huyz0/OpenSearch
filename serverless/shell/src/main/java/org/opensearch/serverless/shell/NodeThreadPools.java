/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;

/**
 * The thread pools a {@link ServerlessNode} runs on, besides core's own: their names, and how they are sized.
 */
public final class NodeThreadPools {

    private NodeThreadPools() {}

    /**
     * The pool forwarded searches and explains are answered on, and the heartbeat's head scan reads on.
     *
     * <p>Not the coordinator's own shard tasks any more: those run on GENERIC (see {@code SearchFanout}),
     * so a search that waits on GENERIC for tasks that also ran on GENERIC cannot park every thread
     * waiting for work no thread is free to do. See the thread pool's construction for why it is
     * separate at all.
     */
    public static final String FANOUT_POOL = "serverless_fanout";

    /**
     * The pool a forwarded write is applied on, at its owner.
     *
     * <p>Not WRITE. A write that arrives at a node that does not own its shard is forwarded, and the WRITE thread
     * that took it waits for the owner's answer; when the owner applied forwarded writes on WRITE too, every node's
     * WRITE threads could end up waiting on every other node's, and none was free to apply what the others were
     * waiting for. A fleet of six under a few hundred writes a second stopped acknowledging almost entirely until
     * the forwards timed out. A forwarded write is applied locally and forwards nothing further, so this pool waits
     * on nothing that runs on a pool a forward waits on.
     */
    public static final String FORWARDED_WRITE_POOL = "serverless_forwarded_write";

    /**
     * The thread a node renews its lease on, and nothing else does.
     *
     * <p>A lease that is not renewed in time is a node its peers may take everything from, so renewal cannot wait behind
     * other work. It ran on GENERIC, and under a burst of activations -- each a few object-store round trips -- renewals
     * ran late, leases ran out, peers revoked them, and a busy node became a dead one, which made the burst bigger.
     */
    public static final String LEASE_POOL = "serverless_lease";

    /**
     * The threads shard activations run on, and nothing else does.
     *
     * <p>They ran on GENERIC, which is also where a write waits for the activation of the shard it is for, and where
     * an activation pass waits for all of its own. A fleet run filled all 128 GENERIC threads with such waiters: the
     * activations they waited for sat in GENERIC's queue behind them, one starting only when a waiting write timed out
     * and gave its thread back. A shard whose owner had closed it locally waited ten minutes and more to be re-taken,
     * answering every forwarded write "not open here" meanwhile.
     */
    public static final String ACTIVATION_POOL = "serverless_activation";

    /**
     * Where shards are published, several at once: a node holding hundreds published them one after another on GENERIC,
     * and the passes that piled up behind each other starved everything else waiting there.
     */
    public static final String PUBLISH_POOL = "serverless_publish";

    /** How many shards a node activates at once; see {@code ServerlessBootstrap#ACTIVATION_CONCURRENCY}. */
    public static final String ACTIVATION_CONCURRENCY_SETTING = "serverless.activation.concurrency";

    /** The pool a forwarded get is answered on, at its owner: not GET, for the reason {@link #FORWARDED_WRITE_POOL} is not WRITE. */
    public static final String FORWARDED_READ_POOL = "serverless_forwarded_read";

    /**
     * Builds a node's thread pool: core's pools and this node's own.
     *
     * @param settings the node settings
     * @return the thread pool
     */
    static ThreadPool build(Settings settings) {
        final int processors = org.opensearch.common.util.concurrent.OpenSearchExecutors.allocatedProcessors(settings);
        return new ThreadPool(
            settings,
            new org.opensearch.threadpool.ScalingExecutorBuilder(
                FANOUT_POOL,
                1,
                Math.max(8, 4 * processors),
                org.opensearch.common.unit.TimeValue.timeValueSeconds(30)
            ),
            // Sized and queued like WRITE and GET, whose work they take over for requests that were forwarded.
            new org.opensearch.threadpool.FixedExecutorBuilder(
                settings,
                FORWARDED_WRITE_POOL,
                processors,
                10_000,
                "thread_pool." + FORWARDED_WRITE_POOL
            ),
            new org.opensearch.threadpool.FixedExecutorBuilder(settings, LEASE_POOL, 1, 16, "thread_pool." + LEASE_POOL),
            // Sized past the default activation concurrency, which is what bounds how many are submitted at once;
            // the queue only absorbs a concurrency raised beyond the pool.
            // Twice the activations allowed at once, and never fewer than sixteen: an activation can wait on another's
            // single-flight while holding a thread.
            new org.opensearch.threadpool.FixedExecutorBuilder(settings, PUBLISH_POOL, 8, 100_000, "thread_pool." + PUBLISH_POOL),
            new org.opensearch.threadpool.FixedExecutorBuilder(
                settings,
                ACTIVATION_POOL,
                Math.max(
                    16,
                    2 * settings.getAsInt(
                        ACTIVATION_CONCURRENCY_SETTING,
                        org.opensearch.serverless.reconcile.BackgroundReconciler.DEFAULT_ACTIVATION_CONCURRENCY
                    )
                ),
                1000,
                "thread_pool." + ACTIVATION_POOL
            ),
            new org.opensearch.threadpool.FixedExecutorBuilder(
                settings,
                FORWARDED_READ_POOL,
                Math.max(4, processors),
                1_000,
                "thread_pool." + FORWARDED_READ_POOL
            )
        );
    }
}
