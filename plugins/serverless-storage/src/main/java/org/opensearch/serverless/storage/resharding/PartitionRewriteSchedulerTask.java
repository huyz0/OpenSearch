/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.serverless.storage.scheduling.RewriteAdmissionController;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;

/**
 * Runs {@link PartitionRewritePublisher#rewrite} on a fixed schedule for one split-target shard --
 * the piece rfc-serverless-opensearch.md &sect;16 Phase 5 flagged as still missing:
 * {@code ShardPartitionRewriteAction} exposed the rewrite as an on-demand trigger only, "this
 * increment does not add its own background scheduler for rewrite," the same "narrower than
 * originally scoped" shape compaction's own on-demand trigger had before {@link
 * org.opensearch.serverless.storage.compaction.CompactionSchedulerTask} closed the equivalent gap
 * for compaction. Mirrors that class's own shape directly: a scheduled tick that swallows every
 * {@link Exception} rather than letting a transient failure escape onto the scheduler thread, since
 * {@link PartitionRewritePublisher#rewrite} is already documented "always safe to call
 * speculatively" -- a failed or skipped tick is never worse than a no-op, and the next scheduled
 * tick simply reevaluates from whatever the (possibly by-then-different) live state is.
 *
 * <p><b>{@code admissionController} ({@code null} disables the cap entirely)</b>: every split-target
 * shard's tick is scheduled independently with no lockstep protection beyond {@code
 * CompactionSchedulerTask}'s own jitter reasoning -- nothing stops many shards on the same node from
 * rewriting concurrently. A non-{@code null} {@link RewriteAdmissionController}, shared across every
 * shard on the node (and with {@code CompactionSchedulerTask}, which competes for the same CPU/IO
 * budget), caps how many rewrite ticks actually run at once; a tick that can't get a permit skips
 * this round entirely, no worse than any other reason {@link PartitionRewritePublisher#rewrite}
 * might no-op, and is re-evaluated on this shard's own next scheduled tick.
 */
public final class PartitionRewriteSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(PartitionRewriteSchedulerTask.class);

    private final PartitionRewritePublisher publisher;
    private final RewriteAdmissionController admissionController;
    private final Scheduler.Cancellable task;

    /**
     * Schedules background partition rewrite for one shard, ticking on the given interval, with no
     * node-wide concurrency cap.
     *
     * @param threadPool thread pool used to schedule the recurring rewrite check
     * @param interval   delay between successive rewrite ticks
     * @param publisher  performs one rewrite attempt (a no-op if this shard has no partition
     *                   descriptor left, or has no published head yet)
     */
    public PartitionRewriteSchedulerTask(ThreadPool threadPool, TimeValue interval, PartitionRewritePublisher publisher) {
        this(threadPool, interval, publisher, null);
    }

    /**
     * Schedules background partition rewrite for one shard, ticking on the given interval.
     *
     * @param threadPool thread pool used to schedule the recurring rewrite check
     * @param interval   delay between successive rewrite ticks
     * @param publisher  performs one rewrite attempt (a no-op if this shard has no partition
     *                   descriptor left, or has no published head yet)
     * @param admissionController {@code null} to disable the node-wide concurrency cap entirely; see this class's own javadoc.
     */
    public PartitionRewriteSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        PartitionRewritePublisher publisher,
        RewriteAdmissionController admissionController
    ) {
        this.publisher = publisher;
        this.admissionController = admissionController;
        // Jittered rather than started on the exact configured interval (finding L-10). Every node
        // constructs this task at roughly the same moment after a cluster restart or a rolling
        // upgrade, and scheduleWithFixedDelay never recomputes the delay, so an un-jittered start
        // leaves every node's copy of this loop ticking in lockstep for the lifetime of the process
        // -- a synchronised burst of cluster-manager work and object-store requests every interval,
        // forever, which is exactly the recovery-stampede shape RFC section 13 asks the reconcilers
        // to avoid. JitteredScheduling only ever extends the first interval, never shortens it.
        this.task = threadPool.scheduleWithFixedDelay(this::rewriteSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    /** Package-private, not private, purely so this task's own catch behavior is directly testable without reflection. */
    void rewriteSafely() {
        if (admissionController != null && admissionController.tryAcquire() == false) {
            // See class javadoc: never worse than a no-op, re-evaluated on this shard's own next tick.
            logger.debug("partition rewrite tick skipped: node's compaction/rewrite admission cap reached");
            return;
        }
        try {
            publisher.rewrite();
        } catch (Exception e) {
            // See class javadoc: swallow and let the next scheduled tick reevaluate. Catches every
            // Exception, not just IOException, matching CompactionSchedulerTask's own reasoning --
            // this runs against a BlobContainer that may be wrapped in RestrictingBlobContainer
            // (rfc-serverless-opensearch.md &sect;15), which throws the unchecked SecurityException
            // on a denied delete rather than IOException.
            logger.warn("partition rewrite tick failed, will retry next tick", e);
        } finally {
            if (admissionController != null) {
                admissionController.release();
            }
        }
    }

    @Override
    public void close() {
        task.cancel();
    }
}
