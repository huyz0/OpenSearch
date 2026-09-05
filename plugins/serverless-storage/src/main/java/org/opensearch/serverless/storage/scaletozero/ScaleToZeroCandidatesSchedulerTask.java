/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidateEntry;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesAction;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesRequest;
import org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesResponse;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The node-level counterpart of {@code CompactionSchedulerTask}/{@code PitrRetentionSchedulerTask}
 * for {@link ScaleToZeroCandidatesAction} (rfc-serverless-opensearch.md &sect;7.3/&sect;10): turns
 * that action's on-demand REST/transport surface into a live, continuously refreshed signal, so an
 * eventual policy consumer doesn't have to poll the REST API itself and every node doesn't need to
 * separately schedule the same cluster-wide fan-out.
 *
 * <p>Runs only on the elected cluster-manager node -- checked fresh on every tick via {@link
 * ClusterService#state()}, the same "don't cache eligibility, the elected node can change"
 * reasoning any singleton-per-cluster scheduled task needs -- since {@link ScaleToZeroCandidatesAction}
 * itself already fans out cluster-wide; running this on every node would just multiply identical
 * work by the node count for no benefit.
 *
 * <p>Now also the "do the work" half, when constructed with a non-{@code null} {@link
 * ShardSuspensionCoordinator}: every successful evaluation's candidates are handed to {@link
 * ShardSuspensionCoordinator#suspendCandidates} exactly as read, with no additional policy applied
 * here -- {@link #latestCandidates()} remains read-only either way, for observability.
 */
public final class ScaleToZeroCandidatesSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(ScaleToZeroCandidatesSchedulerTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ShardSuspensionCoordinator suspensionCoordinator;
    private final Scheduler.Cancellable task;
    private final AtomicReference<List<ScaleToZeroCandidateEntry>> latestCandidates = new AtomicReference<>(List.of());

    /**
     * Wall-clock time the list in {@link #latestCandidates} was computed, or {@code 0} if none has
     * been. Exists because that list is only ever replaced on a <em>successful</em> fan-out and is
     * never invalidated on failure, so without a timestamp a consumer cannot tell a fresh verdict
     * from an hour-old cached one -- see {@link #latestCandidatesAtMillis()} (finding N-4).
     */
    private final java.util.concurrent.atomic.AtomicLong latestCandidatesAtMillis = new java.util.concurrent.atomic.AtomicLong(0L);

    /**
     * Monotonic counter incremented once per successful evaluation. A consumer applying its own
     * consecutive-tick hysteresis needs to know whether it is looking at a <em>new</em> observation
     * or the same one again; comparing this is exact, where comparing timestamps or list contents is
     * not (see finding N-4: drain hysteresis was counting one snapshot N times).
     */
    private final java.util.concurrent.atomic.AtomicLong evaluationSequence = new java.util.concurrent.atomic.AtomicLong(0L);

    /**
     * Starts the scheduled evaluation without acting on its own candidates -- equivalent to calling
     * the other constructor with a {@code null} coordinator (policy-only, this class's original
     * scope before suspension itself existed).
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ScaleToZeroCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected cluster-manager node.
     */
    public ScaleToZeroCandidatesSchedulerTask(ThreadPool threadPool, TimeValue interval, Client client, ClusterService clusterService) {
        this(threadPool, interval, client, clusterService, null);
    }

    /**
     * Starts the scheduled evaluation.
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ScaleToZeroCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected cluster-manager node.
     * @param suspensionCoordinator acts on every evaluation's candidates by suspending them, or
     *                              {@code null} to stay policy-only (no shard is ever suspended).
     */
    public ScaleToZeroCandidatesSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        Client client,
        ClusterService clusterService,
        ShardSuspensionCoordinator suspensionCoordinator
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.suspensionCoordinator = suspensionCoordinator;
        // Jittered rather than started on the exact configured interval (finding L-10). Every node
        // constructs this task at roughly the same moment after a cluster restart or a rolling
        // upgrade, and scheduleWithFixedDelay never recomputes the delay, so an un-jittered start
        // leaves every node's copy of this loop ticking in lockstep for the lifetime of the process
        // -- a synchronised burst of cluster-manager work and object-store requests every interval,
        // forever, which is exactly the recovery-stampede shape RFC section 13 asks the reconcilers
        // to avoid. JitteredScheduling only ever extends the first interval, never shortens it.
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, JitteredScheduling.jitter(interval), ThreadPool.Names.GENERIC);
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception: ClusterService#state() throws an AssertionError
            // (not an Exception) if called before the node's initial cluster state is applied --
            // a real window this task's very first tick or two can land in, since
            // threadPool.scheduleWithFixedDelay starts ticking as soon as createComponents
            // constructs this task, which is well before the node finishes starting up. Swallowed
            // and retried next tick, same tolerance every other scheduled task in this plugin
            // already has for a single failed attempt: nothing is corrupted by a skipped tick, only
            // the staleness of latestCandidates() until the next successful one.
            //
            // Finding L-11: this comment used to end "-- Scheduler.scheduleWithFixedDelay doesn't
            // reschedule a runnable that threw", and six tasks in this plugin were written on that
            // basis. It is not true. AbstractRunnable#run calls onAfter() in a finally block, and
            // ReschedulingRunnable#onAfter reschedules unconditionally while its `run` flag is set,
            // so even an escaping Error still reschedules; ThreadPool#scheduleWithFixedDelay also
            // logs every failure at WARN. The catch is therefore belt-and-braces rather than
            // load-bearing, and -- the reason the correction is worth making -- the tasks elsewhere
            // in this plugin that catch only Exception, or only IOException, are equally safe and
            // were never the latent wedges this comment implied they were. The one genuine wedge in
            // the scheduler is ReschedulingRunnable#onRejection, which clears `run` permanently;
            // on the GENERIC pool that is effectively unreachable outside shutdown.
            logger.warn("scale-to-zero candidate evaluation failed, will retry next tick", t);
        }
    }

    /** Guards against two overlapping fan-outs running the suspension pass concurrently (finding S-4). */
    private final java.util.concurrent.atomic.AtomicBoolean evaluationInFlight = new java.util.concurrent.atomic.AtomicBoolean();

    void evaluate() {
        if (clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager runs this
        }
        // Finding S-4 (this task has the identical shape to ScaleUpCandidatesSchedulerTask, where the
        // finding was written): the fan-out is asynchronous, so a fan-out slower than the interval
        // used to let two responses run the suspension pass concurrently. One at a time; a skipped
        // tick costs nothing here, since suspension is idempotent and the next tick redoes it.
        if (evaluationInFlight.compareAndSet(false, true) == false) {
            logger.debug("skipping scale-to-zero evaluation: the previous fan-out has not completed yet");
            return;
        }
        try {
            client.execute(ScaleToZeroCandidatesAction.INSTANCE, new ScaleToZeroCandidatesRequest(), new ActionListener<>() {
                @Override
                public void onResponse(ScaleToZeroCandidatesResponse response) {
                    try {
                        latestCandidates.set(List.copyOf(response.candidates()));
                        latestCandidatesAtMillis.set(System.currentTimeMillis());
                        evaluationSequence.incrementAndGet();
                        long candidateCount = response.candidates().stream().filter(ScaleToZeroCandidateEntry::candidate).count();
                        logger.debug(
                            "scale-to-zero evaluation: {} shard(s) observed, {} flagged as candidates",
                            response.candidates().size(),
                            candidateCount
                        );
                        if (suspensionCoordinator != null) {
                            suspensionCoordinator.suspendCandidates(response.candidates());
                            suspensionCoordinator.suspendReaderCandidates(response.candidates());
                        }
                    } finally {
                        evaluationInFlight.set(false);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    evaluationInFlight.set(false);
                    logger.warn("scale-to-zero candidate evaluation failed, will retry next tick", e);
                }
            });
        } catch (RuntimeException e) {
            evaluationInFlight.set(false);
            throw e;
        }
    }

    /**
     * The most recently completed evaluation's merged candidate list, or an empty list if no
     * evaluation has completed successfully yet (including on every non-cluster-manager node, which
     * never runs an evaluation of its own).
     */
    public List<ScaleToZeroCandidateEntry> latestCandidates() {
        return latestCandidates.get();
    }

    /**
     * When {@link #latestCandidates()} was computed, in epoch millis, or {@code 0} if no evaluation
     * has ever succeeded on this node.
     *
     * <p><b>Finding N-4.</b> The candidate list is replaced only on a successful fan-out and is
     * never cleared on failure, so once the fan-out starts failing, consumers kept treating the last
     * good list as current indefinitely. It also carried no age, so a consumer on a shorter interval
     * than this task's re-read the identical snapshot many times -- which is how "sustained for 10
     * consecutive ticks" came to be certified by a single observation. Consumers must check this
     * before trusting the list.
     */
    public long latestCandidatesAtMillis() {
        return latestCandidatesAtMillis.get();
    }

    /**
     * How many evaluations have succeeded on this node. Strictly increasing; a consumer that only
     * wants to count genuinely new observations compares this against the value it last saw.
     */
    public long evaluationSequence() {
        return evaluationSequence.get();
    }

    /** Invokes {@link #evaluate()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void evaluateForTesting() {
        evaluate();
    }

    /** Cancels the scheduled evaluation; does not touch {@link #latestCandidates()}, which simply stops updating. */
    @Override
    public void close() {
        task.cancel();
    }

    /**
     * Whether the scheduled evaluation has been cancelled (by {@link #close()}) -- test-only, but
     * public since {@code ServerlessStoragePluginTests} (a different package) needs to verify the
     * plugin's own {@code close()} actually reaches this task, not just that it doesn't throw.
     */
    public boolean isCancelledForTesting() {
        return task.isCancelled();
    }
}
