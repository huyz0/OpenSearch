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
 * <p>Deliberately still the "policy" half only: {@link #latestCandidates()} is read-only, and
 * nothing in this class suspends, closes, or reactivates anything -- see {@code
 * ScaleToZeroCandidatesAction}'s own javadoc for why the actual mechanism stays out of scope here.
 */
public final class ScaleToZeroCandidatesSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(ScaleToZeroCandidatesSchedulerTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Scheduler.Cancellable task;
    private final AtomicReference<List<ScaleToZeroCandidateEntry>> latestCandidates = new AtomicReference<>(List.of());

    /**
     * Starts the scheduled evaluation.
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ScaleToZeroCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected cluster-manager node.
     */
    public ScaleToZeroCandidatesSchedulerTask(ThreadPool threadPool, TimeValue interval, Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, interval, ThreadPool.Names.GENERIC);
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
            // the staleness of latestCandidates() until the next successful one. A scheduled task's
            // own recurring schedule must never die from a single tick's exception either way --
            // Scheduler.scheduleWithFixedDelay doesn't reschedule a runnable that threw.
            logger.warn("scale-to-zero candidate evaluation failed, will retry next tick", t);
        }
    }

    void evaluate() {
        if (clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager runs this
        }
        client.execute(ScaleToZeroCandidatesAction.INSTANCE, new ScaleToZeroCandidatesRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ScaleToZeroCandidatesResponse response) {
                latestCandidates.set(List.copyOf(response.candidates()));
                long candidateCount = response.candidates().stream().filter(ScaleToZeroCandidateEntry::candidate).count();
                logger.debug(
                    "scale-to-zero evaluation: {} shard(s) observed, {} flagged as candidates",
                    response.candidates().size(),
                    candidateCount
                );
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("scale-to-zero candidate evaluation failed, will retry next tick", e);
            }
        });
    }

    /**
     * The most recently completed evaluation's merged candidate list, or an empty list if no
     * evaluation has completed successfully yet (including on every non-cluster-manager node, which
     * never runs an evaluation of its own).
     */
    public List<ScaleToZeroCandidateEntry> latestCandidates() {
        return latestCandidates.get();
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
}
