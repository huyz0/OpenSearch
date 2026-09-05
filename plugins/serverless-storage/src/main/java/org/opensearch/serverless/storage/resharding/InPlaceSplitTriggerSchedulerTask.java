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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesRequest;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesResponse;
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;

/**
 * dynamic-partitioning-plan.md Phase 1 item 1.2: turns {@link ShardSplitCandidatesAction}'s
 * on-demand REST/transport surface into a live, continuously refreshed signal, and (when
 * constructed with a non-{@code null} {@link InPlaceSplitTriggerCoordinator}) the "do the work"
 * half too -- same two-constructor, policy-vs-mechanism shape as {@code
 * org.opensearch.serverless.storage.scaleup.ScaleUpCandidatesSchedulerTask}.
 *
 * <p>Runs only on the elected cluster-manager node, same reasoning as every other candidate
 * scheduler task in this plugin: {@link ShardSplitCandidatesAction} itself already fans out
 * cluster-wide, so running this on every node would just multiply identical work by the node count
 * for no benefit -- and only the cluster-manager can actually submit the {@code
 * InPlaceSplitShardAction} cluster-state update {@link InPlaceSplitTriggerCoordinator} issues.
 */
public final class InPlaceSplitTriggerSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(InPlaceSplitTriggerSchedulerTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final InPlaceSplitTriggerCoordinator triggerCoordinator;
    private final Scheduler.Cancellable task;

    /**
     * Starts the scheduled evaluation without acting on its own candidates -- equivalent to calling
     * the other constructor with a {@code null} coordinator (policy-only).
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ShardSplitCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected cluster-manager node.
     */
    public InPlaceSplitTriggerSchedulerTask(ThreadPool threadPool, TimeValue interval, Client client, ClusterService clusterService) {
        this(threadPool, interval, client, clusterService, null);
    }

    /**
     * Starts the scheduled evaluation.
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ShardSplitCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected
     *                       cluster-manager node, and to supply the current cluster state to the
     *                       coordinator's in-progress-split guard.
     * @param triggerCoordinator acts on every evaluation's candidates by splitting them, or {@code
     *                           null} to stay policy-only (no shard is ever split).
     */
    public InPlaceSplitTriggerSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        Client client,
        ClusterService clusterService,
        InPlaceSplitTriggerCoordinator triggerCoordinator
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.triggerCoordinator = triggerCoordinator;
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
            // Deliberately Throwable, not Exception -- see ScaleToZeroCandidatesSchedulerTask's own
            // javadoc for why: ClusterService#state() throws an AssertionError, not an Exception, if
            // called before the node's initial cluster state is applied, a real window this task's
            // very first tick or two can land in.
            logger.warn("in-place split trigger evaluation failed, will retry next tick", t);
        }
    }

    void evaluate() {
        ClusterState state = clusterService.state();
        if (state.nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager runs this
        }
        client.execute(ShardSplitCandidatesAction.INSTANCE, new ShardSplitCandidatesRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ShardSplitCandidatesResponse response) {
                long candidateCount = response.candidates()
                    .stream()
                    .filter(org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry::candidate)
                    .count();
                logger.debug(
                    "in-place split evaluation: {} shard(s) observed, {} flagged as candidates",
                    response.candidates().size(),
                    candidateCount
                );
                if (triggerCoordinator != null) {
                    triggerCoordinator.triggerCandidates(response.candidates(), state);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("in-place split trigger evaluation failed, will retry next tick", e);
            }
        });
    }

    /** Invokes {@link #evaluate()} synchronously, rather than waiting out the scheduled interval -- test-only visibility. */
    void evaluateForTesting() {
        evaluate();
    }

    /** Cancels the scheduled evaluation. */
    @Override
    public void close() {
        task.cancel();
    }
}
