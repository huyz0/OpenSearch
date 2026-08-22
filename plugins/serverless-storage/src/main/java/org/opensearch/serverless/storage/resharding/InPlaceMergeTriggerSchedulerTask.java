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
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;

/**
 * dynamic-partitioning-plan.md Phase 2 item 2.3: the merge-side counterpart to {@link
 * InPlaceSplitTriggerSchedulerTask}. Reuses the very same cluster-wide per-shard write-rate/size
 * signal ({@link ShardSplitCandidatesAction}) split's own trigger consumes -- the split-candidates
 * action reports every writer shard's raw {@code writesPerMinute}/{@code shardSizeInBytes}, which is
 * exactly the per-child signal {@link InPlaceMergeTriggerCoordinator} needs to sum across a sibling
 * pair, so no separate merge-candidates action is warranted. When constructed with a non-{@code
 * null} coordinator it also does the work (merges); otherwise it stays policy-only.
 *
 * <p>Runs only on the elected cluster-manager node, same reasoning as every other candidate
 * scheduler task in this plugin: {@link ShardSplitCandidatesAction} already fans out cluster-wide,
 * and only the cluster-manager can submit the {@code InPlaceMergeShardAction} cluster-state update
 * the coordinator issues.
 */
public final class InPlaceMergeTriggerSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(InPlaceMergeTriggerSchedulerTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final InPlaceMergeTriggerCoordinator triggerCoordinator;
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
    public InPlaceMergeTriggerSchedulerTask(ThreadPool threadPool, TimeValue interval, Client client, ClusterService clusterService) {
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
     *                       coordinator's merge-eligibility checks.
     * @param triggerCoordinator acts on every evaluation's candidates by merging quiet sibling pairs,
     *                           or {@code null} to stay policy-only (nothing is ever merged).
     */
    public InPlaceMergeTriggerSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        Client client,
        ClusterService clusterService,
        InPlaceMergeTriggerCoordinator triggerCoordinator
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.triggerCoordinator = triggerCoordinator;
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception -- same reasoning as InPlaceSplitTriggerSchedulerTask:
            // ClusterService#state() throws an AssertionError, not an Exception, if called before the
            // node's initial cluster state is applied, a real window this task's first tick can land in.
            logger.warn("in-place merge trigger evaluation failed, will retry next tick", t);
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
                logger.debug("in-place merge evaluation: {} writer shard(s) observed", response.candidates().size());
                if (triggerCoordinator != null) {
                    triggerCoordinator.triggerCandidates(response.candidates(), state);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("in-place merge trigger evaluation failed, will retry next tick", e);
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
