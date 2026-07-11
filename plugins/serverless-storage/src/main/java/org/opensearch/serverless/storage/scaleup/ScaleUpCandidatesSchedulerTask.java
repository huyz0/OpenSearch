/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidateEntry;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesAction;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesRequest;
import org.opensearch.serverless.storage.scaleup.action.ScaleUpCandidatesResponse;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The scale-up counterpart of {@code
 * org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask}: turns {@link
 * ScaleUpCandidatesAction}'s on-demand REST/transport surface into a live, continuously refreshed
 * signal, and (when constructed with a non-{@code null} {@link ReaderReplicaExpansionCoordinator})
 * the "do the work" half too.
 *
 * <p>Deliberately a separate {@link Scheduler.Cancellable} from scale-to-zero's own scheduler task,
 * not a shared one: scale-up and scale-to-zero are independent decisions on independent schedules
 * (an operator may well want to evaluate scale-up far more aggressively than scale-to-zero's
 * suspend/reactivate, which already carries its own hysteresis cooldown) -- exactly how {@code
 * ShardSuspensionCoordinator} (suspend) and {@code ShardReactivationActionFilter} (reactivate)
 * already stay separate mechanisms with separate triggers rather than one combined "rebalance"
 * task.
 *
 * <p>Runs only on the elected cluster-manager node, same reasoning as {@code
 * ScaleToZeroCandidatesSchedulerTask}: {@link ScaleUpCandidatesAction} itself already fans out
 * cluster-wide, so running this on every node would just multiply identical work by the node
 * count for no benefit.
 */
public final class ScaleUpCandidatesSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(ScaleUpCandidatesSchedulerTask.class);

    private final Client client;
    private final ClusterService clusterService;
    private final ReaderReplicaExpansionCoordinator expansionCoordinator;
    private final Scheduler.Cancellable task;
    private final AtomicReference<List<ScaleUpCandidateEntry>> latestCandidates = new AtomicReference<>(List.of());

    /**
     * Starts the scheduled evaluation without acting on its own candidates -- equivalent to calling
     * the other constructor with a {@code null} coordinator (policy-only).
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ScaleUpCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected cluster-manager node.
     */
    public ScaleUpCandidatesSchedulerTask(ThreadPool threadPool, TimeValue interval, Client client, ClusterService clusterService) {
        this(threadPool, interval, client, clusterService, null);
    }

    /**
     * Starts the scheduled evaluation.
     *
     * @param threadPool schedules {@link #evaluate()} on a fixed delay.
     * @param interval how often to evaluate.
     * @param client dispatches the cluster-wide {@link ScaleUpCandidatesAction} request.
     * @param clusterService used on every tick to check whether this is currently the elected cluster-manager node.
     * @param expansionCoordinator acts on every evaluation's candidates by expanding them, or
     *                             {@code null} to stay policy-only (no index is ever expanded).
     */
    public ScaleUpCandidatesSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        Client client,
        ClusterService clusterService,
        ReaderReplicaExpansionCoordinator expansionCoordinator
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.expansionCoordinator = expansionCoordinator;
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception -- see ScaleToZeroCandidatesSchedulerTask's own
            // javadoc for why: ClusterService#state() throws an AssertionError, not an Exception, if
            // called before the node's initial cluster state is applied, a real window this task's
            // very first tick or two can land in.
            logger.warn("scale-up candidate evaluation failed, will retry next tick", t);
        }
    }

    void evaluate() {
        if (clusterService.state().nodes().isLocalNodeElectedClusterManager() == false) {
            return; // not our turn -- see class javadoc for why only the cluster-manager runs this
        }
        client.execute(ScaleUpCandidatesAction.INSTANCE, new ScaleUpCandidatesRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ScaleUpCandidatesResponse response) {
                latestCandidates.set(List.copyOf(response.candidates()));
                long candidateCount = response.candidates().stream().filter(ScaleUpCandidateEntry::candidate).count();
                logger.debug(
                    "scale-up evaluation: {} shard(s) observed, {} flagged as candidates",
                    response.candidates().size(),
                    candidateCount
                );
                if (expansionCoordinator != null) {
                    expansionCoordinator.expandCandidates(response.candidates());
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("scale-up candidate evaluation failed, will retry next tick", e);
            }
        });
    }

    /**
     * The most recently completed evaluation's merged candidate list, or an empty list if no
     * evaluation has completed successfully yet.
     */
    public List<ScaleUpCandidateEntry> latestCandidates() {
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
