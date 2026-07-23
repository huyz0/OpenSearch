/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupAction;
import org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupRequest;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The in-repo half of node autoscaling design doc's "pre-warm before rotation" -- previously left
 * entirely to an external control plane (or an operator) to drive {@link NodeWarmupCoordinator}
 * directly. This closes the specific gap that leaving it *entirely* external has: the window
 * between a reader-role node joining the cluster and whatever external caller gets around to
 * marking it warming, during which the node is fully allocation-eligible and can receive a cold
 * shard placement before anyone told it to wait.
 *
 * <p>Deliberately narrow in scope -- this task does not do any actual cache prefetching itself.
 * Real boot-set prefetch already happens automatically per shard once one lands ({@code
 * ServerlessStorageLazyDirectoryFactory} already calls {@code LazyBundleDirectory#prefetchBootSet}
 * unconditionally on directory creation), which is precisely why prefetching *before* any shard has
 * landed isn't something this node-level task can do -- there is nothing to prefetch yet. What this
 * task closes is purely the self-marking race above; clearing the mark once genuinely ready is left
 * to whatever external readiness check the deployment already has (see {@link #autoClearDelay}), or
 * an operator/control-plane {@code DELETE .../warming} call as before.
 *
 * <p>Runs on every reader-role node (not cluster-manager-only, unlike this package's other
 * scheduled tasks) -- self-marking is inherently local-node work, there is nothing to fan out or
 * deduplicate across the cluster. Dispatches through {@link NodeWarmupAction} via {@code client}
 * rather than calling {@link NodeWarmupCoordinator} directly, deliberately: {@link
 * NodeWarmupCoordinator} mutates cluster state via a plain {@code
 * ClusterService#submitStateUpdateTask} call, which throws {@code NotClusterManagerException} when
 * invoked from a node that isn't currently the cluster-manager -- exactly the node this task most
 * often runs on, since it runs on every reader-role node, not just the cluster-manager. Routing
 * through {@link NodeWarmupAction} (a {@code TransportClusterManagerNodeAction}) lets core
 * transparently forward the request to whichever node actually is cluster-manager.
 */
public final class NodeSelfWarmupSchedulerTask implements Closeable {

    private static final Logger logger = LogManager.getLogger(NodeSelfWarmupSchedulerTask.class);

    private final ClusterService clusterService;
    private final Client client;
    private final TimeValue autoClearDelay;
    private final ThreadPool threadPool;
    private final Scheduler.Cancellable task;
    private final AtomicBoolean markAttemptInFlight = new AtomicBoolean(false);
    private volatile boolean selfMarked = false;

    /**
     * Starts the scheduled self-warmup check.
     *
     * @param threadPool schedules the periodic check, and (if {@code autoClearDelay} is positive)
     *                   the eventual auto-clear.
     * @param interval how often to check whether this node still needs to self-mark.
     * @param clusterService supplies the cluster state this task reads the local node's attributes
     *                       and current warming set from.
     * @param client dispatches {@link NodeWarmupAction} requests to mark/clear this node's own
     *               warming status -- see this class's own javadoc for why a direct {@link
     *               NodeWarmupCoordinator} call isn't used here.
     * @param autoClearDelay how long after a successful self-mark to automatically clear it again --
     *                       {@link TimeValue#ZERO} (the default) means never auto-clear, leaving the
     *                       node warming until something external clears it, exactly like today's
     *                       fully-external flow. A positive value is only a safety net for
     *                       deployments with no readiness check of their own; a control plane with
     *                       a real readiness check should leave this at zero and clear explicitly.
     */
    public NodeSelfWarmupSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        ClusterService clusterService,
        Client client,
        TimeValue autoClearDelay
    ) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.autoClearDelay = autoClearDelay;
        this.task = threadPool.scheduleWithFixedDelay(this::evaluateSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void evaluateSafely() {
        try {
            evaluate();
        } catch (Throwable t) {
            // Deliberately Throwable, not Exception -- see ScaleToZeroCandidatesSchedulerTask's own
            // javadoc: ClusterService#state() can throw before the node's initial cluster state is
            // applied, a real window this task's very first tick or two can land in.
            logger.warn("node self-warmup check failed, will retry next tick", t);
        }
    }

    void evaluate() {
        if (selfMarked) {
            return; // already marked (or attempted and given up retrying isn't useful) -- nothing left to do.
        }
        ClusterState state = clusterService.state();
        DiscoveryNode localNode = state.nodes().getLocalNode();
        if (localNode == null) {
            return; // local node not yet part of applied cluster state -- try again next tick.
        }
        if ("true".equals(localNode.getAttributes().get(ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE)) == false) {
            selfMarked = true; // not a reader-role node -- this task has nothing to do, ever, on this node.
            return;
        }
        if (NodeWarmupCoordinator.currentlyWarmingNames(state).contains(localNode.getName())) {
            // Something else (an earlier tick that raced, an operator, a control plane) already
            // marked this node -- don't attempt again, and don't schedule an auto-clear for a mark
            // this task didn't itself make, since we can't tell whether the other caller wants one.
            selfMarked = true;
            return;
        }
        if (markAttemptInFlight.compareAndSet(false, true) == false) {
            return; // a previous tick's mark call hasn't completed yet.
        }
        String nodeName = localNode.getName();
        client.execute(NodeWarmupAction.INSTANCE, new NodeWarmupRequest(localNode.getId(), true), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                markAttemptInFlight.set(false);
                selfMarked = true;
                logger.info("self-marked this node [{}] as warming before it enters reader shard rotation", nodeName);
                if (autoClearDelay.millis() > 0) {
                    threadPool.schedule(() -> autoClear(localNode.getId(), nodeName), autoClearDelay, ThreadPool.Names.GENERIC);
                }
            }

            @Override
            public void onFailure(Exception e) {
                markAttemptInFlight.set(false); // retry on the next tick.
                logger.warn("failed to self-mark this node [" + nodeName + "] as warming, will retry next tick", e);
            }
        });
    }

    private void autoClear(String nodeId, String nodeName) {
        client.execute(
            NodeWarmupAction.INSTANCE,
            new NodeWarmupRequest(nodeId, false),
            ActionListener.wrap(
                response -> logger.info("auto-cleared this node [{}]'s warming status after the configured delay", nodeName),
                e -> logger.warn("failed to auto-clear this node [" + nodeName + "]'s warming status", e)
            )
        );
    }

    /** Invokes {@link #evaluate()} synchronously -- test-only visibility. */
    void evaluateForTesting() {
        evaluate();
    }

    /** Whether this task has finished (successfully or definitively skipped) its self-warmup attempt -- test-only visibility. */
    boolean isSelfMarkedForTesting() {
        return selfMarked;
    }

    /** Whether the scheduled check has been cancelled -- test-only visibility, mirroring this package's sibling scheduler tasks. */
    public boolean isCancelledForTesting() {
        return task.isCancelled();
    }

    /** Cancels the scheduled check. */
    @Override
    public void close() {
        task.cancel();
    }
}
