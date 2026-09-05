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
import org.opensearch.serverless.storage.scheduling.JitteredScheduling;
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
     * When this task marked the local node warming, or {@code 0} if it has not. Only set on the path
     * where <em>this task</em> did the marking and no auto-clear is configured -- see {@link
     * #warnIfWarmingForever} (finding N-1).
     */
    private volatile long selfMarkedAtMillis = 0L;

    /** When the stuck-warming warning was last emitted, so it repeats at a useful cadence rather than every tick. */
    private volatile long lastStuckWarningAtMillis = 0L;

    /** How often to repeat the stuck-warming warning. Five minutes: loud enough to notice, quiet enough not to be noise. */
    private static final long STUCK_WARNING_INTERVAL_MILLIS = 300_000L;

    /**
     * How many times, and how far apart, a failed auto-clear is retried -- see {@link #autoClear}
     * (finding N-2). Geometric from the first delay: 5s, 10s, 20s, 40s, 80s, 160s. Six attempts is
     * comfortably longer than a cluster-manager election or a transient transport disconnect, and
     * every attempt is a single tiny cluster-state request.
     */
    private static final int AUTO_CLEAR_MAX_ATTEMPTS = 6;
    private static final long AUTO_CLEAR_FIRST_RETRY_MILLIS = 5_000L;

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
            // javadoc: ClusterService#state() can throw before the node's initial cluster state is
            // applied, a real window this task's very first tick or two can land in.
            logger.warn("node self-warmup check failed, will retry next tick", t);
        }
    }

    void evaluate() {
        if (selfMarked) {
            // Not simply "nothing left to do" any more: if this task marked the node and nothing is
            // ever going to clear that mark, someone needs to be told (finding N-1).
            warnIfWarmingForever();
            return;
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
        // Finding N-2. This client.execute used to sit outside any try/catch, with
        // markAttemptInFlight cleared only inside the two listener callbacks. A *synchronous* throw
        // -- a NodeClosedException during shutdown, or an IllegalStateException from the action
        // registry on a still-starting node, which is precisely the window the Throwable catch in
        // evaluateSafely exists for -- escaped to that catch, was logged once, and left the flag set
        // for the process lifetime. The node then never marked itself warming, never logged why
        // again, and silently received reader shards while genuinely cold: the exact inverse of the
        // failure this task exists to prevent, reached by the task's own error handling.
        try {
            client.execute(NodeWarmupAction.INSTANCE, new NodeWarmupRequest(localNode.getId(), true), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    markAttemptInFlight.set(false);
                    selfMarked = true;
                    logger.info("self-marked this node [{}] as warming before it enters reader shard rotation", nodeName);
                    if (autoClearDelay.millis() > 0) {
                        threadPool.schedule(() -> autoClear(localNode.getId(), nodeName, 1), autoClearDelay, ThreadPool.Names.GENERIC);
                    } else {
                        selfMarkedAtMillis = System.currentTimeMillis();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    markAttemptInFlight.set(false); // retry on the next tick.
                    logger.warn("failed to self-mark this node [" + nodeName + "] as warming, will retry next tick", e);
                }
            });
        } catch (RuntimeException e) {
            markAttemptInFlight.set(false);
            throw e;
        }
    }

    /**
     * Warns, repeatedly, that this node is warming and nothing will ever clear it.
     *
     * <p><b>Finding N-1.</b> {@code serverless_storage.node_warmup.self_mark_auto_clear_delay}
     * defaults to zero, meaning "never auto-clear", and {@code NodeWarmupAllocationDecider} answers
     * {@code NO} for every reader shard of every serverless-storage index on a warming node. So an
     * operator who turns on {@code self_mark_eval_interval} -- a plausible "enable pre-warm" action
     * -- without also deploying a control plane that issues the clearing call ends up with every
     * reader node permanently warming, every reader shard permanently {@code UNASSIGNED}, and every
     * search-replica query permanently failing. That contract is documented on the setting, so this
     * is a configuration mistake rather than a code defect; what made it a HIGH-severity one is that
     * its entire signal was a single INFO line at startup, after which a total loss of
     * search-replica serving looked like nothing at all.
     *
     * <p>Deliberately a warning and not an automatic clear: clearing a mark this node was told to
     * hold would break the documented contract for the deployments that use it correctly. Making the
     * state loud is the fix that helps the misconfigured deployment without harming the correct one.
     */
    private void warnIfWarmingForever() {
        long markedAt = selfMarkedAtMillis;
        if (markedAt == 0L) {
            return; // either this task did not do the marking, or an auto-clear is scheduled.
        }
        long now = System.currentTimeMillis();
        if (now - lastStuckWarningAtMillis < STUCK_WARNING_INTERVAL_MILLIS) {
            return;
        }
        ClusterState state = clusterService.state();
        DiscoveryNode localNode = state.nodes().getLocalNode();
        if (localNode == null || NodeWarmupCoordinator.currentlyWarmingNames(state).contains(localNode.getName()) == false) {
            selfMarkedAtMillis = 0L; // something cleared it: exactly what is supposed to happen.
            return;
        }
        lastStuckWarningAtMillis = now;
        logger.warn(
            "this node [{}] has been marked warming for {} ms and no auto-clear is configured "
                + "(serverless_storage.node_warmup.self_mark_auto_clear_delay is 0). While it is warming, "
                + "NodeWarmupAllocationDecider refuses every serverless-storage reader shard on this node, so "
                + "search-replica queries against those indices cannot be served here. Either have the control plane "
                + "issue DELETE /_plugins/_serverless/storage/nodes/{}/warming when the node is ready, or set a "
                + "positive auto-clear delay.",
            localNode.getName(),
            now - markedAt,
            localNode.getId()
        );
    }

    /**
     * Clears this node's warming mark, retrying on failure.
     *
     * <p><b>Finding N-2.</b> This used to be a single {@code client.execute} that logged a WARN and
     * gave up. One transient failure -- a {@code NotClusterManagerException} during an election, a
     * momentary transport disconnect -- therefore left the node warming for its entire process
     * lifetime, because {@code selfMarked} was already true and {@code evaluate()} never ran the
     * marking path again. A single dropped packet turning into a permanently unserviceable reader
     * node is not an acceptable failure mode for a best-effort convenience.
     *
     * @param attempt 1-based attempt number; retries stop at {@link #AUTO_CLEAR_MAX_ATTEMPTS}.
     */
    private void autoClear(String nodeId, String nodeName, int attempt) {
        try {
            client.execute(
                NodeWarmupAction.INSTANCE,
                new NodeWarmupRequest(nodeId, false),
                ActionListener.wrap(
                    response -> logger.info("auto-cleared this node [{}]'s warming status after the configured delay", nodeName),
                    e -> scheduleAutoClearRetry(nodeId, nodeName, attempt, e)
                )
            );
        } catch (RuntimeException e) {
            scheduleAutoClearRetry(nodeId, nodeName, attempt, e);
        }
    }

    private void scheduleAutoClearRetry(String nodeId, String nodeName, int attempt, Exception failure) {
        if (attempt >= AUTO_CLEAR_MAX_ATTEMPTS) {
            // Out of retries. Record the mark time so the stuck-warming watchdog above starts
            // shouting about it, rather than letting the node go quiet while unable to serve.
            selfMarkedAtMillis = System.currentTimeMillis();
            logger.error(
                "gave up after "
                    + attempt
                    + " attempts to auto-clear this node ["
                    + nodeName
                    + "]'s warming status; it will keep refusing serverless-storage reader shards until "
                    + "something clears it explicitly",
                failure
            );
            return;
        }
        long delayMillis = AUTO_CLEAR_FIRST_RETRY_MILLIS << (attempt - 1);
        logger.warn(
            "failed to auto-clear this node ["
                + nodeName
                + "]'s warming status (attempt "
                + attempt
                + "), retrying in "
                + delayMillis
                + " ms",
            failure
        );
        try {
            threadPool.schedule(
                () -> autoClear(nodeId, nodeName, attempt + 1),
                TimeValue.timeValueMillis(delayMillis),
                ThreadPool.Names.GENERIC
            );
        } catch (RuntimeException e) {
            // The thread pool is shutting down; there is no later tick to fall back on.
            logger.warn("could not schedule a warming auto-clear retry for node [" + nodeName + "]", e);
        }
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
