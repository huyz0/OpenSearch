/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.membership.MembershipSource;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Hands the node-local cluster view to the cluster applier: batches the activations waiting to add to it into one
 * projection, applies the result with every live peer described, and counts what that cost.
 *
 * <p>What the view contains is the node's business, and arrives through a {@link Projection}; this class owns only the
 * order views are applied in and the cost of applying them.
 */
final class LocalViewApplier {

    /** Logs under the node's name, where these messages have always been. */
    private static final Logger logger = LogManager.getLogger(ServerlessNode.class);

    /** Builds the view this node should hold, with what a batch of callers adds. */
    @FunctionalInterface
    interface Projection {
        /**
         * Projects the view.
         *
         * @param extra descriptors to include beyond those the node already serves or hosts
         * @param opening shard assignments being opened right now
         * @return the view
         * @throws java.io.IOException if a descriptor cannot be projected
         */
        ClusterState project(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
            throws java.io.IOException;
    }

    private final String nodeName;
    private final Supplier<String> localNodeId;
    private final Supplier<ClusterService> clusterService;
    private final Supplier<MembershipSource> membershipSource;
    private final Supplier<MetadataPlane> metadataPlane;
    private final Projection projection;

    /**
     * Creates the applier of one node.
     *
     * @param nodeName the node's name, for errors
     * @param localNodeId the node's id, read on every call because the node rebinds it at start
     * @param clusterService the node's cluster service, built after this
     * @param membershipSource the membership a test installed, or null to use the metadata plane's
     * @param metadataPlane the node's metadata plane, or null before it is set
     * @param projection builds the view to apply
     */
    LocalViewApplier(
        String nodeName,
        Supplier<String> localNodeId,
        Supplier<ClusterService> clusterService,
        Supplier<MembershipSource> membershipSource,
        Supplier<MetadataPlane> metadataPlane,
        Projection projection
    ) {
        this.nodeName = nodeName;
        this.localNodeId = localNodeId;
        this.clusterService = clusterService;
        this.membershipSource = membershipSource;
        this.metadataPlane = metadataPlane;
        this.projection = projection;
    }

    /**
     * Serialises projecting a view with applying it, so views are applied in the order they were computed
     * and each one includes everything recorded before it -- two opens projecting at once could otherwise
     * apply an older view last and take away an index the newer one had just added.
     */
    private final Object viewLock = new Object();

    /** One caller's part of a batched view: what it adds, and the view that included it. */
    private record PendingView(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening,
        java.util.concurrent.CompletableFuture<ClusterState> applied) {
    }

    private final java.util.List<PendingView> pendingViews = new java.util.ArrayList<>();

    /**
     * Projects the local view with what a caller adds, and applies it -- for every caller waiting at once, in one projection.
     *
     * <p>A projection covers every index this node hosts or serves, and applying it is the same size again, so its cost
     * grows with the shards held; and every activation made one, one at a time under this lock. At four hundred shards that
     * was a fraction of a second; at twelve hundred an open averaged five seconds and a dead node's shards queued for
     * minutes behind each other's projections. The callers waiting on the lock now hand in what they add, and whichever
     * holds it projects and applies them all at once: a burst of N activations pays for a few projections, not N.
     *
     * <p>A projection that fails is retried for each caller alone, so one index whose view cannot be built fails only its
     * own activation, as before.
     */
    ClusterState projectAndApply(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
        throws Exception {
        final PendingView mine = new PendingView(extra, opening, new java.util.concurrent.CompletableFuture<>());
        synchronized (pendingViews) {
            pendingViews.add(mine);
        }
        synchronized (viewLock) {
            if (mine.applied().isDone() == false) {
                final java.util.List<PendingView> batch;
                synchronized (pendingViews) {
                    batch = new java.util.ArrayList<>(pendingViews);
                    pendingViews.clear();
                }
                applyBatch(batch);
            }
        }
        try {
            return mine.applied().get();
        } catch (java.util.concurrent.ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
    }

    private final AtomicLong viewProjectNanos = new AtomicLong();

    private final AtomicLong viewApplyNanos = new AtomicLong();

    private final AtomicLong viewsApplied = new AtomicLong();

    private ClusterState timedProjectAndApply(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
        throws Exception {
        final long projectingAt = System.nanoTime();
        final ClusterState view = projection.project(extra, opening);
        final long applyingAt = System.nanoTime();
        viewProjectNanos.addAndGet(applyingAt - projectingAt);
        applyLocalView(view, () -> 30_000L);
        viewApplyNanos.addAndGet(System.nanoTime() - applyingAt);
        viewsApplied.incrementAndGet();
        return view;
    }

    /**
     * The local views activations have projected and applied: milliseconds projecting, milliseconds applying, and how many
     * -- fewer than the activations when they were batched.
     */
    long[] localViewMillis() {
        return new long[] {
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(viewProjectNanos.get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(viewApplyNanos.get()),
            viewsApplied.get() };
    }

    private void applyBatch(java.util.List<PendingView> batch) {
        if (batch.size() > 1) {
            final java.util.List<IndexDescriptor> extra = new java.util.ArrayList<>();
            final java.util.List<ShardAssignment> opening = new java.util.ArrayList<>();
            for (PendingView each : batch) {
                extra.addAll(each.extra());
                opening.addAll(each.opening());
            }
            try {
                final ClusterState view = timedProjectAndApply(extra, opening);
                for (PendingView each : batch) {
                    each.applied().complete(view);
                }
                return;
            } catch (Exception e) {
                logger.debug("a batched view of " + batch.size() + " activations failed; projecting each alone", e);
            }
        }
        for (PendingView each : batch) {
            try {
                final ClusterState view = timedProjectAndApply(each.extra(), each.opening());
                each.applied().complete(view);
            } catch (Exception e) {
                each.applied().completeExceptionally(e);
            }
        }
    }

    /**
     * Hands this node a new node-local view. The shell is the second caller of
     * {@link ClusterApplier#onNewClusterState}; {@code Coordinator} is the first, and is absent here.
     *
     * @param state the projected view for this node
     * @param timeout how long to wait for the state to be applied
     * @throws Exception if application fails or times out
     */
    void applyLocalView(ClusterState state, TimeValueLike timeout) throws Exception {
        final ClusterState toApply = withPeers(state);
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> failure = new AtomicReference<>();
        clusterService.get()
            .getClusterApplierService()
            .onNewClusterState("serverless-projected-view", () -> toApply, new ClusterApplier.ClusterApplyListener() {
                @Override
                public void onSuccess(String source) {
                    latch.countDown();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    failure.set(e);
                    latch.countDown();
                }
            });
        if (latch.await(timeout.millis(), TimeUnit.MILLISECONDS) == false) {
            throw new IllegalStateException("projected view was never applied on " + nodeName);
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    /**
     * Adds every live member of the fleet to the view's nodes: {@code DiscoveryNodes}
     * built from the membership view, not the local node alone.
     *
     * <p>Nothing in the data plane looks a peer up by id today, so this is a description rather than a
     * dependency; it is what makes {@code IndexShard}'s node lookups safe should a path ever name a peer,
     * and what lets a plugin reading the cluster state see the fleet. A lease whose address does not parse,
     * or that collides with one already added -- a restarted node whose old lease has not yet expired --
     * is skipped: membership is a hint, and a wrong hint here costs a missing entry, never a failed apply.
     */
    private ClusterState withPeers(ClusterState state) {
        MembershipSource source = membershipSource.get();
        if (source == null && metadataPlane.get() != null) {
            source = metadataPlane.get().membership();
        }
        if (source == null || source.current().isEmpty()) {
            return state;
        }
        final DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(state.nodes());
        boolean changed = false;
        for (NodeLease lease : source.current()) {
            if (lease.nodeId().equals(localNodeId.get()) || state.nodes().nodeExists(lease.nodeId())) {
                continue;
            }
            try {
                nodes.add(peerNode(lease));
                changed = true;
            } catch (Exception e) {
                logger.debug("not describing {} in the local view: {}", lease.nodeId(), e.getMessage());
            }
        }
        return changed ? ClusterState.builder(state).nodes(nodes).build() : state;
    }

    /** A peer as the data plane would see it, from its lease: id, ephemeral id, address and roles all from there. */
    private static DiscoveryNode peerNode(NodeLease lease) throws Exception {
        final String address = lease.address();
        final int colon = address.lastIndexOf(':');
        final String host = address.substring(0, colon).replace("[", "").replace("]", "");
        final TransportAddress transportAddress = new TransportAddress(
            InetAddress.getByName(host),
            Integer.parseInt(address.substring(colon + 1))
        );
        return new DiscoveryNode(
            lease.name(),
            lease.nodeId(),
            lease.ephemeralId(),
            transportAddress.address().getHostString(),
            transportAddress.getAddress(),
            transportAddress,
            ServerlessNode.discoveryAttributesFor(lease.roles()),
            ServerlessNode.discoveryRolesFor(lease.roles()),
            Version.CURRENT
        );
    }
}
