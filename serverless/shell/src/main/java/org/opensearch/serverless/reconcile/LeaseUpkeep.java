/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Keeps {@link BackgroundReconciler}'s claims current: renews this node's lease, verifies the heads it holds,
 * reads heads for routing hints and refreshes the fleet's membership.
 */
final class LeaseUpkeep {

    private final BackgroundReconciler reconciler;

    LeaseUpkeep(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
    }

    /**
     * Renews the leases this node holds and releases what it has lost.
     *
     * <p>The one pass that must run on a timer. Everything else here can wait for evidence; a lease
     * cannot, because the evidence that it lapsed is that someone else already took the shard.
     *
     * @return the shards released because this node no longer owns them
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> renewLeases() throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            if (reconciler.renewalDrivenByTimer) {
                // The timer renews the lease; the backstop's job is the half that verifies. Renewing here too
                // was a fourth lease write per TTL that guaranteed nothing the timer's three did not.
                //
                // IfDue, not unconditionally: the scan is one register read per held shard, and running it on
                // every renewal and every backstop alike made it the largest standing cost of an idle node.
                // See NodeLeases#DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS for why a bounded wait is safe here
                // and where the unconditional scan still happens.
                return reconciler.node.leases().verifyHeadsIfDue(reconciler.plane);
            }
            return reconciler.node.leases().heartbeat(reconciler.plane);
        }
    }

    /**
     * Renews this node's lease, and only that: the renewal timer's body.
     *
     * <p>Separated from {@link #verifyHeads()} so the lease can be renewed on its own clock, ahead of and
     * independent from the pass that reads one head per held shard. If the lease had already lapsed by
     * this node's own clock, every writer shard is released <em>before</em> the renewal -- see
     * {@link org.opensearch.serverless.shell.NodeLeases#renewLease}.
     *
     * @return the shards released because the lease had lapsed
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> renewLease() throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            return reconciler.node.leases().renewLease(reconciler.plane);
        }
    }

    /**
     * Reads the head of every held writer shard and releases what this node has lost.
     *
     * @return the shards released
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> verifyHeads() throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            return reconciler.node.leases().verifyHeadsIfDue(reconciler.plane);
        }
    }

    /**
     * Reads the head of every held writer shard now, whatever the interval says.
     *
     * @return the shards released
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> verifyHeadsNow() throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            return reconciler.node.leases().verifyHeads(reconciler.plane);
        }
    }

    /** Whether another node holds a shard's head under a live lease: one register read, against an eviction. */
    boolean heldByAnotherLiveNode(String indexName, int shard) {
        try {
            final var head = reconciler.plane.heads().read(indexName, shard);
            return head.isPresent()
                && head.get().ownerNodeId() != null
                && reconciler.node.localNode().getId().equals(head.get().ownerNodeId()) == false
                && reconciler.plane.heads().isHeld(head.get(), reconciler.plane.clock().getAsLong());
        } catch (Exception e) {
            // Unknown: go on as before, and let the acquisition decide.
            return false;
        }
    }

    boolean headNamesThisNode(String indexName, int shard) {
        try {
            final var head = reconciler.plane.heads().read(indexName, shard);
            return head.isPresent() && reconciler.node.localNode().getId().equals(head.get().ownerNodeId());
        } catch (Exception e) {
            BackgroundReconciler.logger.warn(
                "could not read the head of " + indexName + "[" + shard + "] at the cap; treating it as not ours",
                e
            );
            return false;
        }
    }

    /**
     * Refreshes the membership view if it is older than {@code maxAgeMillis}: one register read, and a lease read
     * per member whose last-read lease has run out -- which is how a departure is noticed.
     *
     * @param maxAgeMillis how old the view may be
     * @throws IOException if the view cannot be read
     */
    public void refreshMembership(long maxAgeMillis) throws IOException {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return;
            }
            reconciler.plane.membership().refreshIfOlderThan(maxAgeMillis);
        }
    }

    /**
     * Refreshes this node's routing hints.
     *
     * @param nowMillis the observer's clock, for hint staleness
     * @return how many hints are held afterwards
     * @throws Exception if the metadata plane cannot be reached
     */
    public int refreshHints(long nowMillis) throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return 0;
            }
            final Map<String, Integer> shardCounts = new java.util.LinkedHashMap<>();
            for (Map.Entry<String, Integer> target : reconciler.wanted) {
                shardCounts.merge(target.getKey(), target.getValue() + 1, Math::max);
            }
            reconciler.hints.refresh(
                (index, shard) -> recentOrReadHead(new ShardId(new org.opensearch.core.index.Index(index, "_na_"), shard)),
                shardCounts,
                nowMillis
            );
            return reconciler.hints.size();
        }
    }

    /** A head the heartbeat read within one renewal, or a fresh read that is then noted for the others. */
    Optional<org.opensearch.serverless.metadata.ShardHead> recentOrReadHead(ShardId shardId) throws IOException {
        final Optional<org.opensearch.serverless.metadata.ShardHead> recent = reconciler.node.sightings()
            .recentHead(
                shardId.getIndexName(),
                shardId.id(),
                Math.max(1_000L, reconciler.plane.leaseTtlMillis() / ReconcileScheduler.RENEWALS_PER_TTL)
            );
        if (recent.isPresent()) {
            return recent;
        }
        final Optional<org.opensearch.serverless.metadata.ShardHead> read = reconciler.plane.heads()
            .read(shardId.getIndexName(), shardId.id());
        reconciler.node.sightings().noteHead(shardId.getIndexName(), shardId.id(), read.orElse(null));
        return read;
    }
}
