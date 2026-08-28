/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.Closeable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The loop that keeps a node's shards where they should be.
 *
 * <p>Phase 6 left this gap explicitly: leases could expire and a successor could take over, but nothing
 * <em>noticed</em>. A node had to be told to activate. This is the noticing — and it is deliberately not
 * privileged. Every node runs the same loop, any number may run it at once, and a node that stops
 * running it harms nothing but its own shards, because every action it takes goes through the same
 * compare-and-swap as everyone else.
 *
 * <p>One tick does three things, in this order and for a reason:
 *
 * <ol>
 *   <li><b>Heartbeat.</b> Renew what is held, release what is lost. Releasing first means the node stops
 *       serving a shard it no longer owns before it does anything else.</li>
 *   <li><b>Activate what is wanted but unheld.</b> Losing the race is normal and silent; the winner
 *       holds a live lease and this node simply tries again next tick.</li>
 *   <li><b>Refresh hints.</b> Last, so they reflect the tick's own effects.</li>
 * </ol>
 */
public final class BackgroundReconciler implements Closeable {

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final RoutingHints hints = new RoutingHints();
    private final Set<Map.Entry<String, Integer>> wanted = ConcurrentHashMap.newKeySet();
    private final Map<ShardId, Long> lastPublishedMaxSeqNo = new ConcurrentHashMap<>();

    /**
     * Creates a reconciler for one node.
     *
     * @param node the node whose shards this manages
     * @param plane the metadata plane
     */
    public BackgroundReconciler(ServerlessNode node, MetadataPlane plane) {
        this.node = node;
        this.plane = plane;
    }

    /**
     * Declares that this node should try to own a shard, now and after any future loss.
     *
     * @param indexName the index
     * @param shardId the shard number
     */
    public void want(String indexName, int shardId) {
        wanted.add(Map.entry(indexName, shardId));
    }

    /**
     * Stops trying to own a shard. Does not release it; that happens through the head.
     *
     * @param indexName the index
     * @param shardId the shard number
     */
    public void stopWanting(String indexName, int shardId) {
        wanted.remove(Map.entry(indexName, shardId));
    }

    /**
     * Runs one reconciliation pass.
     *
     * @param nowMillis the observer's clock, for hint staleness
     * @return what the pass did
     * @throws Exception if the metadata plane cannot be reached
     */
    public TickResult tick(long nowMillis) throws Exception {
        final Set<ShardId> released = node.heartbeat(plane);

        final Set<ShardId> activated = new LinkedHashSet<>();
        for (Map.Entry<String, Integer> target : wanted) {
            final boolean alreadyHeld = node.reconciler()
                .openShards()
                .stream()
                .anyMatch(s -> s.getIndexName().equals(target.getKey()) && s.id() == target.getValue());
            if (alreadyHeld) {
                continue;
            }
            node.activateWriter(plane, target.getKey(), target.getValue()).ifPresent(activated::add);
        }

        // Publish what has changed. Until this existed, publishShard was only ever called by hand, so
        // the durability window was not "since the last commit" -- it was unbounded, and a node could
        // index for hours and lose all of it. Doing it here rather than on a second timer means it
        // inherits the tick's ordering: a node that lost a shard released it above, so it cannot reach
        // this loop holding something it no longer owns.
        final Set<ShardId> published = new LinkedHashSet<>();
        for (ShardId shardId : node.reconciler().openShards()) {
            if (node.reconciler().readerShards().contains(shardId)) {
                continue;
            }
            final var shard = node.reconciler().shard(shardId);
            if (shard == null) {
                continue;
            }
            final long maxSeqNo = shard.seqNoStats().getMaxSeqNo();
            if (maxSeqNo < 0 || maxSeqNo == lastPublishedMaxSeqNo.getOrDefault(shardId, -1L)) {
                // Nothing new. Publishing anyway would flush a fresh commit and upload it every tick,
                // forever, on an idle shard -- an object-store bill for saying nothing happened.
                continue;
            }
            final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
            if (head.isEmpty() || node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
                continue;
            }
            node.publishShard(shardId, head.get().term());
            lastPublishedMaxSeqNo.put(shardId, maxSeqNo);
            published.add(shardId);
        }

        final Map<String, Integer> shardCounts = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Integer> target : wanted) {
            shardCounts.merge(target.getKey(), target.getValue() + 1, Math::max);
        }
        hints.refresh(plane, shardCounts, nowMillis);

        return new TickResult(released, activated, published, hints.size());
    }

    /**
     * Returns this node's routing hints.
     *
     * @return the hints
     */
    public RoutingHints hints() {
        return hints;
    }

    @Override
    public void close() {
        wanted.clear();
    }

    /** What one reconciliation pass did. */
    public static final class TickResult {

        private final Set<ShardId> released;
        private final Set<ShardId> activated;
        private final Set<ShardId> published;
        private final int hintCount;

        TickResult(Set<ShardId> released, Set<ShardId> activated, Set<ShardId> published, int hintCount) {
            this.released = Set.copyOf(released);
            this.activated = Set.copyOf(activated);
            this.published = Set.copyOf(published);
            this.hintCount = hintCount;
        }

        /**
         * Returns shards whose commits this pass published to the object store.
         *
         * @return the published shards
         */
        public Set<ShardId> published() {
            return published;
        }

        /**
         * Returns shards released because this node lost them.
         *
         * @return the released shards
         */
        public Set<ShardId> released() {
            return released;
        }

        /**
         * Returns shards newly acquired by this pass.
         *
         * @return the activated shards
         */
        public Set<ShardId> activated() {
            return activated;
        }

        /**
         * Returns how many routing hints are held after this pass.
         *
         * @return the hint count
         */
        public int hintCount() {
            return hintCount;
        }

        @Override
        public String toString() {
            return "TickResult[released="
                + released.size()
                + ", activated="
                + activated.size()
                + ", published="
                + published.size()
                + ", hints="
                + hintCount
                + "]";
        }
    }
}
