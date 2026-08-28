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
import java.util.Collection;
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
 * <p>Its four jobs used to run only as one pass on one timer. They still <em>compose</em> into that
 * pass -- {@link #tick} runs all four in the order below, and that is the backstop -- but each is now
 * callable on its own, because they are triggered by different things and coupling them coupled their
 * latencies for no reason. See {@link ReconcileSignals}.
 *
 * <ol>
 *   <li><b>{@link #renewLeases()}.</b> Renew what is held, release what is lost. Genuinely clock-bound:
 *       a lease is defined in wall-clock terms. Releasing first means the node stops serving a shard it
 *       no longer owns before it does anything else.</li>
 *   <li><b>{@link #activateWanted()}.</b> Take what is wanted but unheld. Losing the race is normal and
 *       silent; the winner holds a live lease and this node simply tries again. Worth running the
 *       instant something suggests an owner is gone, rather than on the next timer.</li>
 *   <li><b>{@link #publishAll()} / {@link #publishDirty()}.</b> Upload what changed. The node knows the
 *       moment a write lands, so waiting for a timer to discover it only widens the loss window.</li>
 *   <li><b>{@link #refreshHints}.</b> Last, so hints reflect the pass's own effects.</li>
 * </ol>
 */
public final class BackgroundReconciler implements Closeable {

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final RoutingHints hints = new RoutingHints();
    private final Set<Map.Entry<String, Integer>> wanted = ConcurrentHashMap.newKeySet();
    private final Map<ShardId, Long> lastPublishedMaxSeqNo = new ConcurrentHashMap<>();
    private final Set<ShardId> dirty = ConcurrentHashMap.newKeySet();
    private final Set<ShardId> fenced = ConcurrentHashMap.newKeySet();

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
     * Runs one full reconciliation pass: the backstop.
     *
     * <p>This is what makes the edges in {@link ReconcileSignals} safe to be best-effort. Every job runs
     * here regardless of whether its signal ever fired, so a lost, dropped or never-sent signal costs
     * latency and never correctness.
     *
     * @param nowMillis the observer's clock, for hint staleness
     * @return what the pass did
     * @throws Exception if the metadata plane cannot be reached
     */
    public TickResult tick(long nowMillis) throws Exception {
        final Set<ShardId> released = renewLeases();
        final Set<ShardId> activated = activateWanted();
        final Set<ShardId> published = publishAll();
        return new TickResult(released, activated, published, refreshHints(nowMillis));
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
        return node.heartbeat(plane);
    }

    /**
     * Tries to take every wanted shard this node does not already hold.
     *
     * <p>Idempotent and safe to run at any time from any number of nodes, because every acquisition is a
     * compare-and-swap. That is what lets a failure signal call it immediately without any check that
     * the failure was real.
     *
     * @return the shards newly acquired
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> activateWanted() throws Exception {
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
        return activated;
    }

    /**
     * Publishes every open shard whose commit has moved on.
     *
     * <p>The backstop's publish: it looks at everything rather than trusting that a write edge fired.
     *
     * @return the shards published
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> publishAll() throws Exception {
        return publish(node.reconciler().openShards());
    }

    /**
     * Publishes only the shards a write has been reported on since the last publish.
     *
     * <p>The edge's publish. Draining the marks before doing the work rather than after is deliberate:
     * a write that lands mid-publish re-marks the shard and gets its own pass, where clearing afterwards
     * would swallow it and leave the last writes of a burst unpublished until the backstop.
     *
     * @return the shards published
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> publishDirty() throws Exception {
        final Set<ShardId> drained = new LinkedHashSet<>(dirty);
        dirty.removeAll(drained);
        return publish(drained);
    }

    /**
     * Reports that a shard has been written to and should be published soon.
     *
     * @param shardId the shard
     */
    public void markDirty(ShardId shardId) {
        dirty.add(shardId);
    }

    private Set<ShardId> publish(Collection<ShardId> candidates) throws Exception {
        final Set<ShardId> published = new LinkedHashSet<>();
        for (ShardId shardId : candidates) {
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
                // forever, on an idle shard -- an object-store bill for saying nothing happened. This
                // guard stays on the edge path too: a spurious mark must not become an upload.
                continue;
            }
            final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
            if (head.isEmpty() || node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
                continue;
            }
            try {
                node.publishShard(shardId, head.get().term());
            } catch (org.opensearch.serverless.store.StaleWriterException e) {
                // A newer term has already published. This node is a zombie for this shard: it read a
                // head that named it, and by the time it wrote, it did not. Stop serving it now rather
                // than at the next renewal, and go look at ownership -- this is exactly the evidence
                // ownershipDoubted exists for.
                node.reconciler().releaseShard(shardId, "fenced while publishing: " + e.getMessage());
                lastPublishedMaxSeqNo.remove(shardId);
                fenced.add(shardId);
                continue;
            }
            lastPublishedMaxSeqNo.put(shardId, maxSeqNo);
            published.add(shardId);
        }
        return published;
    }

    /**
     * Returns and clears the shards this reconciler was fenced out of while publishing.
     *
     * <p>Drained rather than read so a scheduler acting on it cannot act on the same fencing twice.
     *
     * @return the shards fenced since the last call
     */
    public Set<ShardId> drainFenced() {
        final Set<ShardId> drained = Set.copyOf(fenced);
        fenced.removeAll(drained);
        return drained;
    }

    /**
     * Refreshes this node's routing hints.
     *
     * @param nowMillis the observer's clock, for hint staleness
     * @return how many hints are held afterwards
     * @throws Exception if the metadata plane cannot be reached
     */
    public int refreshHints(long nowMillis) throws Exception {
        final Map<String, Integer> shardCounts = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Integer> target : wanted) {
            shardCounts.merge(target.getKey(), target.getValue() + 1, Math::max);
        }
        hints.refresh(plane, shardCounts, nowMillis);
        return hints.size();
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
        dirty.clear();
        fenced.clear();
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
