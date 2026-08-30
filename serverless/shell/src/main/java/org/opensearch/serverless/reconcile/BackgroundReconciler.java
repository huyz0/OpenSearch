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

    /**
     * How many shards a node takes on demand before refusing. A reasoned bound, not a measured one:
     * phase 9 measured index residency, never shard-count scaling, so nothing here knows what a node can
     * actually hold.
     */
    public static final int DEFAULT_MAX_SHARDS_HELD = 1000;

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(BackgroundReconciler.class);

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final RoutingHints hints = new RoutingHints();
    private final Set<Map.Entry<String, Integer>> wanted = ConcurrentHashMap.newKeySet();
    private final Map<ShardId, Long> lastPublishedMaxSeqNo = new ConcurrentHashMap<>();
    private final Set<ShardId> dirty = ConcurrentHashMap.newKeySet();
    private final Set<ShardId> fenced = ConcurrentHashMap.newKeySet();
    private volatile boolean demandDriven;
    private volatile int maxShardsHeld = DEFAULT_MAX_SHARDS_HELD;

    /**
     * How long a shard may go unused before a node lets go of it, when idle release is enabled.
     *
     * <p>Five minutes. Derived rather than picked: re-opening costs about 41 object-store requests and
     * holding costs a few reads per tick, so the break-even is on the order of a minute at the default
     * backstop interval. Five sits an order of magnitude above the noise in that estimate, because the
     * asymmetry is not symmetric — holding a shard too long wastes a little money, and releasing one that
     * was about to be used again costs a cold open and the latency a user sees.
     */
    public static final long DEFAULT_IDLE_AFTER_MILLIS = 300_000L;

    /** Zero disables it, which is the library default; {@code ServerlessBootstrap} turns it on. */
    private volatile long idleAfterMillis = 0L;

    /** When each shard was opened, so one that has never been used is not instantly idle. */
    private final java.util.Map<ShardId, Long> openedAt = new java.util.concurrent.ConcurrentHashMap<>();

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
        final Set<ShardId> released = new java.util.LinkedHashSet<>(renewLeases());
        final Set<ShardId> activated = activateWanted();
        final Set<ShardId> published = publishAll();
        // After publishing, so a shard released for idleness has just had its chance to flush.
        released.addAll(releaseIdle(nowMillis));
        return new TickResult(released, activated, published, refreshHints(nowMillis));
    }

    /**
     * Lets go of shards nobody has asked about, so that a node can shrink instead of only ever growing.
     *
     * <p><b>Until this existed, nothing ever released a shard voluntarily.</b> A shard was let go only
     * when ownership was taken away — fenced, reassigned, or the lease lost — so a node that answered one
     * query for an index held that shard for the life of the process, and {@code maxShardsHeld} was a
     * refusal rather than an eviction. "Scale to zero" was something an operator did by killing the
     * process, not something the system did.
     *
     * <p><b>The trade is measured on both sides</b> ({@code m20-fanout-notes.md}). Holding an idle shard
     * costs reads on every reconcile tick, in proportion to how many are held. Releasing one and opening
     * it again costs about 41 object-store requests, of which 12 are data. So releasing pays as soon as a
     * shard stays cold for longer than the re-open cost divided by the per-tick holding cost, and the
     * default below is chosen to sit well beyond that rather than at it — a shard released a moment before
     * it was wanted costs both numbers and gains nothing.
     *
     * <p><b>The order is the correctness argument, and it is not the obvious one.</b> A writer publishes,
     * <em>then</em> releases its head, <em>then</em> closes. Publishing first is not what makes the data
     * safe — the log already does that, and a successor would replay it — it is what stops the release
     * handing the next owner a log to replay for no reason. Releasing the head before closing is the part
     * that matters: a node that closed a shard while the head still named it would answer 503 for that
     * shard until its lease lapsed, having volunteered to become the thing failover exists to route
     * around. If the head cannot be released, the shard is kept.
     *
     * <p>A shard that has never been asked for anything is treated as used when it opened, not as
     * infinitely idle — otherwise a demand-driven node would release each shard immediately after taking
     * it and spin.
     *
     * @param nowMillis the observer's clock
     * @return the shards released for idleness
     */
    public Set<ShardId> releaseIdle(long nowMillis) {
        final Set<ShardId> letGo = new java.util.LinkedHashSet<>();
        if (idleAfterMillis <= 0) {
            return letGo;
        }
        for (ShardId shardId : node.reconciler().openShards()) {
            final long lastUsed = node.reconciler().lastUsed(shardId).orElse(openedAt.getOrDefault(shardId, nowMillis));
            if (nowMillis - lastUsed < idleAfterMillis) {
                continue;
            }
            try {
                if (node.reconciler().readerShards().contains(shardId)) {
                    // A reader holds no head and no claim on anything. Closing it loses nothing at all.
                    node.reconciler().releaseShard(shardId, "idle for " + (nowMillis - lastUsed) + "ms");
                    letGo.add(shardId);
                    continue;
                }
                final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
                if (head.isEmpty() || node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
                    // Not ours any more; renewLeases will deal with it and this must not race it.
                    continue;
                }
                node.publishShard(shardId, head.get().term());
                if (plane.heads().release(shardId.getIndexName(), shardId.id(), node.localNode().getId()) == false) {
                    // Somebody else's head now, or the swap lost. Keep the shard and try again next pass;
                    // closing it while the head still points here is the one outcome to avoid.
                    logger.warn("could not release the head for {} while idle; keeping it open", shardId);
                    continue;
                }
                node.reconciler().releaseShard(shardId, "idle for " + (nowMillis - lastUsed) + "ms");
                letGo.add(shardId);
            } catch (Exception e) {
                // Releasing is an optimisation. Failing to do it costs money and nothing else, so it is
                // logged and retried on the next pass rather than propagated into the tick.
                logger.warn("could not release idle shard " + shardId, e);
            }
        }
        for (ShardId shardId : letGo) {
            openedAt.remove(shardId);
            // Stop trying to take it straight back. A demand-driven node re-acquires on the next write or
            // read for it, which is the point; a node that was told to want it keeps wanting it.
            if (demandDriven) {
                wanted.remove(Map.entry(shardId.getIndexName(), shardId.id()));
            }
        }
        return letGo;
    }

    /**
     * Sets how long a shard may go unused before it is released, or zero to never release.
     *
     * <p>Off by default in the library and on in the daemon, the same split
     * {@link #setDemandDrivenActivation(boolean)} uses and for the same reason: a test that drives ticks
     * by hand should not have shards disappearing underneath it, and a node running unattended should not
     * grow forever.
     *
     * @param idleAfterMillis the idle threshold, or 0 to disable
     * @return this, for chaining
     */
    public BackgroundReconciler setIdleAfterMillis(long idleAfterMillis) {
        this.idleAfterMillis = idleAfterMillis;
        return this;
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
            node.activateWriter(plane, target.getKey(), target.getValue()).ifPresent(shardId -> {
                activated.add(shardId);
                // Its idle clock starts here, so a shard just taken is not immediately a candidate for
                // being given straight back.
                openedAt.put(shardId, plane.clock().getAsLong());
            });
        }
        return activated;
    }

    /**
     * Takes shards nobody owns, on evidence that somebody wants them.
     *
     * <p><b>This is a placement policy, and it is off by default.</b> Everywhere else in this class a
     * node takes only what it was told to want. Here it takes a shard because a request for that shard
     * arrived and the head named no live owner — which makes placement "whoever the client happened to
     * ask" rather than a decision anyone made.
     *
     * <p>That is not obviously wrong; it may be the point. Without it, a shard nobody was told to want
     * stays unowned however many writes arrive for it, and something outside the system has to assign
     * shards — which is the controller this whole design deleted. With it, capacity appears where load
     * appears and no allocator exists at all. But it is a different decision from scheduling, so it is a
     * switch rather than a default, and {@link #setMaxShardsHeld} bounds it: past the cap a node refuses,
     * the caller still sees no owner, and the next node to be asked takes it instead. Admission control
     * without an admission controller.
     *
     * <p>Safe under races regardless: acquisition is a compare-and-swap, so two nodes reaching for the
     * same unowned shard produce one owner and one node that forwards.
     *
     * @param candidates the (index, shard) pairs somebody has asked for
     * @return the shards actually taken
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> activateOnDemand(Collection<Map.Entry<String, Integer>> candidates) throws Exception {
        if (demandDriven == false) {
            return Set.of();
        }
        final Set<ShardId> taken = new LinkedHashSet<>();
        for (Map.Entry<String, Integer> candidate : candidates) {
            if (node.reconciler().openShards().size() >= maxShardsHeld) {
                // Refusing is a routing outcome, not an error. Saying so is the difference between a
                // node that is full and a node that is broken, and only one of them should page anyone.
                logger.info(
                    "not taking {}[{}] on demand: already holding {} shards, the cap",
                    candidate.getKey(),
                    candidate.getValue(),
                    maxShardsHeld
                );
                continue;
            }
            final boolean alreadyHeld = node.reconciler()
                .openShards()
                .stream()
                .anyMatch(s -> s.getIndexName().equals(candidate.getKey()) && s.id() == candidate.getValue());
            if (alreadyHeld) {
                continue;
            }
            node.activateWriter(plane, candidate.getKey(), candidate.getValue()).ifPresent(taken::add);
        }
        return taken;
    }

    /**
     * Turns on taking unowned shards that somebody has asked for.
     *
     * @param demandDriven whether to take shards on demand
     * @return this, for chaining
     */
    public BackgroundReconciler setDemandDrivenActivation(boolean demandDriven) {
        this.demandDriven = demandDriven;
        return this;
    }

    /**
     * Sets how many shards this node will hold before refusing to take more on demand.
     *
     * <p>Bounds only demand-driven activation. A shard this node was explicitly told to want is taken
     * regardless — the cap is protection against unbounded growth from traffic, not a quota on
     * deliberate placement.
     *
     * @param maxShardsHeld the cap
     * @return this, for chaining
     */
    public BackgroundReconciler setMaxShardsHeld(int maxShardsHeld) {
        this.maxShardsHeld = maxShardsHeld;
        return this;
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
