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

    /**
     * How long a shard must have gone unused before a node at its cap will evict it for a new one.
     *
     * <p>A minute: long enough that a busy shard is never taken from under a caller, short enough that a
     * full node reshapes its working set within a request or two rather than waiting on the idle sweep.
     */
    public static final long DEFAULT_EVICT_AFTER_MILLIS = 60_000L;

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
    private volatile long evictAfterMillis = DEFAULT_EVICT_AFTER_MILLIS;

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
        // And after that, so a reader this node has just let go of for idleness is not looked up again.
        released.addAll(refreshReaders());
        // Views this node is holding for a record that has gone: in-memory to decide, so it costs nothing
        // on a node holding none, and it must not wait for the reaping cadence below.
        closeReleasedViews();
        // The deployment-wide reap is a listing, and every node would otherwise pay one every pass to be
        // told that a feature nobody used is still not being used. A keep-alive is minutes; this is
        // seconds times a small number, which is soon enough and is a fraction of the cost.
        if (passes++ % REAP_EVERY_PASSES == 0) {
            reapExpiredViews();
        }
        // Only what was published, so the sweep costs nothing at all on a shard nobody is writing to.
        sweepPublished(published);
        return new TickResult(released, activated, published, refreshHints(nowMillis));
    }

    /**
     * Lets go of readers whose commit has moved on, so nothing serves a stale one forever.
     *
     * <p><b>A reader is a cache of one commit, and until this existed nothing invalidated it.</b> A search
     * node that opened shard 0 went on answering from the commit it opened for as long as it held the
     * shard, however much the writer published afterwards — no error, no flag, no gap in the coverage it
     * reported. That is the confident wrong answer this design refuses everywhere else, and it was the one
     * place it was being given.
     *
     * <p><b>Release, not reopen.</b> Moving an open reader onto a newer commit would mean handing a live
     * {@code ReadOnlyEngine} a different commit, which core does not offer and should not. Releasing costs
     * the next search an open — and the next search is the only thing that proves the shard is still
     * wanted at all, so a reader nobody comes back for costs one release and then nothing.
     *
     * <p><b>The cost is one register read per reader per pass</b>, which is the price the design already
     * pays to hold a shard at all, and it acts only when the commit has actually changed: an idle index
     * publishes nothing, so nothing is given up and nothing is reopened.
     *
     * <p>Frozen views are exempt, and that is not an optimisation. A view exists precisely to go on serving
     * a commit the writer has moved past; refreshing one would be the feature deleting itself.
     *
     * @return the readers released
     */
    public Set<ShardId> refreshReaders() {
        final Set<ShardId> stale = new LinkedHashSet<>();
        final Set<ShardId> views = node.reconciler().frozenShards();
        for (ShardId shardId : node.reconciler().readerShards()) {
            if (views.contains(shardId)) {
                continue;
            }
            final var opened = node.reconciler().readerCommit(shardId);
            if (opened.isEmpty()) {
                continue;
            }
            try {
                final var current = plane.segmentPublisher(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id())
                    .readManifest();
                if (current.isEmpty()) {
                    // The commit this reader is serving is no longer published at all -- a deleted index,
                    // most likely. Nothing good comes of guessing; the next search will say what is true.
                    continue;
                }
                if (sameCommit(opened.get(), current.get())) {
                    continue;
                }
                node.reconciler().releaseShard(shardId, "the commit it was serving has been superseded");
                stale.add(shardId);
            } catch (Exception e) {
                // Refreshing is an optimisation over being wrong, not a correctness step of its own: a
                // failure here leaves the reader exactly where it was, which is where it would have been
                // if this method did not exist.
                logger.warn("could not check whether the reader for " + shardId + " is stale; keeping it", e);
            }
        }
        return stale;
    }

    /** How many sweeps a blob must be seen unreferenced by before it is deleted. */
    public static final int DEFAULT_SWEEP_GRACE_PASSES = 2;

    private int sweepGracePasses = DEFAULT_SWEEP_GRACE_PASSES;

    /** Per shard, how many consecutive sweeps have found each blob unreferenced. */
    private final Map<ShardId, Map<String, Integer>> unreferencedFor = new ConcurrentHashMap<>();

    /**
     * Sets how many sweeps a blob must be seen unreferenced by before it is deleted.
     *
     * @param sweepGracePasses the count; zero deletes on the first sweep
     * @return this, for chaining
     */
    public BackgroundReconciler setSweepGracePasses(int sweepGracePasses) {
        this.sweepGracePasses = sweepGracePasses;
        return this;
    }

    /**
     * Collects unreferenced segment blobs for the shards this node has just published.
     *
     * <p><b>Until this existed, nothing in a running deployment ever ran the collector.</b> It was reachable
     * from tests and from an operator, and the garbage a failover leaves — a zombie's unpublishable files,
     * segments a merge superseded — accumulated for the life of the deployment. Storage that only grows is
     * not a bug anybody is paged for, which is why it lasted this long.
     *
     * <p><b>Driven by publishing rather than by holding.</b> A shard that nobody is writing to produces no
     * garbage, so sweeping every held shard on a schedule would pay a listing per shard per pass to be told
     * nothing had changed. This sweeps what was just published, and what a previous sweep is still watching
     * — so the cost follows write activity, and an idle deployment pays nothing.
     *
     * <p><b>And only shards this node owns.</b> Two nodes sweeping one shard is not unsafe — the rule is
     * the same for both — but the owner is the only node that knows the shard is not mid-publish somewhere,
     * and the owner is the node whose publishing created the garbage.
     *
     * @param published the shards published by this pass
     * @return the blobs deleted, qualified by term container
     */
    public Set<String> sweepPublished(Collection<ShardId> published) {
        final Set<String> deleted = new LinkedHashSet<>();
        if (plane.blobStore() == null) {
            return deleted;
        }
        final GarbageCollector collector = new GarbageCollector(plane.blobStore(), plane.basePath());
        final Set<ShardId> toSweep = new LinkedHashSet<>(published);
        toSweep.addAll(unreferencedFor.keySet());
        for (ShardId shardId : toSweep) {
            if (node.reconciler().openShards().contains(shardId) == false || node.reconciler().readerShards().contains(shardId)) {
                // Not ours to sweep any more. Forgetting what we had seen is the safe direction: the next
                // owner starts again from nothing and simply deletes later than it could have.
                unreferencedFor.remove(shardId);
                continue;
            }
            final Map<String, Integer> seen = unreferencedFor.getOrDefault(shardId, Map.of());
            // Null means "everything unreferenced, now". Building the eligible set from what a previous
            // sweep saw would make a grace of zero delete nothing on the first sweep and everything on the
            // second -- a knob that looks like it is off and is really set to one.
            Set<String> eligible = null;
            if (sweepGracePasses > 0) {
                eligible = new java.util.HashSet<>();
                for (Map.Entry<String, Integer> entry : seen.entrySet()) {
                    if (entry.getValue() >= sweepGracePasses) {
                        eligible.add(entry.getKey());
                    }
                }
            }
            try {
                final var swept = collector.sweepShard(plane, shardId.getIndexName(), shardId.id(), eligible);
                deleted.addAll(swept.deleted());
                final Map<String, Integer> next = new java.util.HashMap<>();
                for (String candidate : swept.candidates()) {
                    next.put(candidate, seen.getOrDefault(candidate, 0) + 1);
                }
                if (next.isEmpty()) {
                    unreferencedFor.remove(shardId);
                } else {
                    unreferencedFor.put(shardId, next);
                }
            } catch (Exception e) {
                // Reclaiming space is the one job here whose failure costs only money, so it never
                // interrupts a pass and never escalates.
                logger.warn("could not sweep " + shardId + "; the next pass will try again", e);
            }
        }
        return deleted;
    }

    /**
     * Removes frozen views that have expired, and closes what this node was holding for them.
     *
     * <p><b>Expiry existed as an answer to a request and not as a thing that happened.</b> A search
     * quoting a view past its keep-alive was refused, which is the visible half; the record stayed in the
     * object store pinning its blobs against the sweep, and the shards a node had opened for it stayed
     * open — counted against the node's bound, never a candidate for eviction, serving a commit nobody
     * could still ask for. A keep-alive that only stops answering is not a keep-alive.
     *
     * <p>Every node runs this, and that is safe because it is idempotent: deleting a record twice is
     * deleting it once, and the second node's list simply comes back shorter.
     *
     * <p>Closing local shards is done by asking what this node holds rather than by acting on what this
     * pass deleted — another node may have reaped the record, and this node's shards would then be held
     * against a view that no longer exists anywhere.
     *
     * @return how many records were removed by this pass
     */
    public int reapExpiredViews() {
        int reaped = 0;
        try {
            reaped = plane.reapPointsInTime(plane.clock().getAsLong());
        } catch (Exception e) {
            logger.warn("could not reap expired points in time; the next pass will retry", e);
        }
        closeReleasedViews();
        return reaped;
    }

    /** How many passes between deployment-wide reaps of expired views. */
    public static final int REAP_EVERY_PASSES = 10;

    private long passes;

    /**
     * Closes the frozen views this node is holding whose record has gone.
     *
     * <p>Separate from the reap because it is the half with no listing in it: it asks only about views this
     * node has open, so a node holding none does nothing and pays nothing.
     *
     * @return how many views were closed
     */
    public int closeReleasedViews() {
        int closed = 0;
        for (ShardId view : node.reconciler().frozenShards()) {
            final String viewId = view.getIndex().getUUID();
            try {
                final var record = plane.pointInTime(viewId);
                if (record.isEmpty() || record.get().expiredAt(plane.clock().getAsLong())) {
                    closed += node.reconciler().closeFrozenReader(viewId);
                }
            } catch (Exception e) {
                logger.warn("could not check whether the view " + viewId + " is still held; keeping its shards", e);
            }
        }
        return closed;
    }

    /**
     * Compares two commits by what they name rather than by identity.
     *
     * <p>The term alone is not enough: a writer publishes many commits at one term, which is the ordinary
     * case rather than an edge one. The file set alone is not enough either — a failover can produce the
     * same file names in a different term container, and those are different bytes.
     */
    private static boolean sameCommit(org.opensearch.serverless.store.CommitManifest a, org.opensearch.serverless.store.CommitManifest b) {
        return a.term() == b.term() && a.files().equals(b.files());
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
        final Set<ShardId> letGoSet = new java.util.LinkedHashSet<>();
        if (idleAfterMillis <= 0) {
            return letGoSet;
        }
        for (ShardId shardId : node.reconciler().openShards()) {
            final long lastUsed = node.reconciler().lastUsed(shardId).orElse(openedAt.getOrDefault(shardId, nowMillis));
            if (nowMillis - lastUsed < idleAfterMillis) {
                continue;
            }
            if (letGo(shardId, "idle for " + (nowMillis - lastUsed) + "ms")) {
                letGoSet.add(shardId);
            }
        }
        for (ShardId shardId : letGoSet) {
            openedAt.remove(shardId);
            // Stop trying to take it straight back. A demand-driven node re-acquires on the next write or
            // read for it, which is the point; a node that was told to want it keeps wanting it.
            if (demandDriven) {
                wanted.remove(Map.entry(shardId.getIndexName(), shardId.id()));
            }
        }
        return letGoSet;
    }

    /**
     * Gives up the least recently used shard, if one has been idle long enough to be worth giving up.
     *
     * <p><b>Why a node at its cap should evict rather than refuse.</b> The cap exists to bound what a node
     * holds, not to decide which shards it holds. A node that reached it and then refused everything new
     * would serve whatever it happened to acquire first for as long as it ran — the working set frozen at
     * whatever arrived earliest, which is the opposite of what a demand-driven node is for.
     *
     * <p><b>Why there is a grace period, and why it is not zero.</b> Evicting the least recently used shard
     * unconditionally means a node holding N+1 hot shards evicts one to serve another on every request, and
     * spends its life opening and closing shards instead of answering. A shard is only given up if nobody
     * has touched it for {@link #setEvictAfterMillis(long)} — so a full node whose shards are all in use
     * still refuses, and that refusal is a real capacity signal rather than thrash.
     *
     * <p>Shorter than the idle threshold on purpose: idle release is housekeeping that can wait for a tick,
     * and this is a request waiting for room now.
     *
     * @return true if a shard was released and there is room
     */
    private boolean makeRoom() {
        if (evictAfterMillis <= 0) {
            return false;
        }
        final long now = plane.clock().getAsLong();
        ShardId victim = null;
        long victimLastUsed = Long.MAX_VALUE;
        for (ShardId shardId : node.reconciler().openShards()) {
            final long lastUsed = node.reconciler().lastUsed(shardId).orElse(openedAt.getOrDefault(shardId, now));
            if (now - lastUsed < evictAfterMillis) {
                continue;
            }
            if (lastUsed < victimLastUsed) {
                victim = shardId;
                victimLastUsed = lastUsed;
            }
        }
        if (victim == null) {
            return false;
        }
        if (letGo(victim, "evicted to make room, unused for " + (now - victimLastUsed) + "ms") == false) {
            return false;
        }
        openedAt.remove(victim);
        if (demandDriven) {
            wanted.remove(Map.entry(victim.getIndexName(), victim.id()));
        }
        logger.info("evicted {} to make room at the shard cap", victim);
        return true;
    }

    /**
     * Sets how long a shard must have gone unused before a full node will evict it to make room.
     *
     * @param evictAfterMillis the threshold, or 0 to refuse rather than evict
     * @return this, for chaining
     */
    public BackgroundReconciler setEvictAfterMillis(long evictAfterMillis) {
        this.evictAfterMillis = evictAfterMillis;
        return this;
    }

    /**
     * Gives up one shard, in the only order that is safe.
     *
     * <p><b>Publish, then release the head, then close.</b> Publishing first is not what makes the data
     * durable — the log already did that — it is what keeps a successor from having to replay more than it
     * must. Releasing the head before closing is the part that matters: a node that closed a shard while
     * the head still named it would answer 503 for a shard it had told the world it owns.
     *
     * <p>Shared by idle release and by eviction, because two copies of this sequence is two chances for one
     * of them to get the order wrong.
     *
     * @param shardId the shard to give up
     * @param reason why, for the log
     * @return true if it was released
     */
    private boolean letGo(ShardId shardId, String reason) {
        try {
            if (node.reconciler().readerShards().contains(shardId)) {
                // A reader holds no head and no claim on anything. Closing it loses nothing at all.
                node.reconciler().releaseShard(shardId, reason);
                return true;
            }
            final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
            if (head.isEmpty() || node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
                // Not ours any more; renewLeases will deal with it and this must not race it.
                return false;
            }
            node.publishShard(shardId, head.get().term());
            if (plane.heads().release(shardId.getIndexName(), shardId.id(), node.localNode().getId()) == false) {
                // Somebody else's head now, or the swap lost. Keep the shard and try again next pass;
                // closing it while the head still points here is the one outcome to avoid.
                logger.warn("could not release the head for {}; keeping it open", shardId);
                return false;
            }
            node.reconciler().releaseShard(shardId, reason);
            return true;
        } catch (Exception e) {
            // Releasing is an optimisation. Failing to do it costs money and nothing else, so it is
            // logged and retried on the next pass rather than propagated into the tick.
            logger.warn("could not release shard " + shardId, e);
            return false;
        }
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
            // heldShards, not openShards: a frozen view occupies the node as much as any other shard, so
            // it counts against the bound. makeRoom only ever considers openShards, so counting one here
            // can refuse an activation the node has no room for -- which is the point -- but can never
            // take a view away from the caller holding it.
            if (node.reconciler().heldShards().size() >= maxShardsHeld && makeRoom() == false) {
                // Refusing is a routing outcome, not an error. Saying so is the difference between a
                // node that is full and a node that is broken, and only one of them should page anyone.
                //
                // And it now means something stronger than it used to: the node is full *and* every shard
                // it holds has been used recently. Before, a node that reached the cap stayed at it until
                // the idle sweep came round, refusing new shards while holding ones nobody had touched for
                // minutes -- a cap that behaved like a permanent ceiling rather than a working set.
                logger.info(
                    "not taking {}[{}] on demand: holding {} shards, the cap, and all of them are in use",
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
            node.activateWriter(plane, candidate.getKey(), candidate.getValue()).ifPresent(shardId -> {
                taken.add(shardId);
                // Its idle clock starts here, exactly as it does for a shard this node was told to want.
                // It did not, and that was invisible while the only thing reading openedAt was idle
                // release -- which mostly had a lastUsed to go on, because the request that caused the
                // activation stamped one. A shard taken on demand and then never touched again had no age
                // at all, so it could be neither released for idleness nor evicted for room.
                openedAt.put(shardId, plane.clock().getAsLong());
            });
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
