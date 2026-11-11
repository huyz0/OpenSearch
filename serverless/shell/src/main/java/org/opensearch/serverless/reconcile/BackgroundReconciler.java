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
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The loop that keeps a node's shards where they should be.
 *
 * <p>Without it, leases could expire and a successor could take over, but nothing would
 * <em>notice</em>. A node had to be told to activate. This is the noticing — and it is deliberately not
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
 *   <li><b>{@link LeaseUpkeep#refreshHints}.</b> Last, so hints reflect the pass's own effects.</li>
 * </ol>
 */
public final class BackgroundReconciler implements Closeable {

    /**
     * How many shards a node takes on demand before refusing.
     *
     * <p>Measured, now, and memory is not what bounds it: a held shard costs 110-190 KB of heap, writer or reader, so a
     * thousand is under 200 MB. A fleet of 3 GB nodes ran at 1,200 shards each with heap under 1.7 GB and steady writes at
     * 3-8% errors, and refused no search for want of room -- once publishing ran a pass at a time on a pool of its own.
     * Before that, the cost that grew with shards held was publishing: passes piled up on GENERIC and starved the writes.
     * What a higher cap still costs is background store requests, about 240-330 per held shard-hour, and a longer
     * takeover when a node holding that many dies.
     */
    public static final int DEFAULT_MAX_SHARDS_HELD = 1000;

    /**
     * How long a shard must have gone unused before a node at its cap will evict it for a new one.
     *
     * <p>A minute: long enough that a busy shard is never taken from under a caller, short enough that a
     * full node reshapes its working set within a request or two rather than waiting on the idle sweep.
     */
    public static final long DEFAULT_EVICT_AFTER_MILLIS = 60_000L;

    /**
     * How far below the cap eviction clears room to, as a fraction of {@link #maxShardsHeld}.
     *
     * <p><b>What hysteresis buys here, precisely.</b> It does not reduce how many shards get evicted under
     * sustained demand above the cap — a node that needs to hold K new shards still has to give up K old
     * ones, headroom or not. What it reduces is how often {@link #makeRoom()} has to run its victim search
     * at all: that search is O(open shards), and without headroom a burst of arrivals at a full node pays
     * that scan once per arrival, each time freeing exactly enough room for the one shard that triggered
     * it and nothing more. With headroom, one full pass clears room for the whole burst, up to this
     * fraction of the cap, so the next several arrivals find room already there.
     *
     * <p>Five percent, floored at one shard: a fifth of the {@link #DEFAULT_EVICT_AFTER_MILLIS} of grace
     * a shard already gets before it is eligible at all, chosen the same way that constant was — an order
     * of magnitude inside what would visibly cost something (evicting a shard nobody asked to be evicted,
     * unused as it is, costs nothing but the object-store round trip to publish and release it) rather
     * than measured, because nothing has yet run this shell at a scale where the difference is visible.
     */
    public static final double DEFAULT_EVICT_HEADROOM_FRACTION = 0.05;

    static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(BackgroundReconciler.class);

    final ServerlessNode node;
    final MetadataPlane plane;
    final RoutingHints hints = new RoutingHints();
    final Set<Map.Entry<String, Integer>> wanted = ConcurrentHashMap.newKeySet();
    final Map<ShardId, Long> lastPublishedMaxSeqNo = new ConcurrentHashMap<>();
    final Map<ShardId, Long> lastPublishedAtMillis = new ConcurrentHashMap<>();
    /** Per shard, when the oldest write its last publish did not cover was reported: for publishing by age. */
    final Map<ShardId, Long> unpublishedSinceMillis = new ConcurrentHashMap<>();
    volatile long lazyPublishMaxAgeMillis;
    volatile long lazyPublishMaxOperations;
    volatile long lazyPublishTickMillis = 1L;
    final Set<ShardId> dirty = ConcurrentHashMap.newKeySet();
    final Set<ShardId> fenced = ConcurrentHashMap.newKeySet();
    final Set<String> pinnedIndices = ConcurrentHashMap.newKeySet();
    volatile boolean demandDriven;
    /** Set by a running {@link ReconcileScheduler}: the lease is renewed by its timer, so the backstop must not renew it again. */
    volatile boolean renewalDrivenByTimer;
    final ReconcileActivity activity = new ReconcileActivity();
    final Object activationAdmission = new Object();

    /**
     * The activations in flight, one per shard. The renewal timer, the backstop, a failure-triggered pass and
     * a write waiting for its shard can all ask for the same shard at once; the first starts it and the rest
     * wait on the same future, so one shard is never two acquisitions, two fences and two opens racing.
     *
     * <p>It replaced one monitor over both passes, which was the same guarantee bought by serialising every
     * activation on the node: a node taking a hundred cold shards took them one after another, and a write
     * waiting for one shard waited for all the shards ahead of it.
     */
    final Map<Map.Entry<String, Integer>, java.util.concurrent.CompletableFuture<Optional<ShardId>>> activating = new ConcurrentHashMap<>();
    /** Activations run at most this many at a time, on the generic pool; the rest queue. */
    volatile int activationConcurrency = DEFAULT_ACTIVATION_CONCURRENCY;
    /**
     * Activations waiting for a slot: those this node is the preferred taker of first, see {@link #takeoverRank}.
     *
     * <p>First come, first served used to be the order, and when a node died every survivor received writes for every
     * shard it had held, queued the same few hundred activations in the same order, and ran them at the same moment: one
     * of each five won its head and four spent eight seconds on nothing. Taking a dead node's 300 shards ran at one
     * node's pace whatever the cap. Ranked, each survivor does its own share first, and finds the rest taken by the time it
     * reaches them -- which costs a head read, not an activation.
     */
    final java.util.concurrent.PriorityBlockingQueue<QueuedActivation> activationQueue = new java.util.concurrent.PriorityBlockingQueue<>();

    final java.util.concurrent.atomic.AtomicLong activationSequence = new java.util.concurrent.atomic.AtomicLong();

    /** One activation waiting for a slot, ordered by this node's rank for its shard and then by arrival. */
    record QueuedActivation(int rank, long sequence, Map.Entry<String, Integer> key, java.util.concurrent.CompletableFuture<
        Optional<ShardId>> result, Runnable task) implements Comparable<QueuedActivation> {
        @Override
        public int compareTo(QueuedActivation other) {
            final int byRank = Integer.compare(rank, other.rank);
            return byRank != 0 ? byRank : Long.compare(sequence, other.sequence);
        }
    }

    /** Where a node stands among the live members as the taker of a shard: 0 for the one member every node agrees should try first, by rendezvous hashing, and higher for the rest. */
    public static int takeoverRank(String self, java.util.Collection<String> members, String indexName, int shard) {
        return ShardActivations.takeoverRank(self, members, indexName, shard);
    }

    final java.util.concurrent.atomic.AtomicInteger activationsRunning = new java.util.concurrent.atomic.AtomicInteger();
    /** Guards the shard cap across concurrent on-demand activations, with the slots they have reserved. */
    final Object capLock = new Object();
    int capReserved;
    volatile int maxShardsHeld = DEFAULT_MAX_SHARDS_HELD;
    volatile long evictAfterMillis = DEFAULT_EVICT_AFTER_MILLIS;
    volatile double evictHeadroomFraction = DEFAULT_EVICT_HEADROOM_FRACTION;

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
    volatile long idleAfterMillis = 0L;

    /** When each shard was opened, so one that has never been used is not instantly idle. */
    final java.util.Map<ShardId, Long> openedAt = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Creates a reconciler for one node.
     *
     * @param node the node whose shards this manages
     * @param plane the metadata plane
     */
    public BackgroundReconciler(ServerlessNode node, MetadataPlane plane) {
        this.node = node;
        this.plane = plane;
        this.collector = new GarbageCollector(plane.blobStore(), plane.basePath());
        // Readers and frozen views are admitted under this loop's cap and eviction, not a constant.
        node.setShardAdmission(new ServerlessNode.ShardAdmission() {
            @Override
            public int maxShardsHeld() {
                return BackgroundReconciler.this.maxShardsHeld;
            }

            @Override
            public boolean makeRoom() {
                return BackgroundReconciler.this.makeRoom();
            }
        });
    }

    /**
     * Declares that this node should try to own a shard, now and after any future loss.
     *
     * @param indexName the index
     * @param shardId the shard number
     */
    public void want(String indexName, int shardId) {
        if (activity.isClosed()) {
            return;
        }
        wanted.add(Map.entry(indexName, shardId));
    }

    /**
     * Stops trying to own a shard. Does not release it; that happens through the head.
     *
     * @param indexName the index
     * @param shardId the shard number
     */
    public void stopWanting(String indexName, int shardId) {
        if (activity.isClosed()) {
            return;
        }
        wanted.remove(Map.entry(indexName, shardId));
    }

    /** Runs one full reconciliation pass: the backstop. */
    public TickResult tick(long nowMillis) throws Exception {
        return pass.tick(nowMillis);
    }

    /** Lets go of readers whose commit has moved on, so nothing serves a stale one forever. */
    public Set<ShardId> refreshReaders() {
        return sweeps.refreshReaders();
    }

    /** How many sweeps a blob must be seen unreferenced by before it is deleted. */
    public static final int DEFAULT_SWEEP_GRACE_PASSES = 2;

    int sweepGracePasses = DEFAULT_SWEEP_GRACE_PASSES;

    /** Per shard, how many consecutive sweeps have found each blob unreferenced. */
    final Map<ShardId, Map<String, Integer>> unreferencedFor = new ConcurrentHashMap<>();

    /** The manifest each shard last published from this node, for the sweep's gate. */
    final Map<ShardId, org.opensearch.serverless.store.CommitManifest> lastManifests = new ConcurrentHashMap<>();

    /** Per shard, the files of the manifest the last sweep ran against. */
    final Map<ShardId, Set<String>> sweptAgainst = new ConcurrentHashMap<>();

    /** Shards whose next sweep must run whatever their manifest says. */
    final Set<ShardId> forceSweep = java.util.concurrent.ConcurrentHashMap.newKeySet();

    void forceSweepOfOpenWriters() {
        for (ShardId shardId : node.reconciler().openShards()) {
            if (node.reconciler().isReader(shardId) == false) {
                forceSweep.add(shardId);
                pendingSweeps.add(shardId);
            }
        }
    }

    /**
     * Publishes each shard by the age of its oldest unpublished write, and by how much it holds unpublished, rather
     * than soon after every write: for a node whose writes are durable in its own write log.
     *
     * <p>A publish of a commit that adds one small segment is five or more PUTs, and with one write log per node it is
     * the largest request cost a busy shard has left. Durability does not need it, the log has every acknowledged write.
     * What it buys is search freshness, since searches read published commits, and a shorter log: the log's trim point
     * is the oldest record any shard has not published, so a shard that never publishes keeps every blob after its
     * first unpublished write. Publishing by age bounds both, the delay before a write is searchable and how much of
     * the log is kept; publishing by size bounds what a successor replays.
     *
     * <p>The edges and the backstop both follow it. A release, a hand-off and an idle release publish regardless.
     *
     * <p><b>By the index's refresh interval.</b> An index that sets {@code index.refresh_interval} is published once its
     * oldest unpublished write is that old, and so never more often than that, except by size; one that sets
     * {@code -1} is published only by size or on release; one that sets nothing waits {@code maxAgeMillis}. Every due
     * time is rounded up to the node's tick, the window its write log ships on, so the shards that come due together
     * go out in one pass.
     *
     * @param maxAgeMillis how long a write to an index that sets no refresh interval may wait to be published; zero or
     *     less to publish as before
     * @param maxOperations how many unpublished operations make a shard due sooner; zero or less for never sooner
     * @param tickMillis the node's tick, which every due time is rounded up to; one or less for none
     * @return this, for chaining
     */
    public BackgroundReconciler setLazyPublish(long maxAgeMillis, long maxOperations, long tickMillis) {
        this.lazyPublishMaxAgeMillis = maxAgeMillis;
        this.lazyPublishMaxOperations = maxOperations;
        this.lazyPublishTickMillis = Math.max(1L, tickMillis);
        return this;
    }

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
     * Sets how long a blob must have been unreferenced, by the plane's clock, before a graced sweep may
     * delete it -- on top of the pass count.
     *
     * <p>Zero in the library and {@link GarbageCollector#DEFAULT_MINIMUM_UNREFERENCED_MILLIS} in the
     * daemon, the same split idle release and demand-driven activation use, and for the same reason: a
     * test that drives passes by hand at one clock value is asserting the pass count, and a node running
     * unattended is the one whose passes can come round a second apart under a burst of publishes --
     * which is when two passes are not "long enough" for a freeze on a slow store to have written its
     * record. {@code ServerlessBootstrap} turns it on.
     *
     * @param millis the floor, or zero for a pure pass count
     * @return this, for chaining
     */
    public BackgroundReconciler setMinimumUnreferencedMillis(long millis) {
        collector.setMinimumUnreferencedMillis(millis);
        return this;
    }

    /** Collects unreferenced segment blobs for the shards this node has just published. */
    public Set<String> sweepPublished(Collection<ShardId> published) {
        return sweeps.sweepPublished(published);
    }

    /** Removes frozen views that have expired, and closes what this node was holding for them. */
    public int reapExpiredViews() {
        return sweeps.reapExpiredViews();
    }

    /** How many passes between deployment-wide reaps of expired views. */
    public static final int REAP_EVERY_PASSES = 10;

    /** How many reaps between sweeps of descriptor tombstones past their quarantine: about hourly at the defaults. */
    public static final int TOMBSTONE_SWEEP_EVERY_REAPS = 12;

    final java.util.concurrent.atomic.AtomicBoolean sweepingTombstones = new java.util.concurrent.atomic.AtomicBoolean();
    volatile java.util.concurrent.Future<?> tombstoneSweep = java.util.concurrent.CompletableFuture.completedFuture(null);

    /** How many reaps between slices of the orphaned-shard sweep: about hourly at the defaults. */
    public static final int ORPHAN_SLICE_EVERY_REAPS = 12;

    /**
     * How many shard containers one orphan slice visits by default. A slice costs about this many descriptor
     * reads at worst, once an hour per node, which keeps the background's per-node cost flat.
     */
    public static final int DEFAULT_ORPHAN_SLICE_BUDGET = 100;

    volatile int orphanSliceBudget = DEFAULT_ORPHAN_SLICE_BUDGET;
    final java.util.concurrent.atomic.AtomicBoolean slicingOrphans = new java.util.concurrent.atomic.AtomicBoolean();
    volatile java.util.concurrent.Future<?> orphanSlice = java.util.concurrent.CompletableFuture.completedFuture(null);

    /**
     * Sets how many shard containers a background orphan slice visits; zero turns the background sweep off.
     *
     * @param budget containers per slice
     */
    public void setOrphanSliceBudget(int budget) {
        this.orphanSliceBudget = Math.max(0, budget);
    }

    /**
     * Returns the most recently started orphan slice, for a caller that needs to wait on it.
     *
     * @return the slice's future, complete if none is running
     */
    public java.util.concurrent.Future<?> orphanSlice() {
        return orphanSlice;
    }

    final java.util.concurrent.atomic.AtomicBoolean reclaiming = new java.util.concurrent.atomic.AtomicBoolean();
    volatile java.util.concurrent.Future<?> reclaimSweep = java.util.concurrent.CompletableFuture.completedFuture(null);

    /**
     * Returns the most recently started reclaim drain, for a caller that needs to wait on it.
     *
     * @return the drain's future, complete if none is running
     */
    public java.util.concurrent.Future<?> reclaimSweep() {
        return reclaimSweep;
    }

    /**
     * Returns the most recently started tombstone sweep, for a caller that needs to wait on it.
     *
     * @return the sweep's future, complete if none is running
     */
    public java.util.concurrent.Future<?> tombstoneSweep() {
        return tombstoneSweep;
    }

    /**
     * How many reaps between readings of the deployment's pins: two listings each, so every third reap
     * -- a quarter of an hour at the defaults -- keeps an idle node's listings at one per ten passes
     * plus a fraction, rather than three.
     */
    public static final int PINS_EVERY_REAPS = 3;

    /**
     * The one collector this loop sweeps with. One instance rather than one per pass because it remembers
     * when each candidate was first seen unreferenced, which is what a wall-clock floor is measured from;
     * a fresh collector per pass has never seen anything and would start every clock again.
     */
    final GarbageCollector collector;

    /**
     * The pins -- live view records and snapshot records -- as of the last reap, so a pin that came or
     * went anywhere in the deployment is noticed here. The per-node sweep gate can see only what this node
     * did: a manifest it published losing a file, a view it reaped. A view reaped on another node or a
     * snapshot deleted anywhere freed files a quiet index would otherwise hold until its next merge.
     */
    Set<String> lastPins;

    long reaps;

    /** How many passes between full re-reads of this node's truth from the metadata plane. */
    public static final int RESYNC_EVERY_PASSES = 20;

    long passes;

    /** Lets go of shards nobody has asked about, so that a node can shrink instead of only ever growing. */
    public Set<ShardId> releaseIdle(long nowMillis) {
        return eviction.releaseIdle(nowMillis);
    }

    boolean makeRoom() {
        return eviction.makeRoom();
    }

    /** Gives work to a member with room: a node nearly at its cap, when another live writer has room, lets go of shards past their eviction grace -- least recently used first, down to {@link #HAND_OFF_DOWN_TO} of its cap and at most a tenth of it a pass -- so the next write for one is taken where there is room. */
    public int handOffToMemberWithRoom() {
        return eviction.handOffToMemberWithRoom();
    }

    /** The share of its cap a node hands off down to when a member has room. */
    public static final double HAND_OFF_DOWN_TO = 0.8;

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
     * Sets how far below the cap eviction clears room to, as a fraction of the cap.
     *
     * @param evictHeadroomFraction the fraction; 0 clears exactly one slot at a time, matching the
     *     behaviour before hysteresis existed
     * @return this, for chaining
     */
    public BackgroundReconciler setEvictHeadroomFraction(double evictHeadroomFraction) {
        this.evictHeadroomFraction = evictHeadroomFraction;
        return this;
    }

    /**
     * Marks an index's shards as never evicted, on this node.
     *
     * <p><b>What this is, and what it deliberately is not.</b> It is a node-local runtime setting, exactly
     * like {@link #setMaxShardsHeld} or {@link #setEvictAfterMillis} — set once per node, not a durable
     * per-index record every node in a deployment discovers on its own. Building the durable version would
     * mean a new register namespace, a REST surface to manage it, and a place in the reconcile loop to
     * read it every tick; nothing has asked for an index to be pinned deployment-wide yet, only for a way
     * to say "this stays resident" on the node serving it — the same shape every other capacity knob here
     * already has.
     *
     * <p>Pinning exempts an index's shards from eviction under cap pressure ({@link #makeRoom}) and from
     * idle release ({@link #releaseIdle}) alike: "this stays resident" would be a thin promise if it meant
     * only "unless a shard limit is reached," and a thinner one still if a busy hour and a quiet one gave
     * different answers.
     *
     * @param indexName the index
     * @return this, for chaining
     */
    public BackgroundReconciler pin(String indexName) {
        pinnedIndices.add(indexName);
        return this;
    }

    /**
     * Reverses {@link #pin}. A shard already past its idle or eviction threshold at the moment this is
     * called becomes a candidate on the next pass, not immediately — unpinning is not itself a trigger.
     *
     * @param indexName the index
     * @return this, for chaining
     */
    public BackgroundReconciler unpin(String indexName) {
        pinnedIndices.remove(indexName);
        return this;
    }

    /**
     * Reports whether an index's shards are currently pinned on this node.
     *
     * @param indexName the index
     * @return true if pinned
     */
    public boolean isPinned(String indexName) {
        return pinnedIndices.contains(indexName);
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

    /** Renews the leases this node holds and releases what it has lost. */
    public Set<ShardId> renewLeases() throws Exception {
        return leases.renewLeases();
    }

    /** Renews this node's lease, and only that: the renewal timer's body. */
    public Set<ShardId> renewLease() throws Exception {
        return leases.renewLease();
    }

    /** Reads the head of every held writer shard and releases what this node has lost. */
    public Set<ShardId> verifyHeads() throws Exception {
        return leases.verifyHeads();
    }

    /**
     * Tells the loop whether a renewal timer is running, so the backstop does not renew as well.
     *
     * @param renewalDrivenByTimer true while a {@link ReconcileScheduler} is renewing the lease
     * @return this, for chaining
     */
    public BackgroundReconciler setRenewalDrivenByTimer(boolean renewalDrivenByTimer) {
        this.renewalDrivenByTimer = renewalDrivenByTimer;
        return this;
    }

    /** Tries to take every wanted shard this node does not already hold. */
    public Set<ShardId> activateWanted() throws Exception {
        return activations.activateWanted();
    }

    /** Takes shards nobody owns, on evidence that somebody wants them. */
    public Set<ShardId> activateOnDemand(Collection<Map.Entry<String, Integer>> candidates) throws Exception {
        return activations.activateOnDemand(candidates);
    }

    // A doubt about a shard whose head names another node with a live lease is not acted on: the acquisition would be
    // refused. Raised in the lease's last seconds by every write that could not reach a dying owner, those doubts queued
    // the shard on every survivor, and all of them ran at once when the lease ran out -- 3,699 doubt activations across
    // five survivors for a dead node's 274 shards. The departure pass, and the doubts after the lease has run out, which
    // are left to the survivor named for the shard, take it.

    /** Takes one shard nobody owns because a request for it is waiting, and returns when it is open here -- or when it is clear it will not be. */
    public java.util.concurrent.CompletableFuture<Optional<ShardId>> activateForRequest(String indexName, int shard) {
        return activations.activateForRequest(indexName, shard);
    }

    final java.util.concurrent.atomic.AtomicLong activationsDone = new java.util.concurrent.atomic.AtomicLong();

    /** Where queued activations came from, in {@link #activationSources} order. */
    public static final List<String> ACTIVATION_SOURCES = List.of("wanted", "doubt", "request", "departure", "janitor");

    final java.util.concurrent.atomic.AtomicLongArray activationsBySource = new java.util.concurrent.atomic.AtomicLongArray(
        ACTIVATION_SOURCES.size()
    );

    void countSource(String source) {
        activationsBySource.incrementAndGet(ACTIVATION_SOURCES.indexOf(source));
    }

    /**
     * Activations asked for since the node started, by where the asking came from: which path fills the queue when it is
     * deep.
     *
     * @return counts in {@link #ACTIVATION_SOURCES} order
     */
    public long[] activationSources() {
        final long[] out = new long[ACTIVATION_SOURCES.size()];
        for (int i = 0; i < out.length; i++) {
            out[i] = activationsBySource.get(i);
        }
        return out;
    }

    final java.util.concurrent.atomic.AtomicLong activationWaitNanos = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong activationRunNanos = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong activationWaitMax = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong activationRunMax = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Returns the activation queue's figures, and starts the maxima over.
     *
     * @return the figures
     */
    public ActivationStats activationStats() {
        return new ActivationStats(
            activationQueue.size(),
            activationsRunning.get(),
            activationsDone.get(),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationWaitNanos.get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationRunNanos.get()),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationWaitMax.getAndSet(0L)),
            java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(activationRunMax.getAndSet(0L)),
            currentActivationLimit()
        );
    }

    /**
     * Whether the shard's head already names this node, for a shard it does not have open.
     *
     * <p>Such a shard is not a new one competing for room: it is this node's already, every peer forwards to it,
     * and nobody else can take it while this node's lease is live. Refusing it at the cap left it answering
     * "acquiring, retry" for as long as the node lived -- the fleet run found one holding an acknowledged write
     * nobody replayed. Giving the head back instead would free it with its published commit behind its log, and a
     * shard nobody owns is read from that commit. So it is opened, over the cap if it must be; the next eviction
     * brings the node back under it the proper way, publishing before it lets go.
     */
    /** How far past its cap a node may take a dead member's shards, as a fraction of the cap. */
    public static final double DEFAULT_TAKEOVER_OVERCOMMIT = 0.2;

    volatile double takeoverOvercommit = DEFAULT_TAKEOVER_OVERCOMMIT;

    /**
     * Sets how far past its cap a node may take a dead member's shards; 0 turns overcommitting off. Bounded in memory:
     * a held shard costs 110-190 KB of heap, so a fifth of a thousand is about 40 MB.
     *
     * @param fraction of the cap, at least 0
     * @return this, for chaining
     */
    public BackgroundReconciler setTakeoverOvercommit(double fraction) {
        this.takeoverOvercommit = Math.max(0.0, fraction);
        node.setTakeoverOvercommit(this.takeoverOvercommit);
        return this;
    }

    /** Brings a node that took a dead member's shards past its cap back down to it, a tenth of the cap a pass, and only shards past their eviction grace -- so the shards just taken, which are the ones being written, stay. */
    public int trimToCap() {
        return activations.trimToCap();
    }

    /** Takes over the shards a node held when its lease ran out, rather than waiting for a request to find each one. */
    public List<Map.Entry<String, Integer>> takeOverFrom(String deadNodeId) {
        return janitor.takeOverFrom(deadNodeId);
    }

    /** When a node's lease runs out, read from the store rather than from the membership view. */
    public java.util.OptionalLong leaseLiveUntil(String nodeId) throws java.io.IOException {
        return janitor.leaseLiveUntil(nodeId);
    }

    /** The second look at a departed node's shards: any still named to it are taken now. */
    public void takeOverRemaining(String deadNodeId, List<Map.Entry<String, Integer>> claims) {
        janitor.takeOverRemaining(deadNodeId, claims);
    }

    /** The second look at a departed node's shards, or the last. */
    public List<Map.Entry<String, Integer>> takeOverRemaining(String deadNodeId, List<Map.Entry<String, Integer>> claims, boolean last) {
        return janitor.takeOverRemaining(deadNodeId, claims, last);
    }

    /** How many shards a janitor pass looks at, at most. */
    public static final int DEFAULT_JANITOR_BUDGET = 200;

    /**
     * How many shards a janitor pass takes to replay, at most: each is a real activation on this node's activation
     * threads, though it holds a cap slot only until it is published and given back.
     */
    public static final int DEFAULT_JANITOR_REPLAYS = 32;

    final java.util.concurrent.atomic.AtomicLong janitorCursor = new java.util.concurrent.atomic.AtomicLong();

    /** How many dead nodes' write logs a janitor pass looks at, at most: each is a fence and a few reads per shard named. */
    public static final int DEFAULT_NODE_LOGS_PER_PASS = 4;

    final java.util.concurrent.atomic.AtomicLong nodeLogsCollected = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Returns how many dead nodes' write logs this node's janitor has deleted.
     *
     * @return the count
     */
    public long nodeLogsCollected() {
        return nodeLogsCollected.get();
    }

    /** Cleans up after crashed nodes, a bounded amount at a time. */
    public JanitorPass sweepAbandoned(int budget, int replays) throws IOException {
        return janitor.sweepAbandoned(budget, replays);
    }

    /** Dead owners' shards given back without being taken, since this node started. */
    final java.util.concurrent.atomic.AtomicLong settledClean = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Returns how many dead owners' shards this node has given back without taking them.
     *
     * @return the count
     */
    public long settledClean() {
        return settledClean.get();
    }

    /**
     * The membership this loop's plane consults, for the scheduler to hear departures from.
     *
     * @return the membership
     */
    public org.opensearch.serverless.membership.MembershipSource membership() {
        return plane.membership();
    }

    /** Refreshes the membership view if it is older than {@code maxAgeMillis}: one register read, and a lease read per member whose last-read lease has run out -- which is how a departure is noticed. */
    public void refreshMembership(long maxAgeMillis) throws IOException {
        leases.refreshMembership(maxAgeMillis);
    }

    /** How many shards a node activates at once by default, or the most when the limit adapts. */
    public static final int DEFAULT_ACTIVATION_CONCURRENCY = 8;

    /** Where the adaptive activation limit starts, and the fewest it falls to. */
    static final int ACTIVATION_LIMIT_START = 8;
    static final int ACTIVATION_LIMIT_FLOOR = 4;

    /** How long an activation that opened a shard should take, by default; longer means the store is the limit. */
    public static final long DEFAULT_ACTIVATION_TARGET_MILLIS = 2_000L;

    volatile boolean adaptiveActivation;
    volatile long activationTargetMillis = DEFAULT_ACTIVATION_TARGET_MILLIS;

    /**
     * Lets the number of shards activated at once follow the store, up to the activation concurrency; off, it is that number.
     *
     * <p>Off by default. On a single-drive store it sat at its floor and gained nothing over a fixed eight; it is for a store
     * that takes many requests at once -- S3 -- where an activation is a few dozen fast requests and eight at a time leaves
     * most of the store's throughput unused.
     *
     * @param adaptive whether the limit adapts
     * @return this, for chaining
     */
    public BackgroundReconciler setAdaptiveActivation(boolean adaptive) {
        this.adaptiveActivation = adaptive;
        activations.drainActivations();
        return this;
    }

    /**
     * Sets how long an activation that opened a shard should take before the adaptive limit falls.
     *
     * @param millis the target
     * @return this, for chaining
     */
    public BackgroundReconciler setActivationTargetMillis(long millis) {
        this.activationTargetMillis = Math.max(1L, millis);
        return this;
    }

    final Object activationLimitLock = new Object();
    double activationLimit = ACTIVATION_LIMIT_START;
    double activationRunEwmaMillis = -1;
    long activationLimitCutAtNanos = Long.MIN_VALUE;

    /** The activations allowed at once now: the adaptive limit, under the configured most. */
    public int currentActivationLimit() {
        return activations.currentActivationLimit();
    }

    volatile boolean pacedByWrites = false;

    /**
     * Sets whether activations at once follow the write path's limiter: off by default, until a fleet run says it is worth it.
     *
     * @param paced whether to
     * @return this, for chaining
     */
    public BackgroundReconciler setActivationPacedByWrites(boolean paced) {
        this.pacedByWrites = paced;
        activations.drainActivations();
        return this;
    }

    /** Moves the number of shards activated at once with how fast the store takes them, as the write path's limiter does for appends: down by a quarter when activations run long, at most once a second; up by one a window while they run well inside the target. */
    public void adaptActivationLimit(long runNanos) {
        activations.adaptActivationLimit(runNanos);
    }

    /**
     * Sets how many shards this node activates at once.
     *
     * @param concurrency the bound, at least one
     * @return this, for chaining
     */
    public BackgroundReconciler setActivationConcurrency(int concurrency) {
        this.activationConcurrency = Math.max(1, concurrency);
        activations.drainActivations();
        return this;
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

    /** Publishes every open shard whose commit has moved on. */
    public Set<ShardId> publishAll() throws Exception {
        return publishing.publishAll();
    }

    /** Publishes only the shards a write has been reported on since the last publish. */
    public Set<ShardId> publishDirty() throws Exception {
        return publishing.publishDirty();
    }

    /** Publishes the shards written to since their last publish that are due for another one. */
    public Set<ShardId> publishDirty(long nowMillis, long minIntervalMillis, long maxUnpublishedOperations) throws Exception {
        return publishing.publishDirty(nowMillis, minIntervalMillis, maxUnpublishedOperations);
    }

    /** Whether any marked shard is due for a publish now, by the rule {@link #publishDirty(long, long, long)} applies. */
    public boolean hasDirtyDue(long nowMillis, long minIntervalMillis, long maxUnpublishedOperations) {
        return publishing.hasDirtyDue(nowMillis, minIntervalMillis, maxUnpublishedOperations);
    }

    /** Reports that a shard has been written to and should be published soon. */
    public void markDirty(ShardId shardId) {
        publishing.markDirty(shardId);
    }

    /** Whether any shard has been written to since it was last published. */
    public boolean hasDirty() {
        return publishing.hasDirty();
    }

    /**
     * Returns the sequence number up to which this node's last publish of a shard is certain to have carried
     * every operation, for the stats endpoint's publish-lag figure.
     *
     * @param shardId the shard
     * @return the processed checkpoint read before the last publish's flush, or empty if this node has never
     *         published the shard
     */
    public java.util.OptionalLong lastPublishedMaxSeqNo(ShardId shardId) {
        final Long published = lastPublishedMaxSeqNo.get(shardId);
        return published == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(published);
    }

    /** Returns and clears the shards this reconciler was fenced out of while publishing. */
    public Set<ShardId> drainFenced() {
        return publishing.drainFenced();
    }

    /**
     * Returns this node's routing hints.
     *
     * @return the hints
     */
    public RoutingHints hints() {
        return hints;
    }

    /** Stops new work, settles queued callers, and drains active I/O before shutdown handover. */
    @Override
    public void close() {
        activity.stopAccepting();
        final List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending;
        synchronized (activationAdmission) {
            pending = List.copyOf(activating.values());
            activating.clear();
            activationQueue.clear();
        }
        // Unblock passes waiting for these futures before draining those passes. No admission lock is
        // held while completing a future, since its callbacks may themselves ask for an activation.
        pending.forEach(future -> future.complete(Optional.empty()));
        activity.close();
        // All submissions are now finished; cancel cleanup jobs still waiting on an executor.
        tombstoneSweep.cancel(false);
        orphanSlice.cancel(false);
        reclaimSweep.cancel(false);
        sweepingTombstones.set(false);
        slicingOrphans.set(false);
        reclaiming.set(false);
        wanted.clear();
        dirty.clear();
        lastPublishedAtMillis.clear();
        fenced.clear();
        pendingSweeps.clear();
    }

    final Set<ShardId> pendingSweeps = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Notes shards published outside a pass, so the next pass sweeps what they left behind.
     *
     * @param published the shards an edge-triggered publish uploaded
     */
    public void noteForSweep(Collection<ShardId> published) {
        if (activity.isClosed()) {
            return;
        }
        pendingSweeps.addAll(published);
    }

    Set<ShardId> drainPendingSweeps() {
        final Set<ShardId> drained = new LinkedHashSet<>(pendingSweeps);
        pendingSweeps.removeAll(drained);
        return drained;
    }

    /** Publishes and releases every writer shard this node holds, for a clean shutdown. */
    public int handOverAll() {
        return eviction.handOverAll();
    }

    final java.util.concurrent.ConcurrentHashMap<ShardId, Object> publishLocks = new java.util.concurrent.ConcurrentHashMap<>();

    /** One lock per shard, so the backstop and the edge-triggered publish of one shard do not overlap. */
    Object publishLock(ShardId shardId) {
        return publishLocks.computeIfAbsent(shardId, key -> new Object());
    }

    /** Activates this reconciler's shards. */
    final ShardActivations activations = new ShardActivations(this);

    /** Takes over and settles abandoned shards. */
    final AbandonedShards janitor = new AbandonedShards(this);

    /** Publishes this reconciler's dirty writers. */
    final DirtyPublishing publishing = new DirtyPublishing(this);

    /** Lets go of this reconciler's shards. */
    final ShardEviction eviction = new ShardEviction(this);

    /** Sweeps this reconciler's published files and views. */
    final PublishedSweeps sweeps = new PublishedSweeps(this);

    /** Keeps this reconciler's leases and heads current. */
    final LeaseUpkeep leases = new LeaseUpkeep(this);

    /** Runs this reconciler's passes. */
    final ReconcilePass pass = new ReconcilePass(this);

}
