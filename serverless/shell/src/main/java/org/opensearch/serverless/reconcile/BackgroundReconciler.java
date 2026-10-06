/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.DigestRollups;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardReconciler;
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

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(BackgroundReconciler.class);

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final RoutingHints hints = new RoutingHints();
    private final Set<Map.Entry<String, Integer>> wanted = ConcurrentHashMap.newKeySet();
    private final Map<ShardId, Long> lastPublishedMaxSeqNo = new ConcurrentHashMap<>();
    private final Set<ShardId> dirty = ConcurrentHashMap.newKeySet();
    private final Set<ShardId> fenced = ConcurrentHashMap.newKeySet();
    private final Set<String> pinnedIndices = ConcurrentHashMap.newKeySet();
    private volatile boolean demandDriven;
    /** Set by a running {@link ReconcileScheduler}: the lease is renewed by its timer, so the backstop must not renew it again. */
    private volatile boolean renewalDrivenByTimer;
    /**
     * The activations in flight, one per shard. The renewal timer, the backstop, a failure-triggered pass and
     * a write waiting for its shard can all ask for the same shard at once; the first starts it and the rest
     * wait on the same future, so one shard is never two acquisitions, two fences and two opens racing.
     *
     * <p>It replaced one monitor over both passes, which was the same guarantee bought by serialising every
     * activation on the node: a node taking a hundred cold shards took them one after another, and a write
     * waiting for one shard waited for all the shards ahead of it.
     */
    private final Map<Map.Entry<String, Integer>, java.util.concurrent.CompletableFuture<Optional<ShardId>>> activating =
        new ConcurrentHashMap<>();
    /** Activations run at most this many at a time, on the generic pool; the rest queue. */
    private volatile int activationConcurrency = DEFAULT_ACTIVATION_CONCURRENCY;
    /**
     * Activations waiting for a slot: those this node is the preferred taker of first, see {@link #takeoverRank}.
     *
     * <p>First come, first served used to be the order, and when a node died every survivor received writes for every
     * shard it had held, queued the same few hundred activations in the same order, and ran them at the same moment: one
     * of each five won its head and four spent eight seconds on nothing. Taking a dead node's 300 shards ran at one
     * node's pace whatever the cap. Ranked, each survivor does its own share first, and finds the rest taken by the time it
     * reaches them -- which costs a head read, not an activation.
     */
    private final java.util.concurrent.PriorityBlockingQueue<QueuedActivation> activationQueue =
        new java.util.concurrent.PriorityBlockingQueue<>();

    private final java.util.concurrent.atomic.AtomicLong activationSequence = new java.util.concurrent.atomic.AtomicLong();

    /** One activation waiting for a slot, ordered by this node's rank for its shard and then by arrival. */
    private record QueuedActivation(int rank, long sequence, Runnable task) implements Comparable<QueuedActivation> {
        @Override
        public int compareTo(QueuedActivation other) {
            final int byRank = Integer.compare(rank, other.rank);
            return byRank != 0 ? byRank : Long.compare(sequence, other.sequence);
        }
    }

    /**
     * Where a node stands among the live members as the taker of a shard: 0 for the one member every node agrees should
     * try first, by rendezvous hashing, and higher for the rest. Every member computes the same ranking from the same
     * membership, with no coordination; a stale view only reorders work, it never stops anyone taking a shard.
     *
     * @param self the ranking node
     * @param members the live members, including itself
     * @param indexName the shard's index
     * @param shard the shard number
     * @return how many live members rank above this node for the shard
     */
    public static int takeoverRank(String self, java.util.Collection<String> members, String indexName, int shard) {
        final long mine = rendezvous(self, indexName, shard);
        int above = 0;
        for (String member : members) {
            if (member.equals(self) == false) {
                final long theirs = rendezvous(member, indexName, shard);
                if (theirs > mine || (theirs == mine && member.compareTo(self) < 0)) {
                    above++;
                }
            }
        }
        return above;
    }

    private static long rendezvous(String member, String indexName, int shard) {
        long h = 0xcbf29ce484222325L;
        for (char c : (member + "/" + indexName + "/" + shard).toCharArray()) {
            h ^= c;
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        return h;
    }

    private int rankHere(String indexName, int shard) {
        try {
            final java.util.List<String> members = new java.util.ArrayList<>();
            for (org.opensearch.serverless.membership.NodeLease lease : plane.membership().current()) {
                members.add(lease.nodeId());
            }
            final String self = node.localNode().getId();
            if (members.contains(self) == false) {
                members.add(self);
            }
            return takeoverRank(self, members, indexName, shard);
        } catch (RuntimeException e) {
            // Ordering is an optimisation: without a membership view, arrival order.
            return 0;
        }
    }

    private final java.util.concurrent.atomic.AtomicInteger activationsRunning = new java.util.concurrent.atomic.AtomicInteger();
    /** Guards the shard cap across concurrent on-demand activations, with the slots they have reserved. */
    private final Object capLock = new Object();
    private int capReserved;
    private volatile int maxShardsHeld = DEFAULT_MAX_SHARDS_HELD;
    private volatile long evictAfterMillis = DEFAULT_EVICT_AFTER_MILLIS;
    private volatile double evictHeadroomFraction = DEFAULT_EVICT_HEADROOM_FRACTION;

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
        final Set<ShardId> activated = new java.util.LinkedHashSet<>(activateWanted());
        final Set<ShardId> published = publishAll();
        // After publishing, so a shard released for idleness has just had its chance to flush.
        released.addAll(releaseIdle(nowMillis));
        handOffToMemberWithRoom();
        // And after that, so a reader this node has just let go of for idleness is not looked up again.
        released.addAll(refreshReaders());
        // Views this node is holding for a record that has gone: in-memory to decide, so it costs nothing
        // on a node holding none, and it must not wait for the reaping cadence below.
        closeReleasedViews();
        // The deployment-wide reap is a listing, and every node would otherwise pay one every pass to be
        // told that a feature nobody used is still not being used. A keep-alive is minutes; this is
        // seconds times a small number, which is soon enough and is a fraction of the cost.
        if (passes++ % REAP_EVERY_PASSES == 0) {
            if (reapExpiredViews() > 0) {
                // Files a reaped view was holding are deletable now, and nothing else would notice until
                // the shard next lost a file of its own.
                forceSweepOfOpenWriters();
            }
            reaps++;
            // And the pins this node did not touch: two listings, no reads, every few reaps. A view reaped
            // elsewhere or a snapshot deleted anywhere changes the set, and a changed set is the one signal
            // that files a quiet index held under that pin are collectable now rather than at its next
            // merge.
            if (reaps % PINS_EVERY_REAPS == 0) {
                try {
                    final Set<String> pins = collector.pins();
                    if (lastPins != null && pins.equals(lastPins) == false) {
                        forceSweepOfOpenWriters();
                    }
                    lastPins = pins;
                } catch (Exception e) {
                    logger.warn("could not read the deployment's pins; the sweep gate keeps its last reading", e);
                }
            }
            // Descriptor tombstones past their quarantine, rarely: a name created and deleted daily used
            // to poison its prefix pattern after a year, because nothing ever removed them. Only the
            // buckets this node owns, so the deployment pays once per deleted name, not once per node.
            final org.opensearch.cluster.node.DiscoveryNode self = node.localNode();
            if (reaps % TOMBSTONE_SWEEP_EVERY_REAPS == 0 && self != null) {
                sweepTombstonesOffThePass(self.getId());
            }
            // And one slice of the orphaned-shard sweep, as rarely: a backstop for shard data whose index
            // is gone and whose reclaim never ran. The cursor is shared, so every node's slices advance one
            // cycle, and a slice costs its budget whatever the deployment's size.
            if (reaps % ORPHAN_SLICE_EVERY_REAPS == ORPHAN_SLICE_EVERY_REAPS / 2 && orphanSliceBudget > 0) {
                sliceOrphansOffThePass();
            }
            // And the deletes whose reclaim intents have fallen due, every reap: an intent is due minutes
            // after its delete, not hours, and the queue lists only the buckets that are.
            if (self != null) {
                reclaimOffThePass(self.getId());
            }
        }
        // One small register read: the stored scripts are re-read only when their marker has moved, which
        // is how a script put on another node reaches this one without a listing per pass.
        node.storedScripts().refresh(false);
        // The full re-read of this node's truth, rarely. Activation no longer goes through it -- taking
        // one shard used to re-read every held head -- so this is what reopens a shard whose head still
        // names this node but which is not open here: one closed after a failed log append, or one whose
        // open failed after the head was won. One listing and a head read per claim, every
        // RESYNC_EVERY_PASSES backstops, which at the default interval is once in ten minutes.
        if (passes % RESYNC_EVERY_PASSES == RESYNC_EVERY_PASSES - 1) {
            try {
                activated.addAll(node.syncFrom(plane));
            } catch (Exception e) {
                logger.warn("periodic resync against the metadata plane failed; the next one will retry", e);
            }
        }
        // Only what was published, so the sweep costs nothing at all on a shard nobody is writing to.
        // Edge-triggered publishes are swept here too: only the backstop's result used to enter the
        // watch set, so a burst-then-idle shard kept its merged-away segments forever.
        published.addAll(drainPendingSweeps());
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
            if (node.reconciler().inFlight(shardId) > 0) {
                // A query is running against it. Closing the reader under a running aggregation failed
                // the query with a closed reader, which looks like corruption and is a scheduling choice;
                // the next pass finds the commit still superseded and tries again.
                continue;
            }
            try {
                final var current = plane.segmentPublisher(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id())
                    .readManifest();
                if (current.isEmpty()) {
                    // The commit this reader is serving is no longer published at all. A deleted index,
                    // most likely -- but a manifest read that came back empty is not proof of that on its
                    // own, so the descriptor decides: gone too, and the reader is let go with the records
                    // this node kept of it; still there, and the next search will say what is true.
                    if (plane.describe(shardId.getIndexName()).isEmpty()
                        && node.reconciler()
                            .releaseReader(shardId, opened.get(), "its index has been deleted") == ShardReconciler.ReaderRelease.RELEASED) {
                        node.forgetReader(shardId);
                        stale.add(shardId);
                    }
                    continue;
                }
                if (sameCommit(opened.get(), current.get())) {
                    continue;
                }
                // Only if it is still the reader judged above. The manifest read is a round trip, and a write during
                // it can make this node the shard's writer under the same id: closing that would leave its head
                // naming a node with nothing open, answering "retry" for as long as the node lives.
                if (node.reconciler()
                    .releaseReader(
                        shardId,
                        opened.get(),
                        "the commit it was serving has been superseded"
                    ) != ShardReconciler.ReaderRelease.RELEASED) {
                    continue;
                }
                // And the node's own record of serving it, or a search node that cycles through readers
                // carries every one it ever opened into every reader open for the rest of its life.
                node.forgetReader(shardId);
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

    /** The manifest each shard last published from this node, for the sweep's gate. */
    private final Map<ShardId, org.opensearch.serverless.store.CommitManifest> lastManifests = new ConcurrentHashMap<>();

    /** Per shard, the files of the manifest the last sweep ran against. */
    private final Map<ShardId, Set<String>> sweptAgainst = new ConcurrentHashMap<>();

    /** Shards whose next sweep must run whatever their manifest says. */
    private final Set<ShardId> forceSweep = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private void forceSweepOfOpenWriters() {
        for (ShardId shardId : node.reconciler().openShards()) {
            if (node.reconciler().readerShards().contains(shardId) == false) {
                forceSweep.add(shardId);
                pendingSweeps.add(shardId);
            }
        }
    }

    /**
     * Decides whether a sweep of a shard can find anything a previous sweep did not.
     *
     * <p>A file becomes unreferenced when a manifest stops naming it -- a merge or a delete -- and at no
     * other moment this node can see. So a shard is swept when its newest manifest lacks a file the
     * last sweep's manifest had, when it has candidates on watch for the grace period, when a reaped
     * view may have freed something, or when this node has never swept it since opening it. A publish
     * that only added segments, which is most of them, costs no listing.
     */
    private boolean sweepCanFindSomething(ShardId shardId) {
        if (forceSweep.remove(shardId) || unreferencedFor.containsKey(shardId)) {
            return true;
        }
        final Set<String> previous = sweptAgainst.get(shardId);
        final org.opensearch.serverless.store.CommitManifest current = lastManifests.get(shardId);
        if (previous == null || current == null) {
            return true;
        }
        for (String file : previous) {
            // Every commit replaces its segments_N file, so that one loss says nothing about a merge;
            // the tiny orphan it leaves is collected with the next real one.
            if (file.startsWith("segments_") == false && current.files().containsKey(file) == false) {
                return true;
            }
        }
        return false;
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
        final Set<ShardId> toSweep = new LinkedHashSet<>();
        for (ShardId shardId : published) {
            if (sweepCanFindSomething(shardId)) {
                toSweep.add(shardId);
            }
        }
        toSweep.addAll(unreferencedFor.keySet());
        if (toSweep.isEmpty()) {
            // Nothing published that lost a file, and nothing on watch: this pass costs no listing at all.
            return deleted;
        }
        // The views and snapshots every shard's sweep checks against, read once per pass rather than once
        // per shard: at a hundred published shards and a thousand views that was a hundred thousand reads
        // a pass.
        final java.util.List<org.opensearch.serverless.metadata.PointInTime> views;
        final java.util.List<org.opensearch.serverless.metadata.SnapshotRecord> snapshots;
        try {
            views = plane.livePointsInTime(plane.clock().getAsLong());
            snapshots = plane.liveSnapshots();
        } catch (Exception e) {
            logger.warn("could not read the views and snapshots a sweep must respect; sweeping nothing this pass", e);
            return deleted;
        }
        for (ShardId shardId : toSweep) {
            if (node.reconciler().openShards().contains(shardId) == false || node.reconciler().readerShards().contains(shardId)) {
                // Not ours to sweep any more. Forgetting what we had seen is the safe direction: the next
                // owner starts again from nothing and simply deletes later than it could have.
                unreferencedFor.remove(shardId);
                sweptAgainst.remove(shardId);
                lastManifests.remove(shardId);
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
                final var swept = collector.sweepShard(plane, shardId.getIndexName(), shardId.id(), eligible, views, snapshots);
                final var manifest = lastManifests.get(shardId);
                if (manifest != null) {
                    sweptAgainst.put(shardId, Set.copyOf(manifest.files().keySet()));
                }
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

    /** How many reaps between sweeps of descriptor tombstones past their quarantine: about hourly at the defaults. */
    public static final int TOMBSTONE_SWEEP_EVERY_REAPS = 12;

    private final java.util.concurrent.atomic.AtomicBoolean sweepingTombstones = new java.util.concurrent.atomic.AtomicBoolean();
    private volatile java.util.concurrent.Future<?> tombstoneSweep = java.util.concurrent.CompletableFuture.completedFuture(null);

    /**
     * Starts a tombstone sweep on the generic pool, unless one is still running.
     *
     * <p>Off the pass because its length is set by how much was deleted, and the pass is on a fixed delay:
     * run inline, a node draining a backlog of deletions held up its own idle release, backstop publish,
     * view reaping and resync for as long as the drain took. Single-flight, so a drain that outlasts the
     * interval is not joined by a second one over the same buckets.
     */
    private void sweepTombstonesOffThePass(String selfId) {
        if (sweepingTombstones.compareAndSet(false, true) == false) {
            return;
        }
        try {
            tombstoneSweep = node.threadPool().generic().submit(() -> {
                try {
                    collector.collectTombstones(plane, plane.clock().getAsLong(), selfId);
                } catch (Exception e) {
                    logger.warn("could not sweep descriptor tombstones; the next sweep will retry", e);
                } finally {
                    sweepingTombstones.set(false);
                }
            });
        } catch (RuntimeException e) {
            sweepingTombstones.set(false);
            logger.warn("could not start a tombstone sweep; the next pass that is due will try again", e);
        }
    }

    /** How many reaps between slices of the orphaned-shard sweep: about hourly at the defaults. */
    public static final int ORPHAN_SLICE_EVERY_REAPS = 12;

    /**
     * How many shard containers one orphan slice visits by default. A slice costs about this many descriptor
     * reads at worst, once an hour per node, which keeps the background's per-node cost flat.
     */
    public static final int DEFAULT_ORPHAN_SLICE_BUDGET = 100;

    private volatile int orphanSliceBudget = DEFAULT_ORPHAN_SLICE_BUDGET;
    private final java.util.concurrent.atomic.AtomicBoolean slicingOrphans = new java.util.concurrent.atomic.AtomicBoolean();
    private volatile java.util.concurrent.Future<?> orphanSlice = java.util.concurrent.CompletableFuture.completedFuture(null);

    /**
     * Sets how many shard containers a background orphan slice visits; zero turns the background sweep off.
     *
     * @param budget containers per slice
     */
    public void setOrphanSliceBudget(int budget) {
        this.orphanSliceBudget = Math.max(0, budget);
    }

    private void sliceOrphansOffThePass() {
        if (slicingOrphans.compareAndSet(false, true) == false) {
            return;
        }
        try {
            orphanSlice = node.threadPool().generic().submit(() -> {
                try {
                    final GarbageCollector.OrphanSlice slice = collector.collectOrphanedShards(plane, orphanSliceBudget);
                    if (slice.deleted().isEmpty() == false) {
                        logger.info("the orphan sweep collected {} shard containers", slice.deleted().size());
                    }
                } catch (Exception e) {
                    logger.warn("could not sweep a slice of orphaned shards; the cursor stays and the next slice retries", e);
                } finally {
                    slicingOrphans.set(false);
                }
            });
        } catch (RuntimeException e) {
            slicingOrphans.set(false);
            logger.warn("could not start an orphan slice; the next one that is due will try again", e);
        }
    }

    /**
     * Returns the most recently started orphan slice, for a caller that needs to wait on it.
     *
     * @return the slice's future, complete if none is running
     */
    public java.util.concurrent.Future<?> orphanSlice() {
        return orphanSlice;
    }

    private final java.util.concurrent.atomic.AtomicBoolean reclaiming = new java.util.concurrent.atomic.AtomicBoolean();
    private volatile java.util.concurrent.Future<?> reclaimSweep = java.util.concurrent.CompletableFuture.completedFuture(null);

    /** Drains the due reclaim intents this node owns on the generic pool, unless a drain is still running. */
    private void reclaimOffThePass(String selfId) {
        if (reclaiming.compareAndSet(false, true) == false) {
            return;
        }
        try {
            reclaimSweep = node.threadPool().generic().submit(() -> {
                try {
                    collector.collectReclaims(plane, plane.clock().getAsLong(), selfId);
                } catch (Exception e) {
                    logger.warn("could not drain the reclaim queue; the next reap will retry", e);
                } finally {
                    reclaiming.set(false);
                }
            });
        } catch (RuntimeException e) {
            reclaiming.set(false);
            logger.warn("could not start draining the reclaim queue; the next reap will try again", e);
        }
    }

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
    private final GarbageCollector collector;

    /**
     * The pins -- live view records and snapshot records -- as of the last reap, so a pin that came or
     * went anywhere in the deployment is noticed here. The per-node sweep gate can see only what this node
     * did: a manifest it published losing a file, a view it reaped. A view reaped on another node or a
     * snapshot deleted anywhere freed files a quiet index would otherwise hold until its next merge.
     */
    private Set<String> lastPins;

    private long reaps;

    /** How many passes between full re-reads of this node's truth from the metadata plane. */
    public static final int RESYNC_EVERY_PASSES = 20;

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
        // One record read per view, not per frozen shard: a hundred-shard view was a hundred identical
        // reads every pass on every node holding it.
        final Set<String> viewIds = new LinkedHashSet<>();
        for (ShardId view : node.reconciler().frozenShards()) {
            viewIds.add(view.getIndex().getUUID());
        }
        for (String viewId : viewIds) {
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
     * <p>A pinned index ({@link #pin}) is exempt here too, not only from cap eviction: "this stays
     * resident" would be a thin promise if it only held during a busy hour and let go the moment traffic
     * quieted down.
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
            if (pinnedIndices.contains(shardId.getIndexName())) {
                continue;
            }
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
        // The floor eviction clears to, not merely the one slot the caller needs -- see
        // DEFAULT_EVICT_HEADROOM_FRACTION for why clearing further than the immediate request pays for
        // itself under sustained demand. Never above the cap itself, so a headroom fraction close to or
        // above 1.0 cannot make this evict shards nobody needed room for.
        evictDownTo(Math.min(maxShardsHeld, Math.max(0, maxShardsHeld - headroomShards())), Integer.MAX_VALUE, false);
        return node.reconciler().heldShards().size() < maxShardsHeld;
    }

    /**
     * Gives work to a member with room: a node nearly at its cap, when another live writer has room, lets go of shards
     * past their eviction grace -- least recently used first, down to {@link #HAND_OFF_DOWN_TO} of its cap and at most a
     * tenth of it a pass -- so the next write for one is taken where there is room.
     *
     * <p>Only when someone has room. Shedding on a timer whatever the fleet looked like was measured and dropped: every
     * node kept giving shards up and taking them back, and misrouted writes doubled. A node that joins, or one restarted
     * empty, is what this is for -- without it the full ones stayed full until their shards went idle five minutes later,
     * and the new node took only what happened to arrive at it.
     *
     * @return how many shards were let go
     */
    public int handOffToMemberWithRoom() {
        if (demandDriven == false || evictAfterMillis <= 0 || node.nearlyFull() == false || node.memberWithRoom(plane).isEmpty()) {
            return 0;
        }
        final int floor = (int) Math.floor(maxShardsHeld * HAND_OFF_DOWN_TO);
        return evictDownTo(floor, Math.max(1, maxShardsHeld / 10), true);
    }

    /** The share of its cap a node hands off down to when a member has room. */
    public static final double HAND_OFF_DOWN_TO = 0.8;

    private int evictDownTo(int floor, int most, boolean handOff) {
        int evicted = 0;
        while (node.reconciler().heldShards().size() > floor && evicted < most) {
            final long now = plane.clock().getAsLong();
            final ShardId victim = pickVictim(now);
            if (victim == null) {
                break;
            }
            final long victimLastUsed = node.reconciler().lastUsed(victim).orElse(openedAt.getOrDefault(victim, now));
            final String why = handOff ? "handed off to a member with room" : "evicted to make room";
            if (letGo(victim, why + ", unused for " + (now - victimLastUsed) + "ms") == false) {
                // The head could not be released; letGo already logged why. Stopping rather than trying a
                // different victim next: a head release failing once tends to keep failing, and spinning
                // through every open shard to find one that happens to release is work for no room gained.
                break;
            }
            openedAt.remove(victim);
            if (demandDriven) {
                wanted.remove(Map.entry(victim.getIndexName(), victim.id()));
            }
            node.noteEvicted(handOff);
            evicted++;
        }
        if (evicted > 0) {
            logger.info(
                "{} {} shard(s), {} held against a cap of {} (floor {})",
                handOff ? "handed off" : "evicted to make room",
                evicted,
                node.reconciler().heldShards().size(),
                maxShardsHeld,
                floor
            );
        }
        return evicted;
    }

    /** The number of shards headroom clears beyond the one slot immediately needed, at least one. */
    private int headroomShards() {
        return Math.max(1, (int) Math.round(maxShardsHeld * evictHeadroomFraction));
    }

    /**
     * Picks the least recently used evictable shard, or null if none qualifies.
     *
     * <p>A pinned index's shards are never candidates, at any age -- pinning means "this stays resident,"
     * not "this stays resident a little longer than the rest."
     *
     * @param now the observer's clock
     * @return the shard to evict next, or null
     */
    private ShardId pickVictim(long now) {
        ShardId victim = null;
        long victimLastUsed = Long.MAX_VALUE;
        for (ShardId shardId : node.reconciler().openShards()) {
            if (pinnedIndices.contains(shardId.getIndexName())) {
                continue;
            }
            final long lastUsed = node.reconciler().lastUsed(shardId).orElse(openedAt.getOrDefault(shardId, now));
            if (now - lastUsed < evictAfterMillis) {
                continue;
            }
            if (lastUsed < victimLastUsed) {
                victim = shardId;
                victimLastUsed = lastUsed;
            }
        }
        return victim;
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
            // A reader holds no head and no claim on anything, so closing it loses nothing -- decided and done in one
            // step, because a reader that has become this node's writer since the caller chose it holds a head and
            // must go the writer's way below.
            switch (node.reconciler().releaseReader(shardId, null, reason)) {
                case RELEASED -> {
                    node.forgetReader(shardId);
                    return true;
                }
                case BUSY -> {
                    return false;
                }
                case NOT_A_READER -> {
                    // A writer: below.
                }
            }
            // Under the shard's fence, from before the publish until after the close. A write that has
            // applied to the engine and is about to append to the log holds the other side of that fence;
            // without it the sequence below interleaved with one: the publish completed, a request thread
            // applied a document, this thread released the head, a successor acquired and sealed the log,
            // and the request thread then appended behind the seal and acknowledged. Nobody replays a
            // record behind a seal. The fence makes the release wait for writes in flight and makes writes
            // that arrive during it wait for the close -- after which they find no shard and are refused,
            // which is the truthful answer.
            return node.underShardFence(shardId, () -> letGoFenced(shardId, reason));
        } catch (Exception e) {
            // Releasing is an optimisation. Failing to do it costs money and nothing else, so it is
            // logged and retried on the next pass rather than propagated into the tick.
            logger.warn("could not release shard " + shardId, e);
            return false;
        }
    }

    /** The body of {@link #letGo} for a writer, run while the shard's fence is held. */
    private boolean letGoFenced(ShardId shardId, String reason) throws Exception {
        final var head = plane.heads().read(shardId.getIndexName(), shardId.id());
        if (head.isEmpty() || node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
            // Not ours any more; renewLeases will deal with it and this must not race it.
            return false;
        }
        final var shard = node.reconciler().shard(shardId);
        final long maxSeqNo = shard == null ? -1L : shard.seqNoStats().getMaxSeqNo();
        // Publish only when there is something the last publish did not cover. An unconditional publish
        // here forced a fresh commit, uploaded a new segments_N and swapped the manifest on a shard
        // nobody had written to since it was last published -- seven requests and an orphan blob per
        // idle release, times every shard a node under cap pressure cycles through. The head release is
        // still required either way.
        //
        // And never for a shard whose log could not be appended to: its engine holds an operation the
        // log does not, and publishing would make the refusal the caller was given a lie. A successor
        // rebuilds it from the log, which is the state the caller was told about.
        // Unchanged only if the last publish's commit covered every operation this shard has: see #publish.
        final boolean unchanged = maxSeqNo < 0 || maxSeqNo <= lastPublishedMaxSeqNo.getOrDefault(shardId, -1L);
        if (unchanged == false && node.isWriteFenced(shardId) == false) {
            synchronized (publishLock(shardId)) {
                node.publishShard(shardId, head.get().term());
            }
        }
        if (plane.heads().release(shardId.getIndexName(), shardId.id(), node.localNode().getId()) == false) {
            // Somebody else's head now, or the swap lost. Keep the shard and try again next pass;
            // closing it while the head still points here is the one outcome to avoid.
            logger.warn("could not release the head for {}; keeping it open", shardId);
            return false;
        }
        node.reconciler().releaseShard(shardId, reason);
        lastPublishedMaxSeqNo.remove(shardId);
        // Unmarked only now, with the last publish recorded and no writer left: from here a coordinator may
        // rule the shard out from its digest. A clear that fails leaves it marked, which costs a check.
        try {
            plane.rollups().clearOwned(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id(), head.get().term());
        } catch (Exception e) {
            logger.warn("could not clear the writer mark of " + shardId + " in its digest rollup; searches will not skip it", e);
        }
        // And forget the claim, so a node that churns through shards does not read a head per stale
        // claim on every heartbeat for the rest of its life.
        plane.forgetAssignment(node.localNode().getId(), shardId.getIndexName(), shardId.id());
        if (maxSeqNo < 0) {
            // Opened and given back with nothing ever written: its entry would keep every wide search from ruling
            // it out. See MetadataPlane#forgetIfNothing.
            try {
                plane.forgetIfNothing(shardId.getIndexName());
            } catch (Exception e) {
                logger.debug("could not take empty " + shardId + " out of its rollup", e);
            }
        }
        return true;
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
        if (renewalDrivenByTimer) {
            // The timer renews the lease; the backstop's job is the half that verifies. Renewing here too
            // was a fourth lease write per TTL that guaranteed nothing the timer's three did not.
            //
            // IfDue, not unconditionally: the scan is one register read per held shard, and running it on
            // every renewal and every backstop alike made it the largest standing cost of an idle node.
            // See ServerlessNode#DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS for why a bounded wait is safe here
            // and where the unconditional scan still happens.
            return node.verifyHeadsIfDue(plane);
        }
        return node.heartbeat(plane);
    }

    /**
     * Renews this node's lease, and only that: the renewal timer's body.
     *
     * <p>Separated from {@link #verifyHeads()} so the lease can be renewed on its own clock, ahead of and
     * independent from the pass that reads one head per held shard. If the lease had already lapsed by
     * this node's own clock, every writer shard is released <em>before</em> the renewal -- see
     * {@link ServerlessNode#renewLease}.
     *
     * @return the shards released because the lease had lapsed
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> renewLease() throws Exception {
        return node.renewLease(plane);
    }

    /**
     * Reads the head of every held writer shard and releases what this node has lost.
     *
     * @return the shards released
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> verifyHeads() throws Exception {
        return node.verifyHeadsIfDue(plane);
    }

    /**
     * Reads the head of every held writer shard now, whatever the interval says.
     *
     * @return the shards released
     * @throws Exception if the metadata plane cannot be reached
     */
    public Set<ShardId> verifyHeadsNow() throws Exception {
        return node.verifyHeads(plane);
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
        final java.util.List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending = new java.util.ArrayList<>();
        for (Map.Entry<String, Integer> target : wanted) {
            if (heldAsWriter(target.getKey(), target.getValue()) == false) {
                pending.add(activate(target.getKey(), target.getValue(), false));
            }
        }
        return settle(pending);
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
        final java.util.List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending = new java.util.ArrayList<>();
        for (Map.Entry<String, Integer> candidate : candidates) {
            if (heldAsWriter(candidate.getKey(), candidate.getValue()) == false) {
                pending.add(activate(candidate.getKey(), candidate.getValue(), true));
            }
        }
        return settle(pending);
    }

    /** Whether the shard is open here as a writer: a pass reports only what it newly took. */
    private boolean heldAsWriter(String indexName, int shard) {
        return node.reconciler()
            .openShards()
            .stream()
            .anyMatch(s -> s.getIndexName().equals(indexName) && s.id() == shard && node.reconciler().readerShards().contains(s) == false);
    }

    /**
     * Takes one shard nobody owns because a request for it is waiting, and returns when it is open here --
     * or when it is clear it will not be.
     *
     * <p>What a write to a dormant shard waits on, instead of being told 421 and retrying until some pass has
     * taken the shard: the pass would start on the doubt the refusal raised, and the client's retries, each a
     * head read, were most of the cost of a first write. Shares the single flight every other path uses.
     *
     * @param indexName the index
     * @param shard the shard
     * @return the shard once open here, or empty if this node did not take it: it neither wants the shard nor
     *     runs demand-driven, the node is full, or another node won it
     */
    public java.util.concurrent.CompletableFuture<Optional<ShardId>> activateForRequest(String indexName, int shard) {
        // A shard this node was told to want is taken whatever the policy, and without the cap -- exactly as
        // the pass would take it; any other shard only under demand-driven activation.
        if (wanted.contains(Map.entry(indexName, shard))) {
            return activate(indexName, shard, false);
        }
        if (demandDriven == false) {
            return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
        }
        return activate(indexName, shard, true);
    }

    /** Waits for every activation of a pass and collects the shards it took; the first failure is rethrown. */
    private static Set<ShardId> settle(java.util.List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending) throws Exception {
        final Set<ShardId> taken = new LinkedHashSet<>();
        Exception first = null;
        for (java.util.concurrent.CompletableFuture<Optional<ShardId>> future : pending) {
            try {
                future.get().ifPresent(taken::add);
            } catch (java.util.concurrent.ExecutionException e) {
                final Exception cause = e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
                if (first == null) {
                    first = cause;
                } else {
                    first.addSuppressed(cause);
                }
            }
        }
        if (first != null && taken.isEmpty()) {
            throw first;
        }
        if (first != null) {
            logger.warn("some activations of this pass failed; the next pass retries them", first);
        }
        return taken;
    }

    /**
     * Starts one shard's activation unless it is already in flight, in which case the caller shares it.
     * Taken into the map before it is queued, and out of it only by the task that ran it, so a finished
     * activation is never handed to a later caller.
     */
    private java.util.concurrent.CompletableFuture<Optional<ShardId>> activate(String indexName, int shard, boolean onDemand) {
        return activate(indexName, shard, onDemand, null);
    }

    /** As {@link #activate(String, int, boolean)}, counting the store requests of the activation into a sink if one is given. */
    private java.util.concurrent.CompletableFuture<Optional<ShardId>> activate(
        String indexName,
        int shard,
        boolean onDemand,
        java.util.concurrent.atomic.AtomicLong sink
    ) {
        final Map.Entry<String, Integer> key = Map.entry(indexName, shard);
        final java.util.concurrent.CompletableFuture<Optional<ShardId>> mine = new java.util.concurrent.CompletableFuture<>();
        final java.util.concurrent.CompletableFuture<Optional<ShardId>> running = activating.putIfAbsent(key, mine);
        if (running != null) {
            return running;
        }
        final long queuedAt = System.nanoTime();
        activationQueue.add(new QueuedActivation(rankHere(indexName, shard), activationSequence.incrementAndGet(), () -> {
            final long startedAt = System.nanoTime();
            activationWaitMax.accumulateAndGet(startedAt - queuedAt, Math::max);
            try {
                final Optional<ShardId> taken = sink == null
                    ? activateNow(indexName, shard, onDemand)
                    : org.opensearch.serverless.store.ObjectStores.attributedTo(sink, () -> activateNow(indexName, shard, onDemand));
                if (taken.isPresent()) {
                    // Only an activation that opened a shard says how fast the store is taking them: a refusal or a
                    // shard already held elsewhere returns in a read and would make it look faster than it is.
                    adaptActivationLimit(System.nanoTime() - startedAt);
                }
                mine.complete(taken);
            } catch (Throwable t) {
                mine.completeExceptionally(t);
            } finally {
                activating.remove(key, mine);
                final long endedAt = System.nanoTime();
                activationsDone.incrementAndGet();
                activationWaitNanos.addAndGet(startedAt - queuedAt);
                activationRunNanos.addAndGet(endedAt - startedAt);
                activationRunMax.accumulateAndGet(endedAt - startedAt, Math::max);
            }
        }));
        drainActivations();
        return mine;
    }

    private final java.util.concurrent.atomic.AtomicLong activationsDone = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong activationWaitNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong activationRunNanos = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong activationWaitMax = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong activationRunMax = new java.util.concurrent.atomic.AtomicLong();

    /**
     * What the activation queue is doing: how many wait, how many run, and how long they have waited and run.
     *
     * @param queued activations waiting for a slot
     * @param running activations running
     * @param done activations finished since the node started
     * @param waitMillis their total time in the queue
     * @param runMillis their total time running
     * @param maxWaitMillis the longest wait since the last time these were read
     * @param maxRunMillis the longest run since the last time these were read
     * @param limit how many may run at once now
     */
    public record ActivationStats(int queued, int running, long done, long waitMillis, long runMillis, long maxWaitMillis,
        long maxRunMillis, int limit) {
    }

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

    /** Starts queued activations while fewer than the bound are running; each one that ends starts the next. */
    private void drainActivations() {
        while (true) {
            final int running = activationsRunning.get();
            if (running >= currentActivationLimit() || activationQueue.isEmpty()) {
                return;
            }
            if (activationsRunning.compareAndSet(running, running + 1) == false) {
                continue;
            }
            final QueuedActivation queued = activationQueue.poll();
            final Runnable task = queued == null ? null : queued.task();
            if (task == null) {
                activationsRunning.decrementAndGet();
                continue;
            }
            try {
                // Never GENERIC: what waits for an activation waits there. See ServerlessNode#ACTIVATION_POOL.
                node.threadPool().executor(org.opensearch.serverless.shell.ServerlessNode.ACTIVATION_POOL).execute(() -> {
                    try {
                        task.run();
                    } finally {
                        activationsRunning.decrementAndGet();
                        drainActivations();
                    }
                });
            } catch (RuntimeException e) {
                // A pool that will not take work -- a node shutting down -- runs it here instead of losing it.
                try {
                    task.run();
                } finally {
                    activationsRunning.decrementAndGet();
                }
            }
        }
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
    /** Whether another node holds a shard's head under a live lease: one register read, against an eviction. */
    private boolean heldByAnotherLiveNode(String indexName, int shard) {
        try {
            final var head = plane.heads().read(indexName, shard);
            return head.isPresent()
                && head.get().ownerNodeId() != null
                && node.localNode().getId().equals(head.get().ownerNodeId()) == false
                && plane.heads().isHeld(head.get(), plane.clock().getAsLong());
        } catch (Exception e) {
            // Unknown: go on as before, and let the acquisition decide.
            return false;
        }
    }

    private boolean headNamesThisNode(String indexName, int shard) {
        try {
            final var head = plane.heads().read(indexName, shard);
            return head.isPresent() && node.localNode().getId().equals(head.get().ownerNodeId());
        } catch (Exception e) {
            logger.warn("could not read the head of " + indexName + "[" + shard + "] at the cap; treating it as not ours", e);
            return false;
        }
    }

    /**
     * Takes over the shards a node held when its lease ran out, rather than waiting for a request to find each one.
     *
     * <p>Without this a dead node's shard was taken only when a write or a get for it arrived, failed to reach the
     * dead owner, and raised a doubt -- so takeover time was the lease plus however long the next request for the
     * shard took to come, and a dormant shard with acknowledged writes in its log waited for its first reader.
     *
     * <p><b>Split, not raced.</b> Every survivor hears the departure. Each takes at once the shards that hash to it
     * among the live nodes, and returns the rest for a second look after {@code later} -- by when the survivor they
     * hash to has taken them, unless it is full or slow, in which case anyone may. The head's compare-and-swap
     * decides any race; the split only keeps every survivor from activating every shard at the same moment.
     *
     * <p>Only shards whose head still names the dead node. A node that shut down cleanly released its heads, and
     * those shards stay dormant until something asks for them.
     *
     * @param deadNodeId the node whose lease ran out
     * @return the claims left for the second look
     */
    public List<Map.Entry<String, Integer>> takeOverFrom(String deadNodeId) {
        if (demandDriven == false || deadNodeId.equals(node.localNode().getId())) {
            return List.of();
        }
        // Gone from this node's view is not gone: a view loses a member whose lease read failed, or that it read
        // late. A fleet run had a node that had just started take shards from all five of its live peers.
        try {
            final java.util.OptionalLong liveUntil = leaseLiveUntil(deadNodeId);
            if (liveUntil.isPresent()) {
                logger.info(
                    "node {} left this node's view, but its lease runs {} ms more: its shards stay with it",
                    deadNodeId,
                    liveUntil.getAsLong() - plane.clock().getAsLong()
                );
                return List.of();
            }
        } catch (Exception e) {
            logger.warn("could not read the lease of departed node " + deadNodeId + "; its shards wait for requests", e);
            return List.of();
        }
        final List<Map.Entry<String, Integer>> claims;
        try {
            claims = plane.claimsOf(deadNodeId);
        } catch (Exception e) {
            logger.warn("could not list the claims of departed node " + deadNodeId + "; its shards wait for requests", e);
            return List.of();
        }
        final List<String> live = new java.util.ArrayList<>();
        for (var lease : plane.membership().current()) {
            if (lease.nodeId().equals(deadNodeId) == false && lease.isExpiredAt(plane.clock().getAsLong()) == false) {
                live.add(lease.nodeId());
            }
        }
        java.util.Collections.sort(live);
        final int rank = live.indexOf(node.localNode().getId());
        final List<Map.Entry<String, Integer>> later = new java.util.ArrayList<>();
        final long settledBefore = settledClean.get();
        int taking = 0;
        for (Map.Entry<String, Integer> claim : claims) {
            final int mine = live.isEmpty() || rank < 0
                ? 0
                : Math.floorMod((claim.getKey() + "#" + claim.getValue()).hashCode(), live.size());
            if (rank < 0 || mine == rank) {
                if (takeIfStillHeldBy(deadNodeId, claim)) {
                    taking++;
                }
            } else {
                later.add(claim);
            }
        }
        logger.info(
            "node {} left: taking {} of its {} claimed shards and giving back {} fully published, {} more if still unclaimed later",
            deadNodeId,
            taking,
            claims.size(),
            settledClean.get() - settledBefore,
            later.size()
        );
        return later;
    }

    /**
     * When a node's lease runs out, read from the store rather than from the membership view.
     *
     * @param nodeId the node
     * @return its lease's expiry if the lease exists and has not run out, empty if it is gone or expired
     * @throws java.io.IOException if the lease cannot be read
     */
    public java.util.OptionalLong leaseLiveUntil(String nodeId) throws java.io.IOException {
        final var lease = plane.membership().read(nodeId);
        if (lease.isEmpty() || lease.get().isExpiredAt(plane.clock().getAsLong())) {
            return java.util.OptionalLong.empty();
        }
        return java.util.OptionalLong.of(lease.get().expiresAtMillis());
    }

    /**
     * The second look at a departed node's shards: any still named to it are taken now.
     *
     * @param deadNodeId the node whose lease ran out
     * @param claims the claims {@link #takeOverFrom} left for later
     */
    public void takeOverRemaining(String deadNodeId, List<Map.Entry<String, Integer>> claims) {
        int taking = 0;
        for (Map.Entry<String, Integer> claim : claims) {
            if (takeIfStillHeldBy(deadNodeId, claim)) {
                taking++;
            }
        }
        if (taking > 0) {
            logger.info("node {} left: took {} more of its shards that no other survivor had", deadNodeId, taking);
        }
    }

    private boolean takeIfStillHeldBy(String deadNodeId, Map.Entry<String, Integer> claim) {
        try {
            final var head = plane.heads().read(claim.getKey(), claim.getValue());
            if (head.isEmpty() || deadNodeId.equals(head.get().ownerNodeId()) == false) {
                return false;
            }
            // Given back rather than taken when everything the dead node acknowledged is published: most of what a
            // node holds is idle, and taking all of it put a dead node's shards on survivors already near their cap.
            // Only a shard whose log is ahead of its commit needs a writer, to replay and publish it.
            final MetadataPlane.Settled settled = plane.settleAbandoned(claim.getKey(), claim.getValue(), deadNodeId);
            if (settled == MetadataPlane.Settled.RELEASED_CLEAN) {
                settledClean.incrementAndGet();
                return false;
            }
            if (settled == MetadataPlane.Settled.NOT_ABANDONED) {
                return false;
            }
        } catch (Exception e) {
            logger.debug("could not settle " + claim + " of departed node " + deadNodeId + "; it waits for a request", e);
            return false;
        }
        activate(claim.getKey(), claim.getValue(), true);
        return true;
    }

    /**
     * What one janitor pass did.
     *
     * @param examined shards looked at
     * @param released dead owners' shards given back with everything published, or released marks cleared
     * @param replays shards whose log was ahead of their commit, taken here to be replayed and published
     * @param claimsForgotten dead nodes' claims on shards whose head had moved on
     * @param bucket the rollup bucket read this pass, as group/bucket, or null if none was due here
     * @param full whether the pass used its whole budget, so more is waiting
     */
    public record JanitorPass(int examined, int released, int replays, int claimsForgotten, String bucket, boolean full) {
    }

    /** How many shards a janitor pass looks at, at most. */
    public static final int DEFAULT_JANITOR_BUDGET = 200;

    /**
     * How many shards a janitor pass takes to replay, at most: each is a real activation on this node's activation
     * threads, though it holds a cap slot only until it is published and given back.
     */
    public static final int DEFAULT_JANITOR_REPLAYS = 32;

    private final java.util.concurrent.atomic.AtomicLong janitorCursor = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Cleans up after crashed nodes, a bounded amount at a time.
     *
     * <p>A crash leaves three things naming the dead node: shard heads, claims, and rollup marks that keep its shards
     * from being ruled out of a wide search. A departure noticed live is cleaned up by {@link #takeOverFrom}; this is
     * for everything else -- a fleet that crashed as a whole, a departure nobody heard, a mark whose owner released
     * its head and could not clear it. Each pass lists the claim directories once and reads one rollup bucket,
     * rotating, and the work is split between live nodes by hash, so the steady cost is a few reads a pass.
     *
     * @param budget how many shards to look at, at most
     * @param replays how many shards to take for replay, at most
     * @return what the pass did
     * @throws IOException if the store cannot be listed
     */
    public JanitorPass sweepAbandoned(int budget, int replays) throws IOException {
        final long now = plane.clock().getAsLong();
        final List<String> live = new java.util.ArrayList<>();
        for (var lease : plane.membership().current()) {
            if (lease.isExpiredAt(now) == false) {
                live.add(lease.nodeId());
            }
        }
        java.util.Collections.sort(live);
        final int rank = live.indexOf(node.localNode().getId());
        if (rank < 0) {
            return new JanitorPass(0, 0, 0, 0, null, false);
        }
        final int[] counts = new int[4]; // examined, released, replays, forgotten
        final int[] replaysLeft = { replays };

        // Claims of nodes dead for longer than a live departure takes to be handled -- at most half the budget, or a
        // crashed fleet's thousands of stale claims starve the marks, which are what make a wide search slow.
        final int claimsBudget = Math.max(1, budget / 2);
        final long settledAfter = ReconcileScheduler.RECENT_DEPARTURE_TTLS * plane.leaseTtlMillis();
        for (String nodeId : plane.nodesWithClaims()) {
            if (counts[0] >= claimsBudget) {
                break;
            }
            if (live.contains(nodeId)) {
                continue;
            }
            final var lease = plane.membership().read(nodeId);
            if (lease.isPresent() && now - lease.get().expiresAtMillis() < settledAfter) {
                continue;
            }
            for (Map.Entry<String, Integer> claim : plane.claimsOf(nodeId)) {
                if (counts[0] >= claimsBudget) {
                    break;
                }
                if (Math.floorMod((claim.getKey() + "#" + claim.getValue()).hashCode(), live.size()) != rank) {
                    continue;
                }
                counts[0]++;
                try {
                    settleForJanitor(claim.getKey(), claim.getValue(), nodeId, -1L, counts, replaysLeft);
                } catch (Exception e) {
                    logger.debug("janitor could not settle " + claim + " of " + nodeId, e);
                }
            }
        }

        // One rollup bucket, rotating through every group's buckets; this node reads the ones that hash to it.
        final List<String> groups = plane.rollups().groups();
        java.util.Collections.sort(groups);
        String read = null;
        final int slots = groups.size() * DigestRollups.BUCKETS;
        // This node's slots are every live-size-th one from its rank; the cursor counts through them, and stays on a
        // bucket until a pass gets through it within the budget -- a crashed fleet leaves hundreds of marks in one.
        final int mine = slots <= rank ? 0 : (slots - rank + live.size() - 1) / live.size();
        if (mine > 0) {
            final long slot = rank + (long) live.size() * Math.floorMod(janitorCursor.get(), (long) mine);
            final String group = groups.get((int) (slot / DigestRollups.BUCKETS));
            final int bucket = (int) (slot % DigestRollups.BUCKETS);
            read = group + "/" + bucket;
            for (Map.Entry<String, DigestRollups.Entry> entry : plane.rollups().readBucket(group, bucket).entrySet()) {
                if (entry.getValue().alias() || counts[0] >= budget) {
                    continue;
                }
                if (DigestRollups.recordsNothing(entry.getValue())) {
                    // An entry no wide search can rule out: opened once and given back with nothing written, which
                    // needs no entry; or holding data under no digest, which gets the one its commit carries -- or a
                    // replay, whose publish computes one.
                    counts[0]++;
                    try {
                        if (plane.forgetIfNothing(entry.getKey())) {
                            counts[1]++;
                        } else {
                            final List<Integer> replay = plane.backfillDigest(entry.getKey());
                            if (replay.isEmpty()) {
                                counts[1]++;
                            }
                            for (int shard : replay) {
                                if (replaysLeft[0] > 0) {
                                    replaysLeft[0]--;
                                    counts[2]++;
                                    replayAndRelease(entry.getKey(), shard);
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.debug("janitor could not take " + entry.getKey() + " out of its rollup", e);
                    }
                    continue;
                }
                final List<DigestRollups.ShardState> states = entry.getValue().states();
                for (int shard = 0; shard < states.size() && counts[0] < budget; shard++) {
                    final long marked = states.get(shard).ownerTerm();
                    if (marked == 0L) {
                        continue;
                    }
                    counts[0]++;
                    try {
                        final var head = plane.heads().read(entry.getKey(), shard);
                        if (head.isEmpty()) {
                            continue;
                        }
                        final String owner = head.get().ownerNodeId();
                        if (owner == null) {
                            settleForJanitor(entry.getKey(), shard, null, marked, counts, replaysLeft);
                        } else if (owner.equals(node.localNode().getId()) == false && leaseLiveUntil(owner).isEmpty()) {
                            settleForJanitor(entry.getKey(), shard, owner, -1L, counts, replaysLeft);
                        }
                    } catch (Exception e) {
                        logger.debug("janitor could not settle the mark on " + entry.getKey() + "[" + shard + "]", e);
                    }
                }
            }
            if (counts[0] < budget) {
                // Got through it: the next pass reads the next bucket.
                janitorCursor.incrementAndGet();
            }
        }
        final JanitorPass pass = new JanitorPass(counts[0], counts[1], counts[2], counts[3], read, counts[0] >= budget);
        if (pass.released() + pass.replays() + pass.claimsForgotten() > 0) {
            logger.info(
                "janitor: looked at {} shards, gave back {} fully published, took {} to replay, forgot {} stale claims",
                pass.examined(),
                pass.released(),
                pass.replays(),
                pass.claimsForgotten()
            );
        }
        return pass;
    }

    /** One shard: given back clean, taken to replay within the budget, or -- a claim whose head moved on -- forgotten. */
    private void settleForJanitor(String index, int shard, String deadOwner, long markedTerm, int[] counts, int[] replaysLeft)
        throws IOException {
        final MetadataPlane.Settled settled = deadOwner == null
            ? plane.settleReleased(index, shard, markedTerm)
            : plane.settleAbandoned(index, shard, deadOwner);
        switch (settled) {
            case RELEASED_CLEAN -> counts[1]++;
            case NEEDS_REPLAY -> {
                if (deadOwner != null) {
                    plane.forgetAssignment(deadOwner, index, shard);
                }
                if (replaysLeft[0] > 0) {
                    replaysLeft[0]--;
                    counts[2]++;
                    replayAndRelease(index, shard);
                }
            }
            case NOT_ABANDONED -> {
                if (deadOwner != null && markedTerm < 0L) {
                    final var head = plane.heads().read(index, shard);
                    if (head.isEmpty() || deadOwner.equals(head.get().ownerNodeId()) == false) {
                        plane.forgetAssignment(deadOwner, index, shard);
                        counts[3]++;
                    }
                }
            }
        }
    }

    /**
     * Takes a shard left behind its log, replays and publishes it, and gives it straight back.
     *
     * <p>Held after the replay, every such shard sat on this node's cap until idle release: a pass could afford to
     * replay only a few, and a crashed fleet leaves thousands -- every shard whose commit predates commits recording
     * how far into the log they reach looks behind until replayed once. Given back as soon as it is published, a
     * replay costs a slot for seconds. A shard somebody used meanwhile is kept, for the idle release to judge.
     */
    private void replayAndRelease(String index, int shard) {
        // The janitor's: counted with its passes, though it runs on the activation threads.
        final java.util.concurrent.atomic.AtomicLong sink = org.opensearch.serverless.store.ObjectStores.currentAttribution();
        activate(index, shard, true, sink).whenComplete((taken, failure) -> {
            if (failure != null || taken.isEmpty()) {
                return;
            }
            final ShardId shardId = taken.get();
            final Long opened = openedAt.get(shardId);
            final java.util.OptionalLong used = node.reconciler().lastUsed(shardId);
            if (opened != null && used.isPresent() && used.getAsLong() > opened) {
                return;
            }
            try {
                org.opensearch.serverless.store.ObjectStores.attributedTo(
                    sink,
                    () -> letGo(shardId, "replayed and published by the janitor; nobody is using it")
                );
            } catch (Exception e) {
                logger.debug("could not give back " + shardId + " after the janitor replayed it", e);
            }
        });
    }

    /** Dead owners' shards given back without being taken, since this node started. */
    private final java.util.concurrent.atomic.AtomicLong settledClean = new java.util.concurrent.atomic.AtomicLong();

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

    /**
     * Refreshes the membership view if it is older than {@code maxAgeMillis}: one register read, and a lease read
     * per member whose last-read lease has run out -- which is how a departure is noticed.
     *
     * @param maxAgeMillis how old the view may be
     * @throws IOException if the view cannot be read
     */
    public void refreshMembership(long maxAgeMillis) throws IOException {
        plane.membership().refreshIfOlderThan(maxAgeMillis);
    }

    /** How many shards a node activates at once by default, or the most when the limit adapts. */
    public static final int DEFAULT_ACTIVATION_CONCURRENCY = 8;

    /** Where the adaptive activation limit starts, and the fewest it falls to. */
    static final int ACTIVATION_LIMIT_START = 8;
    static final int ACTIVATION_LIMIT_FLOOR = 4;

    /** How long an activation that opened a shard should take, by default; longer means the store is the limit. */
    public static final long DEFAULT_ACTIVATION_TARGET_MILLIS = 2_000L;

    private volatile boolean adaptiveActivation;
    private volatile long activationTargetMillis = DEFAULT_ACTIVATION_TARGET_MILLIS;

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
        drainActivations();
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

    private final Object activationLimitLock = new Object();
    private double activationLimit = ACTIVATION_LIMIT_START;
    private double activationRunEwmaMillis = -1;
    private long activationLimitCutAtNanos = Long.MIN_VALUE;

    /**
     * The activations allowed at once now: the adaptive limit, under the configured most.
     *
     * @return the limit
     */
    public int currentActivationLimit() {
        if (adaptiveActivation == false) {
            return activationConcurrency;
        }
        synchronized (activationLimitLock) {
            return Math.max(1, Math.min(activationConcurrency, (int) Math.floor(activationLimit)));
        }
    }

    /**
     * Moves the number of shards activated at once with how fast the store takes them, as the write path's limiter does
     * for appends: down by a quarter when activations run long, at most once a second; up by one a window while they run
     * well inside the target.
     *
     * <p>An activation spends nearly all of its time waiting on the object store. Eight at a time left a fast store idle
     * while a dead node's shards queued; thirty-two at a time on a single-drive store made each one slower -- from 1.4 s
     * to 2.3 s, and to nearly 10 s under a takeover -- and steady writes paid for it. The right number is the store's,
     * not a constant.
     *
     * @param runNanos how long an activation that opened a shard took
     */
    public void adaptActivationLimit(long runNanos) {
        if (adaptiveActivation == false) {
            return;
        }
        final long target = activationTargetMillis;
        final double runMillis = runNanos / 1_000_000.0;
        synchronized (activationLimitLock) {
            activationRunEwmaMillis = activationRunEwmaMillis < 0 ? runMillis : 0.8 * activationRunEwmaMillis + 0.2 * runMillis;
            final long now = System.nanoTime();
            if (activationRunEwmaMillis > target) {
                if (activationLimitCutAtNanos == Long.MIN_VALUE || now - activationLimitCutAtNanos >= 1_000_000_000L) {
                    activationLimit = Math.max(ACTIVATION_LIMIT_FLOOR, activationLimit * 0.75);
                    activationLimitCutAtNanos = now;
                }
            } else if (activationRunEwmaMillis < target / 2.0) {
                activationLimit = Math.min(activationConcurrency, activationLimit + 1.0 / activationLimit);
            }
        }
        drainActivations();
    }

    /**
     * Sets how many shards this node activates at once.
     *
     * @param concurrency the bound, at least one
     * @return this, for chaining
     */
    public BackgroundReconciler setActivationConcurrency(int concurrency) {
        this.activationConcurrency = Math.max(1, concurrency);
        drainActivations();
        return this;
    }

    /** One shard's activation, run by exactly one caller at a time. */
    private Optional<ShardId> activateNow(String indexName, int shard, boolean onDemand) throws Exception {
        final ShardId open = node.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(indexName) && s.id() == shard)
            .findFirst()
            .orElse(null);
        if (open != null && node.reconciler().readerShards().contains(open) == false) {
            return Optional.of(open);
        }
        boolean reserved = false;
        if (onDemand) {
            // heldShards, not openShards: a frozen view occupies the node as much as any other shard, so
            // it counts against the bound. makeRoom only ever considers openShards, so counting one here
            // can refuse an activation the node has no room for -- which is the point -- but can never
            // take a view away from the caller holding it. The slots other activations in flight have
            // reserved count too, or a burst of them would each see room for one and all take it.
            boolean full;
            synchronized (capLock) {
                full = open == null && node.reconciler().heldShards().size() + capReserved >= maxShardsHeld;
                if (full == false && open == null) {
                    capReserved++;
                    reserved = true;
                }
            }
            if (full && heldByAnotherLiveNode(indexName, shard)) {
                // Nothing to take, so nothing to make room for. Every survivor of a dead node queues every one of its
                // shards; the ones another survivor has taken by the time this reaches them used to cost an eviction
                // each -- a publish and a head release, for a shard this node then found it could not have -- and
                // survivors at their cap took a dead node's 321 shards at p50 131 s.
                return Optional.empty();
            }
            if (full) {
                // Room is made outside the lock. An eviction is a publish and a head release, and every activation
                // reserves its slot under this lock: evicting inside it made the survivors of a node that died with
                // them at the cap take its shards one eviction at a time, p99 two minutes.
                makeRoom();
            }
            synchronized (capLock) {
                if (reserved) {
                    full = false;
                } else {
                    // makeRoom answers for what is held, not for what is reserved, so the reservations are counted
                    // again after it: room it made may already be spoken for.
                    full = open == null && node.reconciler().heldShards().size() + capReserved >= maxShardsHeld;
                }
                if (full && headNamesThisNode(indexName, shard) == false) {
                    // Refusing is a routing outcome, not an error. Saying so is the difference between a
                    // node that is full and a node that is broken, and only one of them should page anyone.
                    logger.info(
                        "not taking {}[{}] on demand: holding {} shards, the cap, and all of them are in use",
                        indexName,
                        shard,
                        maxShardsHeld
                    );
                    node.noteActivationRefusedAtCap(indexName, shard);
                    return Optional.empty();
                }
                if (open == null && reserved == false) {
                    capReserved++;
                    reserved = true;
                }
            }
        }
        try {
            if (open != null) {
                // Held as a reader. That used to count as held, so a node that had searched a shard could
                // never become its writer until idle release let the reader go. A reader holds no head
                // and no unpublished write, so closing it costs nothing.
                node.reconciler().releaseShard(open, "reopening as a writer");
            }
            final Optional<ShardId> taken = node.activateWriter(plane, indexName, shard);
            // Its idle clock starts here, so a shard just taken is not immediately a candidate for being
            // given straight back -- whether it was wanted or taken on demand.
            taken.ifPresent(shardId -> openedAt.put(shardId, plane.clock().getAsLong()));
            return taken;
        } finally {
            if (reserved) {
                synchronized (capLock) {
                    capReserved--;
                }
            }
        }
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
            // What the commit about to be flushed is certain to hold: every operation at or below the processed
            // checkpoint, read before the flush. Not the highest sequence number assigned -- an operation can be
            // assigned one and still be on its way into the index when the flush commits, and recording its
            // number as published let a later release skip the publish that would have carried it, leaving an
            // acknowledged write readable only after someone replayed the log.
            final long covered = shard.seqNoStats().getLocalCheckpoint();
            if (maxSeqNo < 0 || maxSeqNo <= lastPublishedMaxSeqNo.getOrDefault(shardId, -1L)) {
                // Nothing new. Publishing anyway would flush a fresh commit and upload it every tick,
                // forever, on an idle shard -- an object-store bill for saying nothing happened. This
                // guard stays on the edge path too: a spurious mark must not become an upload.
                continue;
            }
            if (node.isWriteFenced(shardId)) {
                // Its engine holds an operation its log does not. Publishing that commit would make the
                // failure the caller was told a lie; the shard is reopened from its log by the next
                // heartbeat that reaches the store, and published after.
                continue;
            }
            try {
                // The heartbeat read this head a moment ago; the manifest's own compare-and-swap is the
                // fence that matters, so a head up to one renewal old is as good here as a fresh read.
                final var head = recentOrReadHead(shardId);
                if (head.isEmpty() || node.localNode().getId().equals(head.get().ownerNodeId()) == false) {
                    continue;
                }
                synchronized (publishLock(shardId)) {
                    lastManifests.put(shardId, node.publishShard(shardId, head.get().term()));
                }
            } catch (org.opensearch.serverless.store.StaleWriterException e) {
                // A newer term has already published. This node is a zombie for this shard: it read a
                // head that named it, and by the time it wrote, it did not. Stop serving it now rather
                // than at the next renewal, and go look at ownership -- this is exactly the evidence
                // ownershipDoubted exists for. Under the fence, so a write in flight is not acknowledged
                // against a shard that is being closed as fenced.
                try {
                    node.underShardFence(shardId, () -> {
                        node.reconciler().releaseShard(shardId, "fenced while publishing: " + e.getMessage());
                        return null;
                    });
                } catch (Exception releaseFailure) {
                    logger.warn("could not release " + shardId + " after a fenced publish", releaseFailure);
                }
                lastPublishedMaxSeqNo.remove(shardId);
                fenced.add(shardId);
                continue;
            } catch (Exception e) {
                // One shard's upload failing must not defer every other dirty shard to the backstop:
                // the marks were drained before this loop began, so an exception out of it silently left
                // the rest unpublished for up to a backstop interval. This shard is re-marked and the
                // loop goes on.
                logger.warn("could not publish " + shardId + "; it is re-marked and will be retried", e);
                dirty.add(shardId);
                continue;
            }
            lastPublishedMaxSeqNo.put(shardId, covered);
            published.add(shardId);
        }
        return published;
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
        hints.refresh(
            (index, shard) -> recentOrReadHead(new ShardId(new org.opensearch.core.index.Index(index, "_na_"), shard)),
            shardCounts,
            nowMillis
        );
        return hints.size();
    }

    /** A head the heartbeat read within one renewal, or a fresh read that is then noted for the others. */
    private Optional<org.opensearch.serverless.metadata.ShardHead> recentOrReadHead(ShardId shardId) throws IOException {
        final Optional<org.opensearch.serverless.metadata.ShardHead> recent = node.recentHead(
            shardId.getIndexName(),
            shardId.id(),
            Math.max(1_000L, plane.leaseTtlMillis() / ReconcileScheduler.RENEWALS_PER_TTL)
        );
        if (recent.isPresent()) {
            return recent;
        }
        final Optional<org.opensearch.serverless.metadata.ShardHead> read = plane.heads().read(shardId.getIndexName(), shardId.id());
        node.noteHead(shardId.getIndexName(), shardId.id(), read.orElse(null));
        return read;
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

    private final Set<ShardId> pendingSweeps = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Notes shards published outside a pass, so the next pass sweeps what they left behind.
     *
     * @param published the shards an edge-triggered publish uploaded
     */
    public void noteForSweep(Collection<ShardId> published) {
        pendingSweeps.addAll(published);
    }

    private Set<ShardId> drainPendingSweeps() {
        final Set<ShardId> drained = new LinkedHashSet<>(pendingSweeps);
        pendingSweeps.removeAll(drained);
        return drained;
    }

    /**
     * Publishes and releases every writer shard this node holds, for a clean shutdown.
     *
     * <p>Without this a node that stopped cleanly left its heads in place, and on restart with the same id
     * inherited them at the old term while every peer read them as dead. Handing them over is what makes
     * a restart a handover rather than a failover: the next owner acquires at a fresh term, and nothing
     * this node acknowledged is behind a seal.
     *
     * @return how many shards were handed over
     */
    public int handOverAll() {
        int handed = 0;
        for (ShardId shardId : new java.util.ArrayList<>(node.reconciler().openShards())) {
            if (node.reconciler().readerShards().contains(shardId)) {
                continue;
            }
            if (letGo(shardId, "shutting down")) {
                handed++;
            }
        }
        return handed;
    }

    private final java.util.concurrent.ConcurrentHashMap<ShardId, Object> publishLocks = new java.util.concurrent.ConcurrentHashMap<>();

    /** One lock per shard, so the backstop and the edge-triggered publish of one shard do not overlap. */
    private Object publishLock(ShardId shardId) {
        return publishLocks.computeIfAbsent(shardId, key -> new Object());
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
