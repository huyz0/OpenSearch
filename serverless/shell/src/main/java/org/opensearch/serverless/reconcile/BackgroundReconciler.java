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
     * One monitor for both activation passes. The renewal timer, the backstop and a failure-triggered
     * pass can all reach {@link #activateWanted} or {@link #activateOnDemand} at once, and two of them
     * activating the same shard is two acquisitions, two seals and two opens racing on one shard.
     */
    private final Object activationLock = new Object();
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
            // to poison its prefix pattern after a year, because nothing ever removed them.
            if (reaps % TOMBSTONE_SWEEP_EVERY_REAPS == 0) {
                try {
                    collector.collectTombstones(plane, plane.clock().getAsLong());
                } catch (Exception e) {
                    logger.warn("could not sweep descriptor tombstones; the next sweep will retry", e);
                }
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
                    if (plane.describe(shardId.getIndexName()).isEmpty()) {
                        node.reconciler().releaseShard(shardId, "its index has been deleted");
                        node.forgetReader(shardId);
                        stale.add(shardId);
                    }
                    continue;
                }
                if (sameCommit(opened.get(), current.get())) {
                    continue;
                }
                node.reconciler().releaseShard(shardId, "the commit it was serving has been superseded");
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
        final int floor = Math.min(maxShardsHeld, Math.max(0, maxShardsHeld - headroomShards()));
        int evicted = 0;
        while (node.reconciler().heldShards().size() > floor) {
            final long now = plane.clock().getAsLong();
            final ShardId victim = pickVictim(now);
            if (victim == null) {
                break;
            }
            final long victimLastUsed = node.reconciler().lastUsed(victim).orElse(openedAt.getOrDefault(victim, now));
            if (letGo(victim, "evicted to make room, unused for " + (now - victimLastUsed) + "ms") == false) {
                // The head could not be released; letGo already logged why. Stopping rather than trying a
                // different victim next: a head release failing once tends to keep failing, and spinning
                // through every open shard to find one that happens to release is work for no room gained.
                break;
            }
            openedAt.remove(victim);
            if (demandDriven) {
                wanted.remove(Map.entry(victim.getIndexName(), victim.id()));
            }
            evicted++;
        }
        if (evicted > 0) {
            logger.info(
                "evicted {} shard(s) to make room at the shard cap, {} held against a cap of {} (floor {})",
                evicted,
                node.reconciler().heldShards().size(),
                maxShardsHeld,
                floor
            );
        }
        return node.reconciler().heldShards().size() < maxShardsHeld;
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
            if (node.reconciler().readerShards().contains(shardId)) {
                // A reader holds no head and no claim on anything. Closing it loses nothing at all.
                node.reconciler().releaseShard(shardId, reason);
                node.forgetReader(shardId);
                return true;
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
        final boolean unchanged = maxSeqNo < 0 || maxSeqNo == lastPublishedMaxSeqNo.getOrDefault(shardId, -1L);
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
        // And forget the claim, so a node that churns through shards does not read a head per stale
        // claim on every heartbeat for the rest of its life.
        plane.forgetAssignment(node.localNode().getId(), shardId.getIndexName(), shardId.id());
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
            return node.verifyHeads(plane);
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
        synchronized (activationLock) {
            return activateWantedLocked();
        }
    }

    private Set<ShardId> activateWantedLocked() throws Exception {
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
        synchronized (activationLock) {
            return activateOnDemandLocked(candidates);
        }
    }

    private Set<ShardId> activateOnDemandLocked(Collection<Map.Entry<String, Integer>> candidates) throws Exception {
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
            final ShardId open = node.reconciler()
                .openShards()
                .stream()
                .filter(s -> s.getIndexName().equals(candidate.getKey()) && s.id() == candidate.getValue())
                .findFirst()
                .orElse(null);
            if (open != null) {
                if (node.reconciler().readerShards().contains(open) == false) {
                    continue;
                }
                // Held as a reader. That used to count as held, so a node that had searched a shard could
                // never become its writer until idle release let the reader go. A reader holds no head
                // and no unpublished write, so closing it costs nothing.
                node.reconciler().releaseShard(open, "reopening as a writer");
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
            lastPublishedMaxSeqNo.put(shardId, maxSeqNo);
            published.add(shardId);
        }
        return published;
    }

    /**
     * Returns the highest sequence number this node has published for a shard, for the stats endpoint's
     * publish-lag figure.
     *
     * @param shardId the shard
     * @return the sequence number the last publish from this node covered, or empty if it has never
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
