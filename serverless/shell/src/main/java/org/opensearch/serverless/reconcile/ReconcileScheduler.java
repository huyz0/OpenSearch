/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * What makes a node run by itself.
 *
 * <p>Everything before this was callable and nothing was called. {@code tick()} and {@code heartbeat()}
 * existed and worked, and were invoked exclusively by tests — so a real deployment would have let its
 * leases lapse, published nothing, and never picked up a dead node's shards. That was not an unverified
 * claim; it was an absent mechanism.
 *
 * <p><b>Three drivers, not one timer.</b> Wrapping the whole tick in one fixed delay would have been a
 * smaller change and a worse design, because it makes every latency in the system equal to one number:
 *
 * <ul>
 *   <li><b>Lease renewal</b> is on a fixed schedule at a fraction of the TTL, and is the only job here
 *       that genuinely needs a clock. Renewing at {@code ttl / RENEWALS_PER_TTL} means a node has to
 *       miss several consecutive renewals before it loses a shard it is still healthy enough to serve —
 *       a lease that lapses under a live node is the expensive failure, since the shard then moves for
 *       no reason and everything written since the last publish has to be replayed.</li>
 *   <li><b>Publication</b> is edge-triggered by {@link #wrote} and <b>coalesced</b>, never per-write.
 *       The first write in a quiet period schedules a publish one debounce window out; every write
 *       inside that window joins it. So publication costs one object-store upload per window whatever
 *       the write rate — where publishing per write would turn a 10k/s ingest into 10k uploads/s, and
 *       publishing on the backstop's timer would put every write's loss window at the backstop
 *       interval.</li>
 *   <li><b>Activation</b> is triggered by failure, via {@link #ownershipDoubted}. A publish fenced by a
 *       newer term, a forward to an owner with no live lease, a write to a shard nobody holds — each is
 *       evidence that a head is stale, and each arrives long before a timer would notice. Also
 *       coalesced: a storm of failures against one dead node is one pass, not one pass per request.</li>
 * </ul>
 *
 * <p><b>The backstop is not optional.</b> It runs the full {@link BackgroundReconciler#tick} slowly, and
 * it is what makes the two edges safe to be lossy. Edges are accelerators; the timer is the guarantee.
 * A system that converged only on edges would wedge the first time one was dropped, and the symptom
 * would be a shard that no node serves and no node is looking for.
 *
 * <p><b>Every action still goes through compare-and-swap.</b> Nothing here is privileged, nothing needs
 * to be elected, and any number of nodes may run all of it at once. A false signal costs one wasted
 * read.
 */
public final class ReconcileScheduler implements ReconcileSignals, Closeable {

    private static final Logger logger = LogManager.getLogger(ReconcileScheduler.class);

    /**
     * How many renewals fit inside one lease TTL.
     *
     * <p>Three means <em>one</em> may be lost — to a GC pause, a slow object store, a dropped connection
     * — before the lease lapses, and usually two. Not "two may be lost": the renewal is jittered by up to
     * {@link #JITTER_FRACTION} either side, and the holder treats its lease as gone a skew margin before
     * the stamped expiry ({@code BlobLeaseMembership#selfLeaseValidAt}), so after a renewal at {@code t}
     * the next two land no later than {@code t + 2 × 1.2 × TTL/3 = t + 0.8 TTL}, inside the margin, and a
     * third only usually does. The claim this used to make was one lost renewal stronger than the
     * arithmetic.
     *
     * <p>What keeps a healthy node from lapsing is not this number but the shape of the renewal: the
     * lease is renewed on its own timer, and the next renewal is scheduled <em>before</em> the pass goes
     * on to read one shard-head per held shard. A node holding a thousand shards on a slow object store
     * used to lapse because that scan ran ahead of the next renewal's scheduling; it no longer can.
     */
    public static final int RENEWALS_PER_TTL = 3;

    /**
     * How many renewal passes go by between checks that a reader's commit has moved on.
     *
     * <p>Readers used to be refreshed only by the backstop, so a reader on a hot index answered from a
     * commit up to a backstop interval old. Every other renewal cuts that lag to two renewal intervals
     * -- two thirds of a TTL, against a backstop of one -- for a third more manifest reads per reader.
     * Every renewal would cut it further for three times the reads; a manifest read per reader per pass
     * is already the largest per-reader cost, so the middle is taken.
     */
    public static final int READER_REFRESH_EVERY_RENEWALS = 2;

    /**
     * How far either side of the renewal interval each renewal is spread, to keep a co-started fleet
     * from renewing in lockstep.
     */
    public static final double JITTER_FRACTION = 0.2;

    /** How long writes are gathered before one publish covers them all. */
    public static final TimeValue DEFAULT_PUBLISH_DEBOUNCE = TimeValue.timeValueMillis(500);

    /** How many distinct doubted shards are remembered between activation passes. */
    public static final int MAX_PENDING_DOUBTS = 1024;

    /** How often the full pass runs regardless of what any edge did or failed to do. */
    public static final TimeValue DEFAULT_BACKSTOP_INTERVAL = TimeValue.timeValueSeconds(30);

    private final BackgroundReconciler loop;
    private final ThreadPool threadPool;
    private final LongSupplier clock;
    private final TimeValue renewalInterval;
    private final TimeValue publishDebounce;
    private final TimeValue backstopInterval;

    private final AtomicBoolean publishPending = new AtomicBoolean();
    private final AtomicBoolean activationPending = new AtomicBoolean();
    private final java.util.Set<Map.Entry<String, Integer>> doubted = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final AtomicLong renewals = new AtomicLong();
    private final AtomicLong publishes = new AtomicLong();
    private final AtomicLong activations = new AtomicLong();
    private final AtomicLong backstops = new AtomicLong();

    private volatile Scheduler.ScheduledCancellable renewalTask;
    private volatile Scheduler.Cancellable backstopTask;
    private volatile Scheduler.Cancellable janitorTask;
    private volatile TimeValue janitorInterval;
    private final AtomicLong janitorPasses = new AtomicLong();
    private final AtomicLong janitorRequests = new AtomicLong();
    private final AtomicLong janitorExamined = new AtomicLong();
    private final AtomicLong janitorReleased = new AtomicLong();
    private final AtomicLong janitorReplays = new AtomicLong();
    private final AtomicLong janitorForgotten = new AtomicLong();

    /** What the janitor has done since the node started, and the store requests its passes made. */
    public record JanitorStats(long passes, long storeRequests, long examined, long released, long replays, long claimsForgotten) {
    }

    /**
     * Returns the janitor's counters. Store requests are those its passes made on their own thread; the replays it
     * starts run as activations and are counted with them.
     *
     * @return the counters
     */
    public JanitorStats janitorStats() {
        return new JanitorStats(
            janitorPasses.get(),
            janitorRequests.get(),
            janitorExamined.get(),
            janitorReleased.get(),
            janitorReplays.get(),
            janitorForgotten.get()
        );
    }

    /**
     * Sets how often the janitor cleans up after crashed nodes; null, the default, runs none. Takes effect at
     * {@link #start}.
     *
     * @param interval the interval, or null
     * @return this, for chaining
     */
    public ReconcileScheduler setJanitorInterval(TimeValue interval) {
        this.janitorInterval = interval;
        return this;
    }

    /** How soon a janitor pass follows one that used its whole budget: a backlog is worked through, not trickled. */
    static final TimeValue JANITOR_BACKLOG_DELAY = TimeValue.timeValueSeconds(5);

    private void scheduleJanitor(TimeValue delay) {
        if (closed) {
            return;
        }
        janitorTask = threadPool.schedule(() -> {
            final BackgroundReconciler.JanitorPass pass = janitorNow();
            scheduleJanitor(pass != null && pass.full() ? JANITOR_BACKLOG_DELAY : janitorInterval);
        }, delay, ThreadPool.Names.GENERIC);
    }

    /**
     * Runs one janitor pass, now; see {@link BackgroundReconciler#sweepAbandoned}.
     *
     * @return what it did, or null if it failed
     */
    public BackgroundReconciler.JanitorPass janitorNow() {
        janitorPasses.incrementAndGet();
        try {
            final BackgroundReconciler.JanitorPass pass = org.opensearch.serverless.store.ObjectStores.attributedTo(
                janitorRequests,
                () -> loop.sweepAbandoned(BackgroundReconciler.DEFAULT_JANITOR_BUDGET, BackgroundReconciler.DEFAULT_JANITOR_REPLAYS)
            );
            janitorExamined.addAndGet(pass.examined());
            janitorReleased.addAndGet(pass.released());
            janitorReplays.addAndGet(pass.replays());
            janitorForgotten.addAndGet(pass.claimsForgotten());
            return pass;
        } catch (Exception e) {
            logger.warn("janitor pass failed; the next one will try again", e);
            return null;
        }
    }

    private volatile boolean closed;

    /**
     * Builds a scheduler for a node, wires it as that node's signal sink, and starts its timers.
     *
     * <p>The one call a bootstrap makes to turn a node that has to be driven into one that runs. Every
     * interval is derived from the plane rather than configured separately, because the numbers that
     * matter are relative to the lease TTL and a TTL and a renewal interval chosen independently is a
     * lease that lapses under a healthy node.
     *
     * <p><b>The caller owns closing it.</b> Closing cancels the timers; not closing leaves them until
     * the node's thread pool terminates, which is safe but leaves a node doing work after it was meant
     * to stop.
     *
     * @param node the node to drive, whose signals are redirected here
     * @param plane the metadata plane, for its TTL and clock
     * @param loop the reconciler to drive
     * @return the started scheduler
     */
    public static ReconcileScheduler startFor(
        org.opensearch.serverless.shell.ServerlessNode node,
        org.opensearch.serverless.metadata.MetadataPlane plane,
        BackgroundReconciler loop
    ) {
        final ReconcileScheduler scheduler = new ReconcileScheduler(loop, node.threadPool(), plane.clock(), plane.leaseTtlMillis());
        node.setSignals(scheduler);
        return scheduler.start();
    }

    /**
     * Creates a scheduler with intervals derived from the lease TTL.
     *
     * @param loop the reconciler to drive
     * @param threadPool where the work runs
     * @param clock the plane's clock, so hint staleness is judged the same way everywhere
     * @param leaseTtlMillis the lease TTL the renewal schedule must beat
     */
    public ReconcileScheduler(BackgroundReconciler loop, ThreadPool threadPool, LongSupplier clock, long leaseTtlMillis) {
        this(
            loop,
            threadPool,
            clock,
            TimeValue.timeValueMillis(Math.max(1L, leaseTtlMillis / RENEWALS_PER_TTL)),
            DEFAULT_PUBLISH_DEBOUNCE,
            DEFAULT_BACKSTOP_INTERVAL
        );
    }

    /**
     * Creates a scheduler with explicit intervals.
     *
     * @param loop the reconciler to drive
     * @param threadPool where the work runs
     * @param clock the plane's clock
     * @param renewalInterval how often leases are renewed; must be well under the TTL
     * @param publishDebounce how long writes are gathered before one publish
     * @param backstopInterval how often the full pass runs, or null to disable it entirely — which is
     *        for tests that need to prove an edge did the work, and is not a supported way to run a node
     */
    public ReconcileScheduler(
        BackgroundReconciler loop,
        ThreadPool threadPool,
        LongSupplier clock,
        TimeValue renewalInterval,
        TimeValue publishDebounce,
        TimeValue backstopInterval
    ) {
        this.loop = loop;
        this.threadPool = threadPool;
        this.clock = clock;
        this.renewalInterval = renewalInterval;
        this.publishDebounce = publishDebounce;
        this.backstopInterval = backstopInterval;
    }

    /**
     * Starts the timers. The edges work whether or not this was called; the timers do not.
     *
     * @return this, for chaining onto a constructor
     */
    public ReconcileScheduler start() {
        // The backstop must not renew the lease as well: with the timer running, a fourth renewal per
        // TTL bought nothing but a third more lease writes. The backstop still verifies heads.
        loop.setRenewalDrivenByTimer(true);
        // A departure -- a lease that ran out -- is when a dead node's shards should be taken, not when a request
        // for each happens to find it. The membership view is refreshed on the renewal timer, so one is noticed
        // within a renewal interval of the lease running out.
        loop.membership().subscribe(delta -> {
            for (org.opensearch.serverless.membership.NodeLease left : delta.left()) {
                // Only a departure that just happened. A lease that ran out long ago belongs to a node whose shards
                // are dormant -- or to a fleet replaced wholesale, whose every shard taken at once would be a storm --
                // and those are taken as requests ask for them. Once per incarnation: a departure can be heard again.
                final long sinceExpiry = clock.getAsLong() - left.expiresAtMillis();
                if (closed == false && sinceExpiry <= RECENT_DEPARTURE_TTLS * leaseTtlMillis()) {
                    threadPool.generic().execute(() -> takeOverIfGone(left, 0));
                }
            }
        });
        scheduleNextRenewal();
        if (backstopInterval != null) {
            backstopTask = threadPool.scheduleWithFixedDelay(this::backstopNow, backstopInterval, ThreadPool.Names.GENERIC);
        }
        if (janitorInterval != null) {
            scheduleJanitor(janitorInterval);
        }
        logger.info(
            "reconcile scheduler started: renewal every {}, publish debounce {}, backstop {}",
            renewalInterval,
            publishDebounce,
            backstopInterval == null ? "disabled" : backstopInterval
        );
        return this;
    }

    /**
     * Returns the delay until the next renewal: the interval, spread by up to {@link #JITTER_FRACTION}
     * either side.
     *
     * <p>Without this, a thousand nodes started by the same orchestrator in the same second renew in the
     * same millisecond, forever — the interval is fixed, so nothing ever pulls them apart. That is a
     * thundering herd against the object store every {@code ttl/3}, and it gets worse as the fleet grows,
     * which is exactly when it is least affordable.
     *
     * <p>Re-jittered on <em>every</em> renewal rather than only the first, so nodes keep drifting apart
     * instead of locking back into step after a shared pause.
     *
     * @return the delay to use for the next renewal
     */
    public TimeValue nextRenewalDelay() {
        final long base = renewalInterval.millis();
        final long spread = (long) (base * JITTER_FRACTION);
        if (spread <= 0) {
            return renewalInterval;
        }
        // Never below 1ms, and never so long that the jitter itself could outlive the lease: the spread
        // is a fraction of an interval that is already a fraction of the TTL.
        return TimeValue.timeValueMillis(Math.max(1L, base + ThreadLocalRandom.current().nextLong(-spread, spread + 1)));
    }

    private void scheduleNextRenewal() {
        if (closed) {
            return;
        }
        renewalTask = threadPool.schedule(() -> {
            java.util.Set<ShardId> released;
            try {
                released = renewLeaseNow();
                // After the renewal, never before it: reading the membership is I/O the lease must not wait for.
                try {
                    loop.refreshMembership(renewalInterval.millis());
                } catch (Exception e) {
                    logger.warn("could not refresh the membership view; departures are noticed late", e);
                }
            } finally {
                // In a finally, and renewLeaseNow never throws: a renewal loop that stops rescheduling
                // because one pass failed is a node that looks alive until its lease lapses.
                //
                // And scheduled here, before the head scan below, on purpose. The next renewal is timed
                // from this one's start, never from the end of a pass that reads one head per held shard
                // -- that scan is O(shards held) object-store round trips, and a node that scheduled its
                // next renewal only after it finished lapsed under nothing but latency once it held
                // enough shards. The lease is the one job that genuinely needs the clock; nothing else in
                // the pass is allowed to hold it up.
                scheduleNextRenewal();
            }
            // The scan reads a head per held shard and is not what keeps the lease alive: back to GENERIC with it.
            final java.util.Set<ShardId> releasedByRenewal = released;
            threadPool.generic().execute(() -> verifyNow(releasedByRenewal));
        }, nextRenewalDelay(), org.opensearch.serverless.shell.ServerlessNode.LEASE_POOL);
    }

    /**
     * Renews the node lease, and nothing else. Never throws.
     *
     * @return the shards released because the lease had lapsed before it could be renewed
     */
    /** How long ago a lease may have run out and still be a departure whose shards are taken at once, in TTLs. */
    static final int RECENT_DEPARTURE_TTLS = 3;

    /** The departed incarnations already taken over from, so a departure heard twice is acted on once. */
    private final java.util.Set<String> takenOver = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private long leaseTtlMillis() {
        return Math.max(1L, renewalInterval.millis() * RENEWALS_PER_TTL);
    }

    /** How many times a departure whose lease was still live is looked at again, just after that lease would run out. */
    static final int DEPARTURE_RECHECKS = 2;

    /**
     * Takes a departed node's shards once its lease, read from the store, has run out.
     *
     * <p>A node leaves the membership view when its lease read fails or comes back late, as well as when it dies, and a
     * live node's shards taken on that evidence is a storm of contested activations. So the lease is read first; a
     * live one is looked at again just after it would expire, which is when a dead node's departure is real. The
     * incarnation is marked taken over only when the takeover happens, so a false departure does not use up the one
     * proactive takeover a real death of that incarnation gets.
     */
    private void takeOverIfGone(org.opensearch.serverless.membership.NodeLease left, int rechecks) {
        final String incarnation = left.nodeId() + "/" + left.ephemeralId();
        if (closed || takenOver.contains(incarnation)) {
            return;
        }
        long liveUntil;
        try {
            liveUntil = loop.leaseLiveUntil(left.nodeId()).orElse(Long.MIN_VALUE);
        } catch (Exception e) {
            // Unknown, not gone: look again shortly.
            liveUntil = clock.getAsLong();
        }
        if (liveUntil == Long.MIN_VALUE) {
            if (takenOver.add(incarnation)) {
                takeOver(left.nodeId());
            }
            return;
        }
        if (rechecks >= DEPARTURE_RECHECKS) {
            return;
        }
        final long delay = Math.max(0L, liveUntil - clock.getAsLong()) + renewalInterval.millis();
        threadPool.schedule(() -> takeOverIfGone(left, rechecks + 1), TimeValue.timeValueMillis(delay), ThreadPool.Names.GENERIC);
    }

    /**
     * Returns the activation queue's figures; see {@link BackgroundReconciler#activationStats}.
     *
     * @return the figures
     */
    public BackgroundReconciler.ActivationStats activationStats() {
        return loop.activationStats();
    }

    /**
     * Activations asked for by source; see {@link BackgroundReconciler#activationSources}.
     *
     * @return the counts
     */
    public long[] activationSources() {
        return loop.activationSources();
    }

    /** The two looks at a departed node's shards; see BackgroundReconciler#takeOverFrom. */
    private void takeOver(String deadNodeId) {
        try {
            final java.util.List<Map.Entry<String, Integer>> later = loop.takeOverFrom(deadNodeId);
            if (later.isEmpty() == false && closed == false) {
                threadPool.schedule(() -> {
                    final java.util.List<Map.Entry<String, Integer>> deferred = loop.takeOverRemaining(deadNodeId, later, false);
                    if (deferred.isEmpty() == false && closed == false) {
                        // The last look, a lease on: whatever the member named for it has still not taken.
                        threadPool.schedule(
                            () -> loop.takeOverRemaining(deadNodeId, deferred, true),
                            TimeValue.timeValueMillis(renewalInterval.millis() * RENEWALS_PER_TTL),
                            ThreadPool.Names.GENERIC
                        );
                    }
                }, renewalInterval, ThreadPool.Names.GENERIC);
            }
        } catch (Exception e) {
            logger.warn("could not take over the shards of departed node " + deadNodeId, e);
        }
    }

    private java.util.Set<ShardId> renewLeaseNow() {
        renewals.incrementAndGet();
        try {
            return loop.renewLease();
        } catch (Exception e) {
            // Never propagate out of a scheduled task: an exception cancels a fixed-delay schedule, so
            // one unreachable-object-store blip would silently stop this node renewing anything, for
            // good, and the node would look healthy right up until its leases lapsed.
            logger.warn("lease renewal failed; will retry on the next interval", e);
            return java.util.Set.of();
        }
    }

    /**
     * The rest of a renewal pass: verify the heads of every held writer shard, and every other pass
     * check the readers' commits. Runs after the next renewal is already on the clock.
     *
     * @param alreadyReleased shards the renewal itself released, so a doubt is raised once for the pass
     */
    private void verifyNow(java.util.Set<ShardId> alreadyReleased) {
        final java.util.Set<ShardId> released = new java.util.LinkedHashSet<>(alreadyReleased);
        try {
            released.addAll(loop.verifyHeads());
        } catch (Exception e) {
            logger.warn("shard-head verification failed; the backstop will retry", e);
        }
        if (renewals.get() % READER_REFRESH_EVERY_RENEWALS == 0) {
            try {
                loop.refreshReaders();
            } catch (Exception e) {
                logger.warn("reader refresh failed; the backstop will retry", e);
            }
        }
        if (released.isEmpty() == false) {
            // Losing a shard is itself evidence worth acting on: something else took it, so the
            // picture this node has of ownership is out of date beyond just these shards.
            ownershipDoubted(released.iterator().next().getIndexName(), released.iterator().next().id());
        }
    }

    @Override
    public void wrote(ShardId shardId) {
        loop.markDirty(shardId);
        // compareAndSet, not a lock: the point is that the second through millionth write in a window
        // are free. Only the write that finds no publish pending pays for scheduling one.
        if (closed == false && publishPending.compareAndSet(false, true)) {
            threadPool.schedule(this::publishNow, publishDebounce, ThreadPool.Names.GENERIC);
        }
    }

    @Override
    public void ownershipDoubted(String indexName, int shardNumber) {
        // Which shard is remembered, because under demand-driven activation the pass needs to try the
        // shard nobody wants -- not only the ones this node was told to want. Bounded because a storm of
        // doubts about a dead node must cost one pass, not one per request.
        if (doubted.size() < MAX_PENDING_DOUBTS) {
            doubted.add(Map.entry(indexName, shardNumber));
        }
        if (closed == false && activationPending.compareAndSet(false, true)) {
            threadPool.schedule(this::activateNow, TimeValue.ZERO, ThreadPool.Names.GENERIC);
        }
    }

    @Override
    public java.util.concurrent.CompletableFuture<java.util.Optional<ShardId>> activate(String indexName, int shardNumber) {
        if (closed) {
            return java.util.concurrent.CompletableFuture.completedFuture(java.util.Optional.empty());
        }
        activations.incrementAndGet();
        return loop.activateForRequest(indexName, shardNumber);
    }

    /**
     * Renews the lease and verifies the heads once, now: the renewal timer's whole pass, exposed so a
     * test can drive it without a clock.
     *
     * @return the shards released because this node lost them
     */
    public java.util.Set<ShardId> renewNow() {
        final java.util.Set<ShardId> released = new java.util.LinkedHashSet<>(renewLeaseNow());
        try {
            released.addAll(loop.verifyHeads());
        } catch (Exception e) {
            logger.warn("shard-head verification failed; the backstop will retry", e);
        }
        if (released.isEmpty() == false) {
            ownershipDoubted(released.iterator().next().getIndexName(), released.iterator().next().id());
        }
        return released;
    }

    private final java.util.concurrent.atomic.AtomicBoolean publishRunning = new java.util.concurrent.atomic.AtomicBoolean();

    /**
     * Publishes the shards written to since the last publish, now.
     *
     * @return the shards published
     */
    public java.util.Set<ShardId> publishNow() {
        publishPending.set(false);
        // One pass at a time. A pass used to take minutes on a node holding a thousand shards, and each write meanwhile
        // scheduled another: they piled up on GENERIC, blocked on the same shards' publish locks, until nothing else could
        // run there. A pass that finds another running leaves its marks to it; the running one goes round again while any
        // shard is dirty.
        if (publishRunning.compareAndSet(false, true) == false) {
            return java.util.Set.of();
        }
        final java.util.Set<ShardId> all = new java.util.LinkedHashSet<>();
        int rounds = 0;
        try {
            do {
                publishes.incrementAndGet();
                final var published = loop.publishDirty();
                loop.noteForSweep(published);
                all.addAll(published);
                final var fenced = loop.drainFenced();
                if (fenced.isEmpty() == false) {
                    ownershipDoubted(fenced.iterator().next().getIndexName(), fenced.iterator().next().id());
                }
                // Bounded, so a shard whose publish keeps failing -- re-marked each time -- cannot spin a pass while the
                // store is down; the rest wait one debounce.
            } while (closed == false && loop.hasDirty() && ++rounds < 10);
            return all;
        } catch (Exception e) {
            logger.warn("edge-triggered publish failed; the backstop will retry", e);
            return all;
        } finally {
            publishRunning.set(false);
            // A mark made between the last look and here would wait for the next write or the backstop.
            if (closed == false && loop.hasDirty() && publishPending.compareAndSet(false, true)) {
                threadPool.schedule(this::publishNow, publishDebounce, ThreadPool.Names.GENERIC);
            }
        }
    }

    /**
     * Runs an activation pass, now.
     *
     * @return the shards acquired
     */
    public java.util.Set<ShardId> activateNow() {
        activationPending.set(false);
        activations.incrementAndGet();
        // Drained before the work, for the same reason the dirty set is: a doubt raised mid-pass must
        // get its own pass rather than being swallowed by this one.
        final java.util.Set<Map.Entry<String, Integer>> pending = new java.util.LinkedHashSet<>(doubted);
        doubted.removeAll(pending);
        try {
            final java.util.Set<ShardId> taken = new java.util.LinkedHashSet<>(loop.activateWanted());
            taken.addAll(loop.activateOnDemand(pending));
            return taken;
        } catch (Exception e) {
            logger.warn("failure-triggered activation failed; the backstop will retry", e);
            return java.util.Set.of();
        }
    }

    /**
     * Runs the full backstop pass, now.
     *
     * @return what the pass did, or null if it failed
     */
    public BackgroundReconciler.TickResult backstopNow() {
        backstops.incrementAndGet();
        try {
            return loop.tick(clock.getAsLong());
        } catch (Exception e) {
            logger.warn("backstop reconciliation pass failed; will retry on the next interval", e);
            return null;
        }
    }

    /**
     * Returns how many times each driver has run, for tests and for operators asking which one is doing
     * the work.
     *
     * @return the counts
     */
    public Counts counts() {
        return new Counts(renewals.get(), publishes.get(), activations.get(), backstops.get());
    }

    @Override
    public void close() {
        closed = true;
        // Whoever ticks the loop after this is the only thing renewing, so the tick renews again.
        loop.setRenewalDrivenByTimer(false);
        if (renewalTask != null) {
            renewalTask.cancel();
        }
        if (backstopTask != null) {
            backstopTask.cancel();
        }
        if (janitorTask != null) {
            janitorTask.cancel();
        }
    }

    /** How many times each driver has run. */
    public static final class Counts {

        private final long renewals;
        private final long publishes;
        private final long activations;
        private final long backstops;

        Counts(long renewals, long publishes, long activations, long backstops) {
            this.renewals = renewals;
            this.publishes = publishes;
            this.activations = activations;
            this.backstops = backstops;
        }

        /**
         * Returns how many lease renewals have run.
         *
         * @return the count
         */
        public long renewals() {
            return renewals;
        }

        /**
         * Returns how many edge-triggered publishes have run.
         *
         * @return the count
         */
        public long publishes() {
            return publishes;
        }

        /**
         * Returns how many failure-triggered activation passes have run.
         *
         * @return the count
         */
        public long activations() {
            return activations;
        }

        /**
         * Returns how many full backstop passes have run.
         *
         * @return the count
         */
        public long backstops() {
            return backstops;
        }

        @Override
        public String toString() {
            return "Counts[renewals="
                + renewals
                + ", publishes="
                + publishes
                + ", activations="
                + activations
                + ", backstops="
                + backstops
                + "]";
        }
    }
}
