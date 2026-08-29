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
     * How many renewals fit inside one lease TTL. Three means two may be lost — to a GC pause, a slow
     * object store, a dropped connection — before the lease lapses.
     */
    public static final int RENEWALS_PER_TTL = 3;

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
        scheduleNextRenewal();
        if (backstopInterval != null) {
            backstopTask = threadPool.scheduleWithFixedDelay(this::backstopNow, backstopInterval, ThreadPool.Names.GENERIC);
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
            try {
                renewNow();
            } finally {
                // In a finally, and renewNow never throws: a renewal loop that stops rescheduling
                // because one pass failed is a node that looks alive until its lease lapses.
                scheduleNextRenewal();
            }
        }, nextRenewalDelay(), ThreadPool.Names.GENERIC);
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

    /**
     * Renews leases once, now. The renewal timer's body, exposed so a test can drive it without a clock.
     *
     * @return the shards released because this node lost them
     */
    public java.util.Set<ShardId> renewNow() {
        renewals.incrementAndGet();
        try {
            final var released = loop.renewLeases();
            if (released.isEmpty() == false) {
                // Losing a shard is itself evidence worth acting on: something else took it, so the
                // picture this node has of ownership is out of date beyond just these shards.
                ownershipDoubted(released.iterator().next().getIndexName(), released.iterator().next().id());
            }
            return released;
        } catch (Exception e) {
            // Never propagate out of a scheduled task: an exception cancels a fixed-delay schedule, so
            // one unreachable-object-store blip would silently stop this node renewing anything, for
            // good, and the node would look healthy right up until its leases lapsed.
            logger.warn("lease renewal failed; will retry on the next interval", e);
            return java.util.Set.of();
        }
    }

    /**
     * Publishes the shards written to since the last publish, now.
     *
     * @return the shards published
     */
    public java.util.Set<ShardId> publishNow() {
        publishPending.set(false);
        publishes.incrementAndGet();
        try {
            final var published = loop.publishDirty();
            final var fenced = loop.drainFenced();
            if (fenced.isEmpty() == false) {
                ownershipDoubted(fenced.iterator().next().getIndexName(), fenced.iterator().next().id());
            }
            return published;
        } catch (Exception e) {
            logger.warn("edge-triggered publish failed; the backstop will retry", e);
            return java.util.Set.of();
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
        if (renewalTask != null) {
            renewalTask.cancel();
        }
        if (backstopTask != null) {
            backstopTask.cancel();
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
