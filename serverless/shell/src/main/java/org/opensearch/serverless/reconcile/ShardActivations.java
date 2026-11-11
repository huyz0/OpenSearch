/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.reconcile;

import org.opensearch.core.index.shard.ShardId;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Activates shards for {@link BackgroundReconciler}: takes a shard's head and opens its writer, admitting
 * activations a few at a time and ranking takeovers so each survivor of a failed node does its own share first.
 */
final class ShardActivations {

    private final BackgroundReconciler reconciler;

    ShardActivations(BackgroundReconciler reconciler) {
        this.reconciler = reconciler;
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

    static long rendezvous(String member, String indexName, int shard) {
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

    int rankHere(String indexName, int shard) {
        try {
            final java.util.List<String> members = new java.util.ArrayList<>();
            for (org.opensearch.serverless.membership.NodeLease lease : reconciler.plane.membership().current()) {
                members.add(lease.nodeId());
            }
            final String self = reconciler.node.localNode().getId();
            if (members.contains(self) == false) {
                members.add(self);
            }
            return takeoverRank(self, members, indexName, shard);
        } catch (RuntimeException e) {
            // Ordering is an optimisation: without a membership view, arrival order.
            return 0;
        }
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
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            final java.util.List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending = new java.util.ArrayList<>();
            for (Map.Entry<String, Integer> target : reconciler.wanted) {
                if (heldAsWriter(target.getKey(), target.getValue()) == false) {
                    reconciler.countSource("wanted");
                    pending.add(activate(target.getKey(), target.getValue(), false));
                }
            }
            return settle(pending);
        }
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
     * switch rather than a default, and {@link BackgroundReconciler#setMaxShardsHeld} bounds it: past the cap a node refuses,
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
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Set.of();
            }
            if (reconciler.demandDriven == false) {
                return Set.of();
            }
            final java.util.List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending = new java.util.ArrayList<>();
            for (Map.Entry<String, Integer> candidate : candidates) {
                if (heldAsWriter(candidate.getKey(), candidate.getValue()) == false
                    && reconciler.leases.heldByAnotherLiveNode(candidate.getKey(), candidate.getValue()) == false
                    && leftToTakeoverWinner(candidate) == false) {
                    reconciler.countSource("doubt");
                    pending.add(activate(candidate.getKey(), candidate.getValue(), true));
                }
            }
            return settle(pending);
        }
    }

    /**
     * Whether a doubted shard is a dead member's that another member is to take over: then this node leaves it. Every
     * survivor a request reached doubted the shard and queued it, so each held the whole of a dead node's shards in its
     * queue. Not left when anything is unknown -- the head unreadable, no winner -- nor when the winner is this node:
     * routing must never become nobody taking it. The winner's own departure pass, its second look and the writes routed
     * to it take the shard; if the winner fails at that, the second look on every survivor still does.
     */
    boolean leftToTakeoverWinner(Map.Entry<String, Integer> candidate) {
        if (reconciler.node.takeoverBelongsElsewhere(reconciler.plane, candidate.getKey(), candidate.getValue())) {
            reconciler.node.noteTakeoverDoubtLeft();
            return true;
        }
        return false;
    }

    /** Whether the shard is open here as a writer: a pass reports only what it newly took. */
    boolean heldAsWriter(String indexName, int shard) {
        return reconciler.node.reconciler()
            .openShards()
            .stream()
            .anyMatch(s -> s.getIndexName().equals(indexName) && s.id() == shard && reconciler.node.reconciler().isReader(s) == false);
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
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
            }
            // A shard this node was told to want is taken whatever the policy, and without the cap -- exactly as
            // the pass would take it; any other shard only under demand-driven activation.
            reconciler.countSource("request");
            if (reconciler.wanted.contains(Map.entry(indexName, shard))) {
                return activate(indexName, shard, false);
            }
            if (reconciler.demandDriven == false) {
                return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
            }
            return activate(indexName, shard, true);
        }
    }

    /** Waits for every activation of a pass and collects the shards it took; the first failure is rethrown. */
    static Set<ShardId> settle(java.util.List<java.util.concurrent.CompletableFuture<Optional<ShardId>>> pending) throws Exception {
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
            BackgroundReconciler.logger.warn("some activations of this pass failed; the next pass retries them", first);
        }
        return taken;
    }

    /**
     * Starts one shard's activation unless it is already in flight, in which case the caller shares it.
     * Taken into the map before it is queued, and out of it only by the task that ran it, so a finished
     * activation is never handed to a later caller.
     */
    java.util.concurrent.CompletableFuture<Optional<ShardId>> activate(String indexName, int shard, boolean onDemand) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
            }
            return activate(indexName, shard, onDemand, null);
        }
    }

    /** As {@link #activate(String, int, boolean)}, counting the store requests of the activation into a sink if one is given. */
    java.util.concurrent.CompletableFuture<Optional<ShardId>> activate(
        String indexName,
        int shard,
        boolean onDemand,
        java.util.concurrent.atomic.AtomicLong sink
    ) {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
            }
            final Map.Entry<String, Integer> key = Map.entry(indexName, shard);
            final java.util.concurrent.CompletableFuture<Optional<ShardId>> mine = new java.util.concurrent.CompletableFuture<>();
            synchronized (reconciler.activationAdmission) {
                if (reconciler.activity.isClosed()) {
                    return java.util.concurrent.CompletableFuture.completedFuture(Optional.empty());
                }
                final java.util.concurrent.CompletableFuture<Optional<ShardId>> running = reconciler.activating.putIfAbsent(key, mine);
                if (running != null) {
                    return running;
                }
            }
            // Compute fleet ranking outside admission's short critical section. Reserving the future
            // first still coalesces concurrent callers, so only one caller ranks a given shard.
            final long queuedAt = System.nanoTime();
            final BackgroundReconciler.QueuedActivation queued = new BackgroundReconciler.QueuedActivation(
                rankHere(indexName, shard),
                reconciler.activationSequence.incrementAndGet(),
                key,
                mine,
                () -> {
                    final long startedAt = System.nanoTime();
                    reconciler.activationWaitMax.accumulateAndGet(startedAt - queuedAt, Math::max);
                    try {
                        final Optional<ShardId> taken = sink == null
                            ? activateNow(indexName, shard, onDemand)
                            : org.opensearch.serverless.store.ObjectStores.attributedTo(
                                sink,
                                () -> activateNow(indexName, shard, onDemand)
                            );
                        if (taken.isPresent()) {
                            // Only an activation that opened a shard says how fast the store is taking them: a refusal or a
                            // shard already held elsewhere returns in a read and would make it look faster than it is.
                            adaptActivationLimit(System.nanoTime() - startedAt);
                        }
                        mine.complete(taken);
                    } catch (Throwable t) {
                        mine.completeExceptionally(t);
                    } finally {
                        reconciler.activating.remove(key, mine);
                        final long endedAt = System.nanoTime();
                        reconciler.activationsDone.incrementAndGet();
                        reconciler.activationWaitNanos.addAndGet(startedAt - queuedAt);
                        reconciler.activationRunNanos.addAndGet(endedAt - startedAt);
                        reconciler.activationRunMax.accumulateAndGet(endedAt - startedAt, Math::max);
                    }
                }
            );
            final boolean enqueued;
            synchronized (reconciler.activationAdmission) {
                enqueued = reconciler.activity.isClosed() == false;
                if (enqueued) {
                    reconciler.activationQueue.add(queued);
                }
            }
            if (enqueued) {
                drainActivations();
            } else {
                reconciler.activating.remove(key, mine);
                mine.complete(Optional.empty());
            }
            return mine;
        }
    }

    /** Starts queued activations while fewer than the bound are running; each one that ends starts the next. */
    void drainActivations() {
        if (reconciler.activity.isClosed()) {
            return;
        }
        while (true) {
            final int running = reconciler.activationsRunning.get();
            if (running >= currentActivationLimit() || reconciler.activationQueue.isEmpty()) {
                return;
            }
            if (reconciler.activationsRunning.compareAndSet(running, running + 1) == false) {
                continue;
            }
            final BackgroundReconciler.QueuedActivation queued = reconciler.activationQueue.poll();
            final Runnable task = queued == null ? null : queued.task();
            if (task == null) {
                reconciler.activationsRunning.decrementAndGet();
                continue;
            }
            try {
                // Never GENERIC: what waits for an activation waits there. See ServerlessNode#ACTIVATION_POOL.
                reconciler.node.threadPool().executor(org.opensearch.serverless.shell.ServerlessNode.ACTIVATION_POOL).execute(() -> {
                    try {
                        task.run();
                    } finally {
                        reconciler.activationsRunning.decrementAndGet();
                        drainActivations();
                    }
                });
            } catch (RuntimeException e) {
                // Refuse honestly. Running store I/O inline here could activate a shard during shutdown.
                reconciler.activating.remove(queued.key(), queued.result());
                queued.result().completeExceptionally(e);
                reconciler.activationsRunning.decrementAndGet();
            }
        }
    }

    /** The hard ceiling for takeovers: the cap and its overcommit. Nothing is taken past it. */
    int takeoverCeiling() {
        return reconciler.maxShardsHeld + (int) Math.floor(reconciler.maxShardsHeld * reconciler.takeoverOvercommit);
    }

    /**
     * Whether a shard may be taken past the cap: its head names another node whose lease is no longer live -- a dead or
     * revoked member's shard, which somebody has to take now -- and overcommitting is on. Ordinary demand still evicts
     * first: only a takeover is urgent enough to be worth holding more than the cap for a while.
     */
    boolean takeoverMayOvercommit(String indexName, int shard) {
        if (reconciler.takeoverOvercommit <= 0.0) {
            return false;
        }
        try {
            final var head = reconciler.plane.heads().read(indexName, shard);
            return head.isPresent()
                && head.get().ownerNodeId() != null
                && reconciler.node.localNode().getId().equals(head.get().ownerNodeId()) == false
                && reconciler.plane.heads().isHeld(head.get(), reconciler.plane.clock().getAsLong()) == false;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Brings a node that took a dead member's shards past its cap back down to it, a tenth of the cap a pass, and only
     * shards past their eviction grace -- so the shards just taken, which are the ones being written, stay.
     *
     * @return how many shards were let go
     */
    public int trimToCap() {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return 0;
            }
            if (reconciler.evictAfterMillis <= 0 || reconciler.node.reconciler().heldShards().size() <= reconciler.maxShardsHeld) {
                return 0;
            }
            return reconciler.eviction.evictDownTo(reconciler.maxShardsHeld, Math.max(1, reconciler.maxShardsHeld / 10), false);
        }
    }

    /**
     * The activations allowed at once now: the adaptive limit, under the configured most.
     *
     * @return the limit
     */
    public int currentActivationLimit() {
        final int limit;
        if (reconciler.adaptiveActivation == false) {
            limit = reconciler.activationConcurrency;
        } else {
            synchronized (reconciler.activationLimitLock) {
                limit = Math.max(1, Math.min(reconciler.activationConcurrency, (int) Math.floor(reconciler.activationLimit)));
            }
        }
        if (reconciler.pacedByWrites == false) {
            return limit;
        }
        // Paced by the write path's limiter: a takeover storm saturated the store, and the live writes paid -- a fleet run
        // shed 77% of them while survivors opened a dead node's shards. While the limiter has cut how many writes may wait,
        // activations are cut in the same proportion; never below one, so takeovers still move.
        return Math.max(1, (int) Math.floor(limit * reconciler.node.writeHeadroom()));
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
        if (reconciler.adaptiveActivation == false) {
            return;
        }
        final long target = reconciler.activationTargetMillis;
        final double runMillis = runNanos / 1_000_000.0;
        synchronized (reconciler.activationLimitLock) {
            reconciler.activationRunEwmaMillis = reconciler.activationRunEwmaMillis < 0
                ? runMillis
                : 0.8 * reconciler.activationRunEwmaMillis + 0.2 * runMillis;
            final long now = System.nanoTime();
            if (reconciler.activationRunEwmaMillis > target) {
                if (reconciler.activationLimitCutAtNanos == Long.MIN_VALUE
                    || now - reconciler.activationLimitCutAtNanos >= 1_000_000_000L) {
                    reconciler.activationLimit = Math.max(BackgroundReconciler.ACTIVATION_LIMIT_FLOOR, reconciler.activationLimit * 0.75);
                    reconciler.activationLimitCutAtNanos = now;
                }
            } else if (reconciler.activationRunEwmaMillis < target / 2.0) {
                reconciler.activationLimit = Math.min(
                    reconciler.activationConcurrency,
                    reconciler.activationLimit + 1.0 / reconciler.activationLimit
                );
            }
        }
        drainActivations();
    }

    /** One shard's activation, run by exactly one caller at a time. */
    Optional<ShardId> activateNow(String indexName, int shard, boolean onDemand) throws Exception {
        try (org.opensearch.common.lease.Releasable work = reconciler.activity.enter()) {
            if (work == null) {
                return Optional.empty();
            }
            final ShardId open = reconciler.node.reconciler()
                .openShards()
                .stream()
                .filter(s -> s.getIndexName().equals(indexName) && s.id() == shard)
                .findFirst()
                .orElse(null);
            if (open != null && reconciler.node.reconciler().isReader(open) == false) {
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
                synchronized (reconciler.capLock) {
                    full = open == null
                        && reconciler.node.reconciler().heldShards().size() + reconciler.capReserved >= reconciler.maxShardsHeld;
                    if (full == false && open == null) {
                        reconciler.capReserved++;
                        reserved = true;
                    }
                }
                if (full && takeoverMayOvercommit(indexName, shard)) {
                    // A dead member's shard, and room under the takeover ceiling: taken without evicting. Evicting here put
                    // a publish and a head release in front of every shard a survivor near its cap took from a dead node,
                    // one at a time, and that -- not the open -- was most of how long a kill took to recover from. The trim
                    // in each pass brings the node back to its cap once the taken shards have been idle a while.
                    synchronized (reconciler.capLock) {
                        if (reconciler.node.reconciler().heldShards().size() + reconciler.capReserved < takeoverCeiling()) {
                            reconciler.capReserved++;
                            reserved = true;
                            full = false;
                        }
                    }
                    if (reserved) {
                        reconciler.node.noteTakeoverOvercommitted();
                    }
                }
                if (full && reconciler.leases.heldByAnotherLiveNode(indexName, shard)) {
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
                    reconciler.makeRoom();
                }
                synchronized (reconciler.capLock) {
                    if (reserved) {
                        full = false;
                    } else {
                        // makeRoom answers for what is held, not for what is reserved, so the reservations are counted
                        // again after it: room it made may already be spoken for.
                        full = open == null
                            && reconciler.node.reconciler().heldShards().size() + reconciler.capReserved >= reconciler.maxShardsHeld;
                    }
                    if (full && reconciler.leases.headNamesThisNode(indexName, shard) == false) {
                        // Refusing is a routing outcome, not an error. Saying so is the difference between a
                        // node that is full and a node that is broken, and only one of them should page anyone.
                        BackgroundReconciler.logger.info(
                            "not taking {}[{}] on demand: holding {} shards, the cap, and all of them are in use",
                            indexName,
                            shard,
                            reconciler.maxShardsHeld
                        );
                        reconciler.node.noteActivationRefusedAtCap(indexName, shard);
                        return Optional.empty();
                    }
                    if (open == null && reserved == false) {
                        reconciler.capReserved++;
                        reserved = true;
                    }
                }
            }
            try {
                if (open != null) {
                    // Held as a reader. That used to count as held, so a node that had searched a shard could
                    // never become its writer until idle release let the reader go. A reader holds no head
                    // and no unpublished write, so closing it costs nothing.
                    reconciler.node.reconciler().releaseShard(open, "reopening as a writer");
                }
                final Optional<ShardId> taken = reconciler.node.activateWriter(reconciler.plane, indexName, shard);
                // Its idle clock starts here, so a shard just taken is not immediately a candidate for being
                // given straight back -- whether it was wanted or taken on demand.
                taken.ifPresent(shardId -> reconciler.openedAt.put(shardId, reconciler.plane.clock().getAsLong()));
                // Opened from a commit that already holds everything it has: that commit is this node's last publish
                // too, so neither the next pass nor an idle release publishes the same operations again.
                taken.ifPresent(shardId -> reconciler.node.reconciler().takePublishedThroughAtOpen(shardId).ifPresent(through -> {
                    reconciler.lastPublishedMaxSeqNo.put(shardId, through);
                    // And spaced from now, as the publish it replaces would have been, so the first write
                    // after the take does not publish at once.
                    reconciler.lastPublishedAtMillis.put(shardId, reconciler.plane.clock().getAsLong());
                }));
                return taken;
            } finally {
                if (reserved) {
                    synchronized (reconciler.capLock) {
                        reconciler.capReserved--;
                    }
                }
            }
        }
    }
}
