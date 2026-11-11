/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

/**
 * Decides for {@link ServerlessNode} whether a write may go ahead: the shard's fence, its incarnation, and this
 * node's lease and head.
 */
public final class WriteFences {

    private final ServerlessNode node;

    WriteFences(ServerlessNode node) {
        this.node = node;
    }

    /**
     * How long a read confirming that an index incarnation still exists licenses answering from its shards.
     *
     * <p><b>This is what makes routing from a cached descriptor safe.</b> A coordinator routes by a
     * resolution up to {@code MetadataPlane#DEFAULT_ROUTING_CACHE_MILLIS} old, and a stale one carries the
     * uuid of an incarnation another node has deleted. The uuid check where the request lands cannot catch
     * that when the landing node still holds that incarnation's shard open -- the uuids match, because both
     * are the old one -- and a write was acknowledged into a deleted index. So the landing node answers only
     * if it has read the descriptor and found that very uuid within this window, by its own monotonic clock,
     * and a delete answers only after waiting longer than the window ({@link #DELETE_SETTLE_MILLIS}). A write
     * acknowledged here was therefore acknowledged before any delete of its index returned: concurrent with
     * the delete, never after it.
     *
     * <p>The read is per index and per window, not per request: a shard written a thousand times a second
     * pays one descriptor read a second, and an idle one pays nothing.
     */
    public static final long INCARNATION_FENCE_MILLIS = 1_000L;

    /**
     * How long a delete waits before answering: the fence, plus a margin for two clocks' rates.
     *
     * <p>Durations, not timestamps, are compared on each side, so skew between the nodes does not enter;
     * only drift in how fast their clocks run does, and a quarter of the window is far beyond that.
     */
    public static final long DELETE_SETTLE_MILLIS = INCARNATION_FENCE_MILLIS + 250L;

    volatile long incarnationFenceMillis = INCARNATION_FENCE_MILLIS;

    /** When each incarnation was last confirmed to exist, by uuid, in {@link System#nanoTime} terms. */
    final java.util.concurrent.ConcurrentHashMap<String, Long> incarnationConfirmedAtNanos = new java.util.concurrent.ConcurrentHashMap<>();

    /** Striped, so a burst of writes to one index pays one confirming read rather than one each. */
    final Object[] incarnationLocks = newLocks(64);

    private static Object[] newLocks(int n) {
        final Object[] locks = new Object[n];
        for (int i = 0; i < n; i++) {
            locks[i] = new Object();
        }
        return locks;
    }

    /**
     * Sets the incarnation fence window; zero confirms on every answer. For tests that need the check to
     * run rather than be answered from a recent confirmation.
     *
     * @param millis the window
     */
    public void setIncarnationFenceMillis(long millis) {
        this.incarnationFenceMillis = Math.max(0L, millis);
    }

    /** Records that an incarnation was read and found live at {@code startedAtNanos}, for the incarnation fence. */
    void confirmIncarnation(String uuid, long startedAtNanos) {
        incarnationConfirmedAtNanos.merge(uuid, startedAtNanos, Math::max);
    }

    /**
     * Refuses to answer from a shard unless its index incarnation was confirmed to exist within the window.
     *
     * @param shardId the shard about to answer
     * @throws StaleIncarnationException if the index is gone or has been recreated under another uuid
     * @throws java.io.IOException if the descriptor cannot be read
     */
    public void ensureIncarnationLive(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        final org.opensearch.serverless.metadata.MetadataPlane plane = node.metadataPlane;
        if (plane == null) {
            // No metadata plane, nothing that can delete an index behind this node's back.
            return;
        }
        final String uuid = shardId.getIndex().getUUID();
        final long window = java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(incarnationFenceMillis);
        if (confirmedWithin(uuid, window)) {
            return;
        }
        // A routing read this node made moments ago -- resolving a search's names, say -- found this very
        // incarnation, and a read is a read whoever made it: stamped with when it began, it confirms exactly
        // what the fence's own read would. A wide search used to read every descriptor twice, once to route
        // and once here.
        final var routed = plane.readConfirmingIncarnation(shardId.getIndexName(), uuid);
        if (routed.isPresent() && System.nanoTime() - routed.get().readStartedNanos() <= window) {
            confirmIncarnation(uuid, routed.get().readStartedNanos());
            // And it is a read of the mapping and settings like any other: a change made on another node reaches
            // this one's shards through whichever read sees it first, so the periodic refresh is only a backstop.
            node.descriptorRefresh.applyIfNewer(shardId.getIndexName(), routed.get().descriptor(), routed.get().readStartedNanos());
            return;
        }
        synchronized (incarnationLocks[Math.floorMod(uuid.hashCode(), incarnationLocks.length)]) {
            if (confirmedWithin(uuid, window)) {
                return;
            }
            // Stamped with when the read began, never when it returned: the descriptor was live at some
            // instant after this, so this is the earliest the confirmation can honestly claim.
            final long startedAt = System.nanoTime();
            final var descriptor = plane.describe(shardId.getIndexName());
            if (descriptor.isEmpty() || descriptor.get().uuid().equals(uuid) == false) {
                incarnationConfirmedAtNanos.remove(uuid);
                throw new StaleIncarnationException(shardId);
            }
            incarnationConfirmedAtNanos.put(uuid, startedAt);
            // The read is the refresh's read too: apply it, and let the periodic refresh skip this index.
            try {
                node.descriptorRefresh.applyDescriptor(shardId.getIndexName(), descriptor.get());
                node.descriptorReadAtNanos.put(shardId.getIndexName(), startedAt);
            } catch (Exception e) {
                ServerlessNode.logger.warn(
                    "could not apply the descriptor of " + shardId.getIndexName() + " read by the incarnation fence",
                    e
                );
            }
            if (incarnationConfirmedAtNanos.size() > 4096) {
                // A confirmation older than the window licenses nothing, so those are all that is dropped.
                final long now = System.nanoTime();
                incarnationConfirmedAtNanos.values().removeIf(at -> now - at > window);
            }
        }
    }

    boolean confirmedWithin(String uuid, long windowNanos) {
        final Long at = incarnationConfirmedAtNanos.get(uuid);
        return at != null && System.nanoTime() - at <= windowNanos;
    }

    /**
     * Refuses a write to a shard whose log could not be appended to, before the engine applies it.
     *
     * @param shardId the shard
     * @throws java.io.IOException if the shard is write-fenced
     */
    void refuseIfWriteFenced(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        if (node.leases().ownershipUnverified) {
            throw new java.io.IOException(
                "refusing to write to "
                    + shardId
                    + ": this node's lease lapsed and it has not yet re-read the shard-heads it holds; retry shortly"
            );
        }
        if (node.writeFenced.contains(shardId)) {
            throw new java.io.IOException(
                "refusing to write to "
                    + shardId
                    + ": its write-ahead log could not be appended to and the shard is being reopened from the log; reads are still served"
            );
        }
    }

    /**
     * Reports whether a writer shard is refusing writes after a failed log append.
     *
     * @param shardId the shard
     * @return true while the shard is fenced and awaiting a reopen
     */
    public boolean isWriteFenced(org.opensearch.core.index.shard.ShardId shardId) {
        return node.writeFenced.contains(shardId);
    }

    /**
     * Refuses the write if this node's own lease has already expired by its own clock.
     *
     * <p><b>Why the write path and not just the heartbeat.</b> A node learns it has lost a shard by reading
     * the shard-head, which it does on its heartbeat. A node that cannot reach the object store cannot read
     * the head <em>or</em> renew its lease, so today it keeps accepting writes for as long as the partition
     * lasts — the one case where the existing check is guaranteed not to fire is the one where it is needed.
     * Checking the lease this node last published costs nothing, needs no I/O, and is therefore affordable
     * on every write.
     *
     * <p><b>It bounds; it does not fence.</b> The check and the append are not atomic, so a pause between
     * them long enough to outlive the deadline still lands the append. That is the classic lease race and
     * it cannot be closed from the writer's side. It is closed on the reader's side instead, by the replay
     * cutoff in {@link org.opensearch.serverless.store.WalStore#position()}: a successor does not read what
     * was appended after it took over, whether the appender believed itself entitled or not.
     *
     * <p>The shard is released rather than merely refused, because a node past its own deadline has lost
     * every shard it held, not just this one's write — and a released shard is one a successor can take
     * cleanly instead of racing.
     *
     * @param shardId the shard being written to
     * @throws java.io.IOException if this node may no longer write
     */
    void ensureOwnLeaseIsStillValid(org.opensearch.core.index.shard.ShardId shardId) throws java.io.IOException {
        if (releaseIfLeaseLapsed(shardId)) {
            throw new java.io.IOException(
                "refusing to write to "
                    + shardId
                    + ": this node's lease has expired, so it can no longer claim to hold the shard; "
                    + "the shard has been released and the write was not acknowledged"
            );
        }
    }

    /**
     * Releases the shard if this node's own lease has expired by its own clock, without refusing anything.
     *
     * @param shardId the shard
     * @return true if the lease had lapsed and the shard was released
     */
    boolean releaseIfLeaseLapsed(org.opensearch.core.index.shard.ShardId shardId) {
        final var plane = node.metadataPlane;
        if (plane == null) {
            return false;
        }
        final long now = plane.clock().getAsLong();
        if (plane.membership().selfLeaseValidAt(now) && node.leases().ownershipUnverified == false) {
            return false;
        }
        // Remembered so the next renewal gives the head back in the register: a closed shard whose head
        // still names this node, once the lease is renewed, is a shard nobody serves and nobody can take.
        final var shard = node.reconciler.shard(shardId);
        if (shard != null) {
            node.leases().headsToGiveBack.put(shardId, shard.getOperationPrimaryTerm());
        }
        node.reconciler.releaseShard(shardId, "this node's own lease expired at or before " + now);
        node.writeFenced.remove(shardId);
        node.writerAssignments.remove(ServerlessNode.shardKey(shardId.getIndexName(), shardId.id()));
        return true;
    }

    /**
     * Runs an action that gives a shard away while holding the shard's write fence.
     *
     * <p>Waits for every write in flight on the shard -- applied to the engine, appending to the log,
     * about to be acknowledged -- and keeps new ones out until the action returns. A write that arrives
     * during the action then finds whatever the action left: no shard, and it is refused. This is what
     * makes "publish, release the head, close" safe against the write path; see {@link NodeWritePath#appendOrRelease}.
     *
     * <p><b>Two locks, two jobs, one order.</b> This fence guards the shard being <em>given away</em>;
     * the reconciler's write guard ({@code ShardReconciler#writeGuard}) guards a <em>commit</em> landing
     * between an apply and its append. The write path holds this fence's read side and then the guard's
     * read side; a release holds this fence's write side and, through the publish inside it, the guard's
     * write side around the flush; an ordinary publish holds only the guard's. Nothing that holds the
     * guard ever waits on this fence, so the two cannot deadlock, and a publish still overlaps with
     * writes everywhere but the flush.
     *
     * @param shardId the shard
     * @param action what to do while nothing can be acknowledged against the shard
     * @param <T> the action's result
     * @return the action's result
     * @throws Exception whatever the action throws
     */
    public <T> T underShardFence(org.opensearch.core.index.shard.ShardId shardId, java.util.concurrent.Callable<T> action)
        throws Exception {
        final java.util.concurrent.locks.ReentrantReadWriteLock fence = node.shardFences.computeIfAbsent(
            shardId,
            k -> new java.util.concurrent.locks.ReentrantReadWriteLock()
        );
        fence.writeLock().lock();
        try {
            return action.call();
        } finally {
            fence.writeLock().unlock();
            if (node.reconciler == null || node.reconciler.shard(shardId) == null) {
                // Closed: the fence goes with it, or a node that churns through shards keeps a lock per
                // shard it ever held. A writer that fetched this fence and locks it after this point
                // notices the swap in enterWritePath and starts over on the current one.
                node.shardFences.remove(shardId, fence);
                // And the group committer's queue for it, for the same reason and with the same safety: a
                // flush already in flight holds its own reference and finishes, and its members are judged
                // by the shard-identity check every caller makes after the append.
                node.walCommitter.forget(shardId);
                final NodeLogWriter log = node.nodeLogWriter;
                if (log != null) {
                    log.forget(shardId);
                }
            }
        }
    }

    /** Takes the read side of a shard's fence; the caller unlocks what is returned. */
    java.util.concurrent.locks.Lock enterWritePath(org.opensearch.core.index.shard.ShardId shardId) {
        while (true) {
            final java.util.concurrent.locks.ReentrantReadWriteLock fence = node.shardFences.computeIfAbsent(
                shardId,
                k -> new java.util.concurrent.locks.ReentrantReadWriteLock()
            );
            fence.readLock().lock();
            if (node.shardFences.get(shardId) == fence) {
                return fence.readLock();
            }
            // The shard was closed, and possibly reopened, between fetching the fence and locking it; the
            // fence in the map is the one the next release will take, so that is the one to hold.
            fence.readLock().unlock();
        }
    }

    /**
     * Reports whether this node holds a shard as its writer, by evidence no older than the given age.
     *
     * <p>For the node a write was forwarded to. Being open here was the only check, and "open here" is
     * true of a shard whose head has named another node since the last heartbeat: the lease fences a
     * write only once the lapsed-lease rule in {@link NodeLeases#renewLease} holds, and the head is the truth the
     * rule approximates. The heartbeat notes every held head once per renewal, so with an age of one
     * renewal this costs no register read on a healthy node; a head older than that is read fresh, and
     * one that names another node closes the shard here on the spot.
     *
     * <p>A head that cannot be read is no evidence, and the answer is then no: a write needs the store
     * for its log append anyway, so refusing costs the caller one retry it was going to need.
     *
     * @param shardId the shard
     * @param maxHeadAgeMillis how old a noted head may be before it is re-read
     * @return true if the shard may accept a write on this node right now
     */
    public boolean holdsAsWriter(org.opensearch.core.index.shard.ShardId shardId, long maxHeadAgeMillis) {
        node.ensureStarted();
        if (node.reconciler.shard(shardId) == null || node.reconciler.isReader(shardId) || node.writeFenced.contains(shardId)) {
            return false;
        }
        if (node.leases().ownershipUnverified) {
            // The lease lapsed and the heads have not been re-read since the renewal; see renewLease.
            return false;
        }
        final org.opensearch.serverless.metadata.MetadataPlane plane = node.metadataPlane;
        if (plane == null) {
            return true;
        }
        if (plane.membership().selfLeaseValidAt(plane.clock().getAsLong()) == false) {
            return false;
        }
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head = node.sightings()
            .recentHead(shardId.getIndexName(), shardId.id(), maxHeadAgeMillis);
        if (head.isEmpty()) {
            try {
                head = plane.heads().read(shardId.getIndexName(), shardId.id());
            } catch (java.io.IOException e) {
                ServerlessNode.logger.debug("could not read the head of {} to answer a forwarded write; refusing it", shardId);
                return false;
            }
            node.sightings().noteHead(shardId.getIndexName(), shardId.id(), head.orElse(null));
        }
        if (node.leases.namesThisNode(head, shardId)) {
            return true;
        }
        try {
            node.leases.releaseLost(
                plane,
                shardId,
                "a forwarded write found the shard-head naming " + head.map(h -> h.ownerNodeId()).orElse("nobody"),
                true
            );
        } catch (Exception e) {
            ServerlessNode.logger.warn("could not release " + shardId + " after its head stopped naming this node", e);
        }
        node.signals.ownershipDoubted(shardId.getIndexName(), shardId.id());
        return false;
    }
}
