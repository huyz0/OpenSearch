/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.write.ShardBehindLogException;
import org.opensearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * The check that keeps a reader from serving a commit behind an acknowledged write while nobody is writing its shard,
 * and the memo of when each reader last passed it.
 *
 * <p>See {@link #validateReader} for what is checked and when.
 */
public final class ReaderValidation {

    private final Supplier<String> localNodeId;
    private final Supplier<ShardReconciler> reconciler;
    private final Supplier<ReconcileSignals> signals;
    private final ThreadPool threadPool;
    private final Consumer<ShardId> forgetReader;

    /**
     * Creates the reader check of one node.
     *
     * @param localNodeId this node's id, read on every call because the node rebinds it at start
     * @param reconciler the node's shard reconciler, installed at start
     * @param signals who hears that a shard's ownership is in doubt
     * @param threadPool the node's thread pool; log listings run on its generic pool
     * @param forgetReader called when a superseded reader has been let go, to drop the node's record of it
     */
    ReaderValidation(
        Supplier<String> localNodeId,
        Supplier<ShardReconciler> reconciler,
        Supplier<ReconcileSignals> signals,
        ThreadPool threadPool,
        Consumer<ShardId> forgetReader
    ) {
        this.localNodeId = localNodeId;
        this.reconciler = reconciler;
        this.signals = signals;
        this.threadPool = threadPool;
        this.forgetReader = forgetReader;
    }

    /**
     * When nothing has happened since a reader was checked that could put its commit behind an acknowledged write.
     *
     * @param commit the commit the check was made against
     * @param departures the {@link #departures} count at the check
     * @param owner the live owner the check found, or null if nobody was writing the shard
     * @param ownerEphemeralId the process of the live owner the check found, or null
     * @param ownerExpiresAtMillis the expiry of that owner's lease as the membership snapshot held it, or
     *     {@code Long.MIN_VALUE} if the snapshot did not hold it
     * @param headTerm the head's term at the check
     * @param checkedAtNanos when the head was last read for it
     */
    private record ReaderCheck(CommitManifest commit, long departures, String owner, String ownerEphemeralId, long ownerExpiresAtMillis,
        long headTerm, long checkedAtNanos) {
        ReaderCheck reread(long nanos, long ownerExpiresAt) {
            return new ReaderCheck(commit, departures, owner, ownerEphemeralId, ownerExpiresAt, headTerm, nanos);
        }
    }

    /**
     * How long a reader's check stands before the shard's head is read again, by default.
     *
     * <p>A check that found nobody writing the shard stood until a node departed, with no read at all: a writer that
     * took the shard afterwards, acknowledged a write and gave the head back unpublished -- a lease lapse under
     * throttling -- left the reader serving its commit as current, one document short and no failure reported. Every
     * acquisition moves the head's term, so a head read that finds the same term is proof nothing was written behind
     * the commit; it is made at most once a second per shard, the bound the incarnation fence already holds reads to,
     * and the log is listed only when the term has moved.
     */
    static final long READER_CHECK_MILLIS = 1_000L;

    private volatile long readerCheckMillis = READER_CHECK_MILLIS;

    /**
     * Sets how long a reader's check stands before its head is read again; zero reads it on every check. For tests.
     *
     * @param millis the window
     */
    void setReaderCheckMillis(long millis) {
        this.readerCheckMillis = Math.max(0L, millis);
    }

    /**
    * The default of {@code serverless.search.reader_check.live_owner_millis}: one lease renewal interval at the default TTL.
    *
    * <p>A check that found the shard's writer live re-read the head every second: one read per searched shard per
    * second, the largest store cost a searched index had. While that writer stays live a head read cannot refuse the
    * reader anyway: a newer live owner is recorded and the reader goes on serving, lagging it by the publish window by
    * design. The read matters once nobody live writes the shard, and while the writer the check found holds its lease
    * without a gap nobody can have taken the shard from it: a successor takes a shard only after revoking a lease it
    * judged run out, and a writer that lets go publishes first and pushes the commit to the readers.
    *
    * <p>So the check stands, without a read, while the membership snapshot shows the same process holding its lease
    * and proves it held it throughout: the copy held now is the one the check saw, or a renewal stamped before that
    * copy ran out (a renewal stamps its time plus the TTL). Any gap -- the snapshot's copy run out, a restart, a lease
    * that went -- brings the full check back, as does the window passing. Zero, or a value below the reader check
    * window, keeps one window for both.
    */
    static final long LIVE_OWNER_CHECK_MILLIS = 10_000L;

    private volatile long liveOwnerCheckMillis = LIVE_OWNER_CHECK_MILLIS;

    /**
     * Sets how long a check that found a live writer stands while that writer stays live; see {@link #LIVE_OWNER_CHECK_MILLIS}.
     *
     * @param millis the window; zero to use the reader check window for both
     */
    void setLiveOwnerCheckMillis(long millis) {
        this.liveOwnerCheckMillis = Math.max(0L, millis);
    }

    /** The lease the membership snapshot holds for a node, or null; reads nothing. */
    private static org.opensearch.serverless.membership.NodeLease snapshotLease(MetadataPlane plane, String nodeId) {
        for (org.opensearch.serverless.membership.NodeLease lease : plane.membership().current()) {
            if (nodeId.equals(lease.nodeId())) {
                return lease;
            }
        }
        return null;
    }

    /**
     * Whether the owner a check found has held its lease without a gap since, as far as the membership snapshot can
     * prove it: the same process, live, and either the same copy or one renewed before the checked copy ran out.
     */
    private static boolean ownerHeldThroughout(MetadataPlane plane, ReaderCheck last) {
        if (last.ownerExpiresAtMillis() == Long.MIN_VALUE || last.ownerEphemeralId() == null) {
            return false;
        }
        final org.opensearch.serverless.membership.NodeLease lease = snapshotLease(plane, last.owner());
        return lease != null
            && lease.isExpiredAt(plane.clock().getAsLong()) == false
            && last.ownerEphemeralId().equals(lease.ephemeralId())
            && lease.expiresAtMillis() - plane.leaseTtlMillis() <= last.ownerExpiresAtMillis();
    }

    /** The last check of each reader, by index and shard number. */
    private final Map<String, ReaderCheck> readerChecks = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * How many times a lease has been seen to leave the membership. A reader whose shard's writer may have died
     * since it was checked is checked again, and nothing short of a departure can make that so: a writer that is
     * still live publishes what it writes, and a reader that falls behind a live writer is the publish window,
     * not a lie.
     */
    private final AtomicLong departures = new AtomicLong();

    private final AtomicBoolean watchingDepartures = new AtomicBoolean();

    /**
     * Refuses to let a reader serve a commit behind an acknowledged write while nobody is writing the shard.
     *
     * <p><b>What can make a commit fall behind.</b> Every write is logged before it is acknowledged, and its
     * writer publishes it later. A writer that lets go of the shard publishes first. One that dies, or lapses past
     * its lease and gives the shard back, does not -- and until another node takes the shard and replays the log,
     * the published commit is all a reader has. A search or a get answered from it then misses acknowledged
     * documents and says nothing.
     *
     * <p><b>What is checked, and when.</b> Each publish records how far into the log its commit reaches. A reader
     * of a shard with no live writer lists the log for a record past that point; one found means the commit is
     * behind, and the reader refuses -- {@link ShardBehindLogException}, retryable -- and doubts the shard's
     * ownership, which is what makes a node take it, replay the log and publish. A shard with a live writer is
     * not checked: its readers lag it by the publish window, which the search path accepts by design. The check
     * runs when a reader is first served and again whenever a lease has left the membership since, so a cached
     * reader costs nothing while nobody dies.
     */
    void validateReader(MetadataPlane plane, String indexName, int shardNumber, ShardId shardId) throws java.io.IOException {
        validateReader(plane, indexName, shardNumber, shardId, null);
    }

    /**
     * As {@link #validateReader(MetadataPlane, String, int, ShardId)}, for the check that follows an open: the head the
     * open read beside its descriptor stands in for the head read here, and the register it read after that head for the
     * register read here, when the reader is serving that very commit. Both were read in the order this check reads them.
     *
     * @param ahead what the open read, or null to read both here
     */
    void validateReader(MetadataPlane plane, String indexName, int shardNumber, ShardId shardId, ReadAhead ahead)
        throws java.io.IOException {
        watchDepartures(plane);
        final String key = NodeContext.shardKey(indexName, shardNumber);
        final CommitManifest commit = reconciler.get().readerCommit(shardId).orElse(null);
        if (commit == null) {
            // Not a reader -- the shard is open here as this node's writer, which is current by definition.
            return;
        }
        final ReaderCheck last = readerChecks.get(key);
        final long departed = departures.get();
        final long nowAtCheck = System.nanoTime();
        final boolean sameCheck = last != null && last.commit() == commit && last.departures() == departed;
        final long liveOwnerWindow = readerCheckMillis == 0L ? 0L : Math.max(readerCheckMillis, liveOwnerCheckMillis);
        if (last != null
            && last.owner() != null
            && (last.commit() == commit || (commit.term() == last.headTerm() && last.owner().equals(commit.writer())))
            && nowAtCheck - last.checkedAtNanos() < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(liveOwnerWindow)
            && ownerHeldThroughout(plane, last)) {
            // The writer the last check found has held its lease throughout: see LIVE_OWNER_CHECK_MILLIS. Not tied to the
            // departure count, which moves whenever the snapshot re-reads any peer's lease. A newer commit that same writer
            // published in the same term -- the one its push moved the reader onto -- leaves the check standing: a check
            // that finds a live writer verifies nothing about the commit itself.
            return;
        }
        if (sameCheck
            && nowAtCheck - last.checkedAtNanos() < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(readerCheckMillis)
            && (last.owner() == null || liveLease(plane, last.owner()))) {
            return;
        }
        final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> readAhead = last == null && ahead != null
            ? ahead.headIfRead()
            : null;
        final long now = readAhead != null ? ahead.headReadAtNanos() : nowAtCheck;
        final var head = readAhead != null ? readAhead : plane.heads().read(indexName, shardNumber);
        final String owner = head.map(org.opensearch.serverless.metadata.ShardHead::ownerNodeId).orElse(null);
        final long headTerm = head.map(org.opensearch.serverless.metadata.ShardHead::term).orElse(0L);
        if (sameCheck && last.headTerm() == headTerm && java.util.Objects.equals(last.owner(), owner)) {
            // Nobody has acquired the shard since the last check -- every acquisition moves the term -- so nothing can
            // have been written behind the commit that the check did not already account for.
            if (owner == null || liveLease(plane, owner)) {
                final org.opensearch.serverless.membership.NodeLease seen = owner == null ? null : snapshotLease(plane, owner);
                readerChecks.put(key, last.reread(now, seen == null ? Long.MIN_VALUE : seen.expiresAtMillis()));
                return;
            }
        }
        if (owner != null
            && owner.equals(localNodeId.get()) == false
            && liveLease(plane, owner)
            && head.get().ownerEphemeralId() != null
            && ownerIncarnationLive(plane, owner, head.get().ownerEphemeralId())) {
            final org.opensearch.serverless.membership.NodeLease seen = snapshotLease(plane, owner);
            readerChecks.put(
                key,
                new ReaderCheck(
                    commit,
                    departed,
                    owner,
                    head.get().ownerEphemeralId(),
                    seen == null ? Long.MIN_VALUE : seen.expiresAtMillis(),
                    headTerm,
                    now
                )
            );
            return;
        }
        // Nobody is writing it -- no owner, a dead one, or this node named with nothing open. The commit has to be the
        // published one first: a writer that replayed the log and published replaced it, and let go of the log behind
        // its own commit, so the log holds nothing past this one and it would be served one write short. A fleet
        // run found exactly that, answered complete.
        final CommitManifest openedRegister = readAhead == null ? null : ahead.register();
        final var published = openedRegister != null && sameCommit(commit, openedRegister)
            ? java.util.Optional.of(openedRegister)
            : plane.segmentPublisher(indexName, shardId.getIndex().getUUID(), shardNumber).readManifest();
        // A commit its writer staged after the register is not superseded by it: the writer is gone and had published it.
        if (published.isPresent() && sameCommit(commit, published.get()) == false && commit.stagedAfter(published.get()) == false) {
            readerChecks.remove(key);
            // Let go now if no query holds it, so the retry opens the current commit; one that does is let go by the
            // reconciler's next refresh, and refuses until then.
            if (reconciler.get()
                .releaseReader(
                    shardId,
                    commit,
                    "the commit it was serving has been superseded"
                ) == org.opensearch.serverless.shard.ShardReconciler.ReaderRelease.RELEASED) {
                forgetReader.accept(shardId);
            }
            throw ShardBehindLogException.superseded(indexName, shardNumber);
        }
        boolean behind = false;
        if (head.isPresent()) {
            // Then it has to cover the node logs: the writers the head names, and a dead owner, hold acknowledged records
            // until a successor replays and publishes them.
            final java.util.List<String> nodeLogs = new java.util.ArrayList<>(head.get().writers());
            if (owner != null && nodeLogs.contains(owner) == false) {
                nodeLogs.add(owner);
            }
            behind = plane.nodeLogHolds(
                nodeLogs,
                indexName,
                shardId.getIndex().getUUID(),
                shardNumber,
                headTerm,
                java.util.Optional.of(commit)
            );
        }
        if (behind) {
            readerChecks.remove(key);
            signals.get().ownershipDoubted(indexName, shardNumber);
            throw new ShardBehindLogException(indexName, shardNumber);
        }
        readerChecks.put(key, new ReaderCheck(commit, departed, null, null, Long.MIN_VALUE, headTerm, now));
    }

    /**
     * The live writer the last check of a reader found.
     *
     * @param indexName the index
     * @param shardNumber the shard
     * @return the writer's node id, or null if the check found nobody writing the shard or there was none
     */
    String checkedOwner(String indexName, int shardNumber) {
        final ReaderCheck last = readerChecks.get(NodeContext.shardKey(indexName, shardNumber));
        return last == null ? null : last.owner();
    }

    /** Counts departures from the membership this node consults, once per plane. */
    private void watchDepartures(MetadataPlane plane) {
        if (watchingDepartures.compareAndSet(false, true)) {
            plane.membership().subscribe(delta -> {
                if (delta.left().isEmpty() == false) {
                    departures.incrementAndGet();
                }
            });
        }
    }

    /** Whether two manifests name the same commit: the same term and the same files. */
    private static boolean sameCommit(CommitManifest a, CommitManifest b) {
        return a.term() == b.term() && a.files().equals(b.files());
    }

    /** Whether a node holds a live lease: the membership snapshot first, the register only when it says no. */
    private static boolean liveLease(MetadataPlane plane, String nodeId) {
        final long now = plane.clock().getAsLong();
        for (org.opensearch.serverless.membership.NodeLease lease : plane.membership().current()) {
            if (nodeId.equals(lease.nodeId()) && lease.isExpiredAt(now) == false) {
                return true;
            }
        }
        try {
            return plane.membership().read(nodeId).map(lease -> lease.isExpiredAt(now) == false).orElse(false);
        } catch (java.io.IOException e) {
            // Unknown is not live: the check that follows is the safe side.
            return false;
        }
    }

    /** Whether the live lease of a node belongs to the process the head names, not a restart of it. */
    private static boolean ownerIncarnationLive(MetadataPlane plane, String nodeId, String ephemeralId) {
        for (org.opensearch.serverless.membership.NodeLease lease : plane.membership().current()) {
            if (nodeId.equals(lease.nodeId())) {
                return lease.ephemeralId() == null || ephemeralId.equals(lease.ephemeralId());
            }
        }
        try {
            return plane.membership()
                .read(nodeId)
                .map(lease -> lease.ephemeralId() == null || ephemeralId.equals(lease.ephemeralId()))
                .orElse(false);
        } catch (java.io.IOException e) {
            return false;
        }
    }

    /**
     * The shard head a reader open reads alongside its descriptor, and the register it reads after that head.
     *
     * <p>A reader's first check needs the head, then a register read made after it: a commit published between a register
     * read and a later head read could be one the log behind it was trimmed past. The open used to read the descriptor, then
     * the register, and the check then read the head and the register again: four reads, one after another. The head needs no
     * descriptor, so it is read beside it, and the register the open reads once the head is in is the one the check needs.
     * Two reads in two round trips, and the head also says whether anyone writes the shard, which decides whether the open
     * asks a writer for its acknowledged tail at all.
     *
     * <p>The head is read on another pool. If that pool has not started it when the open needs it, the open reads it itself:
     * an open never waits on a queue, so a saturated pool cannot stall one.
     */
    static final class ReadAhead {

        private final MetadataPlane plane;
        private final String indexName;
        private final int shardNumber;
        private final AtomicBoolean claimed = new AtomicBoolean();
        private final java.util.concurrent.CompletableFuture<java.util.Optional<org.opensearch.serverless.metadata.ShardHead>> head =
            new java.util.concurrent.CompletableFuture<>();
        private volatile long headReadAtNanos;
        private volatile CommitManifest register;

        private ReadAhead(MetadataPlane plane, String indexName, int shardNumber) {
            this.plane = plane;
            this.indexName = indexName;
            this.shardNumber = shardNumber;
        }

        /**
         * Starts reading a shard's head.
         *
         * @param plane the metadata plane
         * @param indexName the index
         * @param shardNumber the shard
         * @param executor where the read runs; null, or a refusal, leaves it to the open
         * @return the read under way
         */
        static ReadAhead start(MetadataPlane plane, String indexName, int shardNumber, java.util.concurrent.Executor executor) {
            final ReadAhead ahead = new ReadAhead(plane, indexName, shardNumber);
            if (executor != null) {
                try {
                    executor.execute(ahead::readIfUnclaimed);
                } catch (RuntimeException refused) {
                    // Read by the open instead.
                }
            }
            return ahead;
        }

        private void readIfUnclaimed() {
            if (claimed.compareAndSet(false, true) == false) {
                return;
            }
            final long startedAt = System.nanoTime();
            try {
                final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> read = plane.heads().read(indexName, shardNumber);
                headReadAtNanos = startedAt;
                head.complete(read);
            } catch (Exception e) {
                head.completeExceptionally(e);
            }
        }

        /**
         * Returns the head, reading it here if nobody has started to.
         *
         * @return the head, or empty if the shard has none
         * @throws java.io.IOException if the read failed
         */
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> head() throws java.io.IOException {
            readIfUnclaimed();
            try {
                return head.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException("interrupted waiting for the head of " + indexName + "[" + shardNumber + "]", e);
            } catch (java.util.concurrent.ExecutionException e) {
                throw e.getCause() instanceof java.io.IOException io ? io : new java.io.IOException(e.getCause());
            }
        }

        /** @return the head if its read has finished and succeeded, without waiting; null otherwise */
        java.util.Optional<org.opensearch.serverless.metadata.ShardHead> headIfRead() {
            return head.isDone() && head.isCompletedExceptionally() == false ? head.join() : null;
        }

        /** @return when the head read was sent, on the nano clock */
        long headReadAtNanos() {
            return headReadAtNanos;
        }

        /**
         * Records the register the open read after {@link #head()} returned, and opened.
         *
         * @param manifest the register's commit
         */
        void openedFromRegister(CommitManifest manifest) {
            this.register = manifest;
        }

        /** @return the register read after the head that the open opened, or null if it opened something else */
        CommitManifest register() {
            return register;
        }

        /**
         * Whether the head says nobody writes the shard: no head, or one given back.
         *
         * @return true if the head was read and names no owner
         */
        boolean unowned() {
            final java.util.Optional<org.opensearch.serverless.metadata.ShardHead> read = headIfRead();
            return read != null && (read.isEmpty() || read.get().ownerNodeId() == null);
        }
    }
}
