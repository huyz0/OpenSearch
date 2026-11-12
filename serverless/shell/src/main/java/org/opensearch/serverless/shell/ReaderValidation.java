/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.ReconcileSignals;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.serverless.store.CommitManifest;
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
final class ReaderValidation {

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
     * @param headTerm the head's term at the check
     * @param checkedAtNanos when the head was last read for it
     */
    private record ReaderCheck(CommitManifest commit, long departures, String owner, long headTerm, long checkedAtNanos) {
        ReaderCheck reread(long nanos) {
            return new ReaderCheck(commit, departures, owner, headTerm, nanos);
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
        watchDepartures(plane);
        final String key = ServerlessNode.shardKey(indexName, shardNumber);
        final CommitManifest commit = reconciler.get().readerCommit(shardId).orElse(null);
        if (commit == null) {
            // Not a reader -- the shard is open here as this node's writer, which is current by definition.
            return;
        }
        final ReaderCheck last = readerChecks.get(key);
        final long departed = departures.get();
        final long now = System.nanoTime();
        final boolean sameCheck = last != null && last.commit() == commit && last.departures() == departed;
        if (sameCheck
            && now - last.checkedAtNanos() < java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(readerCheckMillis)
            && (last.owner() == null || liveLease(plane, last.owner()))) {
            return;
        }
        final var head = plane.heads().read(indexName, shardNumber);
        final String owner = head.map(org.opensearch.serverless.metadata.ShardHead::ownerNodeId).orElse(null);
        final long headTerm = head.map(org.opensearch.serverless.metadata.ShardHead::term).orElse(0L);
        if (sameCheck && last.headTerm() == headTerm && java.util.Objects.equals(last.owner(), owner)) {
            // Nobody has acquired the shard since the last check -- every acquisition moves the term -- so nothing can
            // have been written behind the commit that the check did not already account for.
            if (owner == null || liveLease(plane, owner)) {
                readerChecks.put(key, last.reread(now));
                return;
            }
        }
        if (owner != null
            && owner.equals(localNodeId.get()) == false
            && liveLease(plane, owner)
            && head.get().ownerEphemeralId() != null
            && ownerIncarnationLive(plane, owner, head.get().ownerEphemeralId())) {
            readerChecks.put(key, new ReaderCheck(commit, departed, owner, headTerm, now));
            return;
        }
        // Nobody is writing it -- no owner, a dead one, or this node named with nothing open. The commit has to be the
        // published one first: a writer that replayed the log and published replaced it, and trimmed the log behind
        // its own commit, so a listing finds nothing past this one and it would be served one write short. A fleet
        // run found exactly that, answered complete.
        final var log = plane.walStore(indexName, shardId.getIndex().getUUID(), shardNumber);
        // The log's listing does not depend on the manifest's answer, so it runs alongside the manifest read rather than
        // after it: both start after the head read, which is what the check needs of them -- a writer that published
        // after it had to take the head first. Only a commit recording how far it reaches is answered by a listing.
        final java.util.concurrent.CompletableFuture<Boolean> landed = commit.walOrdinal() >= 0L
            ? java.util.concurrent.CompletableFuture.supplyAsync(() -> {
                try {
                    return log.landedPast(commit.term(), commit.walOrdinal());
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }, threadPool.generic())
            : null;
        final var published = plane.segmentPublisher(indexName, shardId.getIndex().getUUID(), shardNumber).readManifest();
        // A commit its writer staged after the register is not superseded by it: the writer is gone and had published it.
        if (published.isPresent() && sameCommit(commit, published.get()) == false && commit.stagedAfter(published.get()) == false) {
            if (landed != null) {
                landed.cancel(false);
            }
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
        // Then it has to cover the log on its own. One that does not say how far it reaches covers none of its own
        // term.
        boolean behind;
        if (landed != null) {
            // How far the commit reaches is recorded: a listing answers.
            behind = awaitListing(landed);
        } else {
            // Published before that was recorded. Treating such a commit as covering none of its term made every
            // one ever written look behind -- the log keeps a term's highest record after a publish -- and a fleet
            // reopened on old manifests took over its whole population on the first searches. The records say.
            final org.opensearch.index.shard.IndexShard opened = reconciler.get().shard(shardId);
            final long committed = opened == null ? -1L : opened.seqNoStats().getMaxSeqNo();
            behind = log.maxSeqNoFromTerm(commit.term()) > committed;
        }
        if (behind == false && head.isPresent()) {
            // A write that went to a node's write log rather than the shard's own is in neither listing: the writers the
            // head names, and a dead owner, hold such records until a successor replays and publishes them.
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
        readerChecks.put(key, new ReaderCheck(commit, departed, null, headTerm, now));
    }

    /** The answer of a log listing run alongside the reader check, its failure rethrown as the check's. */
    private static boolean awaitListing(java.util.concurrent.CompletableFuture<Boolean> landed) throws java.io.IOException {
        try {
            return landed.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.IOException("interrupted listing the log for a reader check", e);
        } catch (java.util.concurrent.ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof java.io.UncheckedIOException unchecked) {
                throw unchecked.getCause();
            }
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new java.io.IOException("listing the log for a reader check failed", cause);
        }
    }

    /**
     * The live writer the last check of a reader found.
     *
     * @param indexName the index
     * @param shardNumber the shard
     * @return the writer's node id, or null if the check found nobody writing the shard or there was none
     */
    String checkedOwner(String indexName, int shardNumber) {
        final ReaderCheck last = readerChecks.get(ServerlessNode.shardKey(indexName, shardNumber));
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
}
