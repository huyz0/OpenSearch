/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Opens and closes shards on this node.
 *
 * <p><b>The invariant this class exists to hold</b>, established by measurement rather than by argument:
 *
 * <blockquote>Absence from a projected view is never a removal signal. A shard closes when the
 * shard-head says this node no longer owns it, and never because a locally-computed view failed to
 * mention it.</blockquote>
 *
 * <p>A two-node probe measured both halves of why that matters. Applying a view naming no indices at
 * all left a shard STARTED and still serving, so projection is harmless on its own. But the close-set a
 * <em>diff-based</em> reconciler would have computed was exactly the set of locally-open shards. The
 * hazard is entirely a property of the reconciler, which is this class.
 *
 * <p>So the invariant is structural, not merely tested. {@link #ensureOpen} contains no code path that
 * closes anything, and {@link #releaseShard} takes a shard id the caller must have obtained from truth.
 * There is deliberately no method that accepts a view and closes what is missing from it, which is the
 * shape {@code IndicesClusterStateService} uses and the reason it is replaced rather than reused.
 *
 * <p><b>The second invariant: local disk never outranks the published commit.</b> Every shard here —
 * writer, reader, frozen view — opens on a directory that reads published segments from the object store
 * and keeps only what this node itself writes on local disk. Local disk is a cache of one node's own
 * output, and it outlives the node's tenure of a shard: a writer fenced after a local flush leaves a
 * segment on disk that its successor, restoring the same commit, will name identically and fill with
 * other bytes. A directory that preferred the local copy served the fenced writer's documents under the
 * successor's manifest — a confidently wrong answer, not an error. So before any open, every local file
 * the manifest names is deleted, and every local commit with it; what remains local is either what the
 * engine is about to write or an unreferenced leftover Lucene removes on open.
 */
public final class ShardReconciler {

    static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ShardReconciler.class);

    final IndicesService indicesService;
    final DiscoveryNode localNode;
    final Map<ShardId, IndexShard> open = new ConcurrentHashMap<>();

    /** The steps of a writer's open, in {@link #writerOpenPhaseMillis} order. */
    public static final java.util.List<String> WRITER_OPEN_PHASES = java.util.List.of(
        "index_service",
        "manifest",
        "create_shard",
        "local_files",
        "fence",
        "recover",
        "start",
        // Not a step of its own: the part of recover spent reading the log.
        "log_read"
    );

    /** Time writers' opens spent in each step, summed, and how many opened. */
    final java.util.concurrent.atomic.AtomicLong[] writerOpenNanos = new java.util.concurrent.atomic.AtomicLong[WRITER_OPEN_PHASES.size()];
    final java.util.concurrent.atomic.AtomicLong writerOpens = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong logRecordsRead = new java.util.concurrent.atomic.AtomicLong();
    final java.util.concurrent.atomic.AtomicLong loggedCommitOpens = new java.util.concurrent.atomic.AtomicLong();

    /** Writers' opens that started from a commit a predecessor logged rather than from the register's. */
    public long loggedCommitOpens() {
        return loggedCommitOpens.get();
    }

    /** Log records writers' opens have read for replay, summed: with the opens, how much log an open finds. */
    public long logRecordsRead() {
        return logRecordsRead.get();
    }

    {
        for (int i = 0; i < writerOpenNanos.length; i++) {
            writerOpenNanos[i] = new java.util.concurrent.atomic.AtomicLong();
        }
    }

    /**
     * Where writers' opens spend their time, by step, in milliseconds summed since start, then the count of opens. The
     * open is most of a takeover; this says which part of it.
     */
    public long[] writerOpenPhaseMillis() {
        final long[] out = new long[writerOpenNanos.length + 1];
        for (int i = 0; i < writerOpenNanos.length; i++) {
            out[i] = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(writerOpenNanos[i].get());
        }
        out[writerOpenNanos.length] = writerOpens.get();
        return out;
    }

    /** Queries running against each shard right now, so a release does not close a reader mid-query. */
    final Map<ShardId, java.util.concurrent.atomic.AtomicInteger> inFlight = new ConcurrentHashMap<>();
    final Set<ShardId> readers = ConcurrentHashMap.newKeySet();
    volatile java.util.function.Function<ShardId, SegmentPublisher> publishers;

    /** Whether a shard's next publish puts its segments in node packs, or null to leave its publisher as built. */
    volatile java.util.function.Predicate<IndexShard> nodePackPlacement;

    /**
     * Sets how a shard's segment placement is decided at each publish.
     *
     * @param placement true for node packs
     */
    public void setNodePackPlacement(java.util.function.Predicate<IndexShard> placement) {
        this.nodePackPlacement = placement;
    }

    volatile java.util.function.Function<ShardId, org.opensearch.serverless.store.WalStore> walStores;

    /** Whether this node still owns a shard at a term, asked by a writer after it fences its own term. */
    volatile java.util.function.BiPredicate<ShardId, Long> ownsAtTerm;

    /**
     * Sets how a writer confirms, after fencing its own term, that the term is still this node's.
     *
     * @param ownsAtTerm true while the shard's head names this node at that term
     */
    public void setOwnershipCheck(java.util.function.BiPredicate<ShardId, Long> ownsAtTerm) {
        this.ownsAtTerm = ownsAtTerm;
    }

    final Map<ShardId, org.opensearch.serverless.store.WalStore> walCache = new ConcurrentHashMap<>();
    /** One publisher per open shard, so it can remember the generation of its own last publish. */
    final Map<ShardId, SegmentPublisher> publisherCache = new ConcurrentHashMap<>();

    /**
     * Creates a reconciler for one node.
     *
     * @param indicesService the node's indices service
     * @param localNode this node's identity
     */
    public ShardReconciler(IndicesService indicesService, DiscoveryNode localNode) {
        this.indicesService = indicesService;
        this.localNode = localNode;
    }

    /** Opens shards for this reconciler. */
    final ShardOpening opener = new ShardOpening(this);

    /** Opens and closes this reconciler's readers and frozen views. */
    final ShardReaders readerOps = new ShardReaders(this);
    /** Applies changed mappings and settings to open shards. */
    final ShardRefresh refresh = new ShardRefresh(this);

    /**
     * Returns what applies changed mappings and settings to the shards held open here.
     *
     * @return the refresher
     */
    public ShardRefresh refresh() {
        return refresh;
    }

    /** Publishes this reconciler's writers. */
    final ShardPublishing publishing = new ShardPublishing(this);

    /** Closes this reconciler's shards. */
    final ShardRelease releasing = new ShardRelease(this);

    /**
     * Supplies the object-store publisher for each shard. Without one, shards open from local disk only,
     * which is what the projection tests exercise.
     *
     * @param publishers index name and shard number to publisher
     */
    public void setSegmentPublishers(java.util.function.Function<ShardId, SegmentPublisher> publishers) {
        this.publishers = publishers;
    }

    /**
     * Supplies the write-ahead log for each shard. Without one, writes are durable only once published.
     *
     * <p>Keyed on the {@link ShardId} rather than on a name and a number, because a shard's storage is
     * located by the index's uuid and a {@code ShardId} carries it. Passing the name alone meant the plane
     * had to read the descriptor back to find the uuid, on a path that already knew it.
     *
     * @param walStores shard to WAL store
     */
    public void setWalStores(java.util.function.Function<ShardId, org.opensearch.serverless.store.WalStore> walStores) {
        this.walStores = walStores;
    }

    /**
     * Returns the WAL for a shard, or null if none is configured.
     *
     * @param shardId the shard
     * @return the WAL store, or null
     */
    public org.opensearch.serverless.store.WalStore wal(ShardId shardId) {
        if (walStores == null) {
            return null;
        }
        // Memoized, and it has to be. A WalStore carries the append ordinal that orders records within a
        // term, and the snapshot that makes truncation lag a publish cycle. Building a fresh one per
        // call restarts the ordinal at 1, so every append overwrites the same blob and the log holds
        // exactly the last write -- which is what happened, and what the end-to-end test caught.
        return walCache.computeIfAbsent(shardId, walStores);
    }

    /** Per writer shard, the lock that keeps a commit from landing between an apply and its append. */
    final Map<ShardId, ReentrantReadWriteLock> writeGuards = new ConcurrentHashMap<>();

    ReentrantReadWriteLock guardFor(ShardId shardId) {
        return writeGuards.computeIfAbsent(shardId, ignored -> new ReentrantReadWriteLock());
    }

    /**
     * Holds a write's place against the publish path's commit, from before the engine applies it until its
     * log record is durable.
     *
     * <p><b>What this closes.</b> A write is applied to the engine and then appended to the log; a publish
     * flushes the engine and then swaps the manifest. With nothing between them, a flush can land between
     * an apply and its append: the commit contains the operation, the append then fails, the shard is
     * released and the caller is told the write was not acknowledged — and the successor restores the
     * commit and serves it. The refusal was a lie. A write holds this in read mode across apply and
     * append; {@link #publish} takes it in write mode around the flush only, so publishes still overlap
     * with uploads and writes still overlap with each other.
     *
     * @param shardId the shard being written
     * @return the hold, to be released once the append has landed or failed
     */
    public Releasable writeGuard(ShardId shardId) {
        final ReentrantReadWriteLock.ReadLock lock = guardFor(shardId).readLock();
        lock.lock();
        return lock::unlock;
    }

    /** Publishes a shard's current commit to the object store. */
    public CommitManifest publish(ShardId shardId, long term) throws IOException {
        return publishing.publish(shardId, term);
    }

    /** Publishes a shard's current commit, staging it if its publisher stages commits and the publish is routine. */
    public CommitManifest publish(ShardId shardId, long term, boolean mayStage) throws IOException {
        return publishing.publish(shardId, term, mayStage);
    }

    /**
     * Writes a shard's staged commit to its manifest register, if it has one.
     *
     * @param shardId the shard
     * @param dueOnly true to write it only once it has waited its time
     * @return the commit written, or null if nothing was
     * @throws IOException if the register cannot be written, or a newer term holds it
     */
    public CommitManifest checkpoint(ShardId shardId, boolean dueOnly) throws IOException {
        return publishing.checkpoint(shardId, dueOnly);
    }

    /**
     * Returns the newest commit this node holds for a shard it writes: a staged one, or what it last published.
     *
     * @param shardId the shard
     * @return the commit, or null if this node does not write it or has published nothing for it
     */
    public CommitManifest newestCommit(ShardId shardId) {
        if (open.containsKey(shardId) == false || readers.contains(shardId)) {
            return null;
        }
        final org.opensearch.serverless.store.SegmentPublisher publisher = publisherCache.get(shardId);
        return publisher == null ? null : publisher.newestCommit();
    }

    volatile java.util.function.BiConsumer<IndexShard, Exception> onShardFailed;

    /**
     * Sets what happens when an open shard's engine fails: told the shard and the cause, on the thread that failed it.
     *
     * @param onShardFailed the listener, or null for none
     */
    public void setOnShardFailed(java.util.function.BiConsumer<IndexShard, Exception> onShardFailed) {
        this.onShardFailed = onShardFailed;
    }

    volatile BeforePublish beforePublish;

    /**
     * Sets what runs before every publish writes its manifest.
     *
     * @param hook the hook, or null for none
     */
    public void setBeforePublish(BeforePublish hook) {
        this.beforePublish = hook;
    }

    /**
     * Opens and starts every assigned shard that is not already open. Idempotent, and never closes
     * anything.
     *
     * @param view the node-local view, used for index metadata and node identity
     * @param owned the shards this node owns according to shard-heads
     * @return the shards opened by this call, excluding those already open
     * @throws IOException if a shard cannot be created or recovered
     */
    public Set<ShardId> ensureOpen(ClusterState view, Collection<ShardAssignment> owned) throws IOException {
        final Set<ShardId> opened = new LinkedHashSet<>();
        for (ShardAssignment assignment : owned) {
            final IndexMetadata indexMetadata = view.metadata().index(assignment.indexName());
            if (indexMetadata == null) {
                // Truth says this node owns the shard, but the view does not describe the index. That is
                // a projector bug, not a removal: refusing loudly is the only safe response, because
                // continuing would silently serve nothing.
                throw new IllegalStateException(
                    "assigned shard "
                        + assignment
                        + " has no descriptor in the projected view; the projector must describe every"
                        + " index this node owns a shard of"
                );
            }
            final ShardId shardId = new ShardId(indexMetadata.getIndex(), assignment.shardId());
            final IndexShard already = open.get(shardId);
            if (already != null) {
                if (already.getOperationPrimaryTerm() >= assignment.term()) {
                    continue;
                }
                // Held here at an older term than the one this node has just won. The takeover fenced that
                // term's log, so a write through the old shard would land at the fence and be refused -- and
                // before the fence existed it landed behind the seal and was never replayed. Reopen at the
                // term that was won, which fences this node's own earlier writer and replays what it wrote.
                releaseShard(
                    shardId,
                    "reopening at term " + assignment.term() + ", won while held at " + already.getOperationPrimaryTerm()
                );
            }
            // A writer opens lazily too. It used to download every published file first, which made a
            // cold start proportional to the shard rather than to what the first write touches; a merge
            // reads its inputs through the block cache and writes its output locally, and the publisher
            // inherits unchanged files by name, so nothing about publishing needed the download.
            opener.openAndStart(onBlockCache(indexMetadata), shardId, assignment.term(), view.nodes(), true, null);
            opened.add(shardId);
        }
        return opened;
    }

    /**
     * The four calls that open a shard, in order. Step two is the one that is easy to omit: createIndex does
     * not apply the mapping, and without it the first write returns MAPPING_UPDATE_REQUIRED as a
     * <em>result value</em> rather than throwing.
     */
    /** When each open shard was last touched by a request. */
    final java.util.concurrent.ConcurrentHashMap<ShardId, Long> lastUsed = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * One monitor per index, guarding the create-or-update of its {@link IndexService}.
     *
     * <p>Evicted when the index's last shard closes, under the monitor itself, so a node that churns
     * through many indices — every frozen view is one — does not keep a lock object per index it ever
     * held. A thread that took the monitor from the map just before it was evicted re-reads the map after
     * acquiring it and goes round again, which is the only way a removed monitor can be told apart from a
     * live one.
     */
    final java.util.concurrent.ConcurrentHashMap<Index, Object> indexLocks = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Shards between {@code createShard} and being recorded as open, so an index's service is not closed
     * underneath a shard of it that is still being built.
     */
    final ConcurrentHashMap<Index, Set<ShardId>> opening = new ConcurrentHashMap<>();

    /**
     * The commit each shard currently being opened is opening at, for the directory factory.
     *
     * <p>The reconciler reads a shard's manifest once to decide how to open it; the directory the store
     * then builds needs the same manifest, and used to read it again. Armed here from before the shard is
     * created until it is open, so the factory can take the one already in hand — see
     * {@link #pendingManifest}.
     */
    final Map<ShardId, CommitManifest> pendingManifests = new ConcurrentHashMap<>();

    /**
     * Writers opened from a published commit that already held every operation they have, by the sequence number
     * that commit is certain to hold. Taken once by {@link #takePublishedThroughAtOpen}.
     */
    final Map<ShardId, Long> publishedThroughAtOpen = new ConcurrentHashMap<>();

    /**
     * Returns, once, how far the published commit a writer was just opened from already covers it: the commit's local
     * checkpoint, when nothing was replayed past it.
     *
     * <p>A shard taken and not written to has nothing to publish. Without this, the node taking it knew of no publish
     * of its own and published the same operations again under its term: a new commit, a manifest swap, a log trim
     * and the sweep after it, about a dozen requests per shard taken, and the same again when it was let go idle.
     *
     * @param shardId the writer just opened
     * @return the sequence number the published commit covers, or empty if it may not cover everything
     */
    public java.util.OptionalLong takePublishedThroughAtOpen(ShardId shardId) {
        final Long through = publishedThroughAtOpen.remove(shardId);
        return through == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(through);
    }

    /**
     * The log a shard currently being opened should replay, for the moments its engine is being built.
     *
     * <p><b>Why the engine cannot simply ask for the shard's log.</b> Every shard on this node has one --
     * a reader and a frozen view included -- and replaying into either of those would be wrong: a reader
     * serves a published commit and owns no history, and a view is a snapshot of one. Only the reconciler
     * knows which of the three it is opening, and it knows it only for the duration of that open. So the
     * answer is armed here, immediately before the engine is constructed, and disarmed the moment the
     * shard is open; an engine that asks at any other time is told there is nothing to replay, which is
     * the truthful answer.
     */
    final java.util.concurrent.ConcurrentHashMap<ShardId, org.opensearch.serverless.store.WalStore> pendingReplay =
        new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Returns the records the shard currently being opened should replay, if any.
     *
     * <p>Called by {@link org.opensearch.serverless.engine.ServerlessWriterEngine} from inside recovery.
     * The result is already bounded by the cutoff taken when this replay was armed, so records a node that
     * has lost the shard appended after that moment are not returned.
     *
     * @param shardId the shard whose engine is being built
     * @return the records to replay, empty if this shard should not replay any
     * @throws IOException if the log cannot be read
     */
    public java.util.List<org.opensearch.serverless.store.WalRecord> replayRecordsFor(ShardId shardId) throws IOException {
        final var records = new java.util.ArrayList<org.opensearch.serverless.store.WalRecord>();
        forEachReplayRecord(shardId, records::add);
        return records;
    }

    /** Streams a fenced log once; the engine retains only a disposable local spool. */
    public void forEachReplayRecord(
        ShardId shardId,
        org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, IOException> consumer
    ) throws IOException {
        final var log = pendingReplay.get(shardId);
        if (log == null) {
            return;
        }
        final long readingAt = System.nanoTime();
        try {
            log.forEachReplayableAfterFencing(record -> {
                consumer.accept(record);
                logRecordsRead.incrementAndGet();
            });
            // After the shard's own log, which holds what was written before the node log was: oldest first.
            final NodeLogReplay fromNodeLog = pendingNodeLogReplay.get(shardId);
            final NodeLogHooks hooks = nodeLog;
            if (fromNodeLog != null && hooks != null) {
                final org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, IOException> counting = record -> {
                    consumer.accept(record);
                    logRecordsRead.incrementAndGet();
                };
                // Oldest writer first, so a record a later writer replayed and wrote again comes after the original.
                final java.util.List<String> writers = fromNodeLog.writers().isEmpty()
                    ? java.util.Collections.singletonList(null)
                    : fromNodeLog.writers();
                for (String writer : writers) {
                    nodeLogReplayed.merge(shardId, hooks.replay(shardId, writer, fromNodeLog.term(), counting), Long::sum);
                }
            }
            // Last: records a caller handed in for this open, newer than anything the shard's logs hold.
            final java.util.List<org.opensearch.serverless.store.WalRecord> preloaded = preloadedReplay.remove(shardId);
            if (preloaded != null) {
                for (org.opensearch.serverless.store.WalRecord record : preloaded) {
                    consumer.accept(record);
                }
                // Counted as node-log replay, so the open publishes them before it serves: no log of this shard holds them.
                nodeLogReplayed.merge(shardId, (long) preloaded.size(), Long::sum);
            }
        } finally {
            writerOpenNanos[WRITER_OPEN_PHASES.indexOf("log_read")].addAndGet(System.nanoTime() - readingAt);
        }
    }

    /**
     * Hands the next writer open of a shard records to replay with their own sequence numbers, after whatever the shard's
     * logs hold: for a caller that builds a shard from part of another, such as a shard split, whose records are in the
     * other shard's log and not in this one's. Nothing calls it by default.
     *
     * <p>Replayed as recovery replays a log, so each record keeps its sequence number, and the sequence numbers missing
     * between them are filled with no-ops once recovery ends. A shard that replayed any is published before the open
     * returns, so no later open depends on the other shard's log for them. The records are dropped, unused, by an open
     * that replays no log, such as a reader's, and by an open that fails.
     *
     * @param shardId the shard about to be opened as a writer on this node
     * @param records the records, oldest first, each past the local checkpoint of the commit the shard opens from
     * @throws IllegalStateException if records are already waiting for the shard's next open
     */
    public void preloadReplay(ShardId shardId, java.util.List<org.opensearch.serverless.store.WalRecord> records) {
        if (preloadedReplay.putIfAbsent(shardId, java.util.List.copyOf(records)) != null) {
            throw new IllegalStateException("records are already waiting for the next open of " + shardId);
        }
    }

    final java.util.concurrent.ConcurrentHashMap<ShardId, java.util.List<org.opensearch.serverless.store.WalRecord>> preloadedReplay =
        new java.util.concurrent.ConcurrentHashMap<>();

    public boolean retainReplay(ShardId shardId, org.opensearch.serverless.engine.ReplaySpool spool) throws IOException {
        if (replayedRecords.putIfAbsent(shardId, spool) != null) {
            throw new IOException("replay already retained for " + shardId);
        }
        return true;
    }

    final java.util.concurrent.ConcurrentHashMap<ShardId, org.opensearch.serverless.engine.ReplaySpool> replayedRecords =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** Per index and shard, the term at which a takeover has just fenced every older term of the log. */
    final java.util.concurrent.ConcurrentHashMap<String, Long> olderTermsFenced = new java.util.concurrent.ConcurrentHashMap<>();

    /** What a shard's open and publish need of the node's per-node write log, when it writes one. */
    public interface NodeLogHooks extends LoggedCommitHooks {
        /**
         * Returns how far a shard's records have landed in this node's log.
         *
         * @param shardId the shard
         * @return the position, or null if none is unpublished
         */
        org.opensearch.serverless.store.NodeLogCommitter.Position position(ShardId shardId);

        /**
         * Records that a publish covering everything up to a position has landed.
         *
         * @param shardId the shard
         * @param upTo what {@link #position} returned before the publish flushed, or null
         */
        void published(ShardId shardId, org.opensearch.serverless.store.NodeLogCommitter.Position upTo);

        /**
         * Streams the records a shard being opened must replay from the node logs.
         *
         * @param shardId the shard
         * @param writer a node whose log may hold the shard's unpublished records, or null for this node's own reopen
         * @param term the term the shard opens at
         * @param consumer what takes each record
         * @return how many records were streamed
         * @throws IOException if a fence or a read fails
         */
        long replay(
            ShardId shardId,
            String writer,
            long term,
            org.opensearch.common.CheckedConsumer<org.opensearch.serverless.store.WalRecord, IOException> consumer
        ) throws IOException;

        /**
         * Records that a shard opened at a term has replayed every log its head named and published what they held, so
         * the head need name them no longer.
         *
         * @param shardId the shard
         * @param term the term it opened at
         * @throws IOException if the head cannot be read or written
         */
        void replayedAndPublished(ShardId shardId, long term) throws IOException;

        /**
         * Records in the shard's head that its own write log is quiet: every term of it closed, and whatever it held
         * published. Owners that write their node's log then open it without listing it.
         *
         * @param shardId the shard
         * @param term the term this node holds it at
         * @throws IOException if the head cannot be read or written
         */
        void shardLogQuiet(ShardId shardId, long term) throws IOException;
    }

    volatile NodeLogHooks nodeLog;

    /**
     * Sets this node's per-node write log, so opens replay from it and publishes trim it; null for a node that writes
     * none.
     *
     * @param nodeLog the hooks
     */
    public void setNodeLog(NodeLogHooks nodeLog) {
        this.nodeLog = nodeLog;
    }

    /** Per index and shard, the term at which this node took a head that said the shard's own log is quiet. */
    final java.util.concurrent.ConcurrentHashMap<String, Long> shardLogQuietAt = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Shards whose own log this node closed and replayed records from at open, by the term it holds them at: the first
     * publish at that term holds those records, and then the head can say the log is quiet.
     */
    final java.util.concurrent.ConcurrentHashMap<ShardId, Long> shardLogQuietOnPublish = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Records whether the head this node just took says the shard's own log is quiet, so the open that follows -- at
     * that term -- neither fences nor replays it. One use: the open consumes it.
     *
     * @param indexName the index
     * @param shard the shard
     * @param term the term taken
     * @param quiet whether the head says the shard's own log is quiet
     */
    public void noteShardLogQuiet(String indexName, int shard, long term, boolean quiet) {
        if (quiet) {
            shardLogQuietAt.put(indexName + "#" + shard, term);
        } else {
            shardLogQuietAt.remove(indexName + "#" + shard);
        }
    }

    /** Per index and shard, the nodes the head named when it was taken: whose node logs the open replays. */
    final java.util.concurrent.ConcurrentHashMap<String, List<String>> nodeLogWriters = new java.util.concurrent.ConcurrentHashMap<>();

    /** A writer open in progress: whose node logs it replays, oldest first, and at what term. */
    record NodeLogReplay(List<String> writers, long term) {
    }

    final java.util.concurrent.ConcurrentHashMap<ShardId, NodeLogReplay> pendingNodeLogReplay =
        new java.util.concurrent.ConcurrentHashMap<>();

    /** Records streamed from the node logs by an open in progress; a shard that replayed any publishes before serving. */
    final java.util.concurrent.ConcurrentHashMap<ShardId, Long> nodeLogReplayed = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Records the nodes a shard's head named when this node took it, so the open that follows replays their write logs.
     * One use: the open consumes it. An open with none noted is this node reopening its own shard.
     *
     * @param indexName the index
     * @param shard the shard
     * @param writers the nodes whose logs may hold the shard's unpublished records, oldest first; empty for none
     */
    public void noteNodeLogWriters(String indexName, int shard, List<String> writers) {
        if (writers.isEmpty()) {
            nodeLogWriters.remove(indexName + "#" + shard);
        } else {
            nodeLogWriters.put(indexName + "#" + shard, List.copyOf(writers));
        }
    }

    /**
     * Records that a takeover has fenced every term older than {@code term}, so the open that follows it -- at
     * that term -- need not list the log again to find them fenced. One use: the open consumes it.
     *
     * @param indexName the index
     * @param shard the shard
     * @param term the term taken
     */
    public void noteOlderTermsFenced(String indexName, int shard, long term) {
        olderTermsFenced.put(indexName + "#" + shard, term);
    }

    /**
     * Returns the commit a shard being opened right now is opening at, for the store's directory factory.
     *
     * <p>Armed for exactly the window in which the shard's {@code Store} is built; empty at any other
     * time, when the factory should read the register itself. A frozen view's slot holds the frozen commit
     * under the view's own index identity, so the factory need not read the view's record either.
     *
     * @param index the index the store is being built for, a view's synthetic identity included
     * @param shardNumber the shard
     * @return the commit, if one is armed
     */
    public Optional<CommitManifest> pendingManifest(Index index, int shardNumber) {
        return Optional.ofNullable(pendingManifests.get(new ShardId(index, shardNumber)));
    }

    /** The given index, served from the block-cache directory: published segments remote, own output local. */
    static IndexMetadata onBlockCache(IndexMetadata indexMetadata) {
        final String storeType = org.opensearch.serverless.shell.ServerlessNode.BLOCK_CACHE_STORE_TYPE;
        if (storeType.equals(indexMetadata.getSettings().get("index.store.type"))) {
            return indexMetadata;
        }
        return IndexMetadata.builder(indexMetadata)
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put("index.store.type", storeType)
                    .build()
            )
            .build();
    }

    /** Opens a shard as a reader: serving the published commit, owning nothing. */
    public ShardId openReader(ClusterState view, String indexName, int shardNumber) throws IOException {
        return readerOps.openReader(view, indexName, shardNumber);
    }

    /** Opens a shard as a reader at a commit the caller has already read. */
    public ShardId openReader(ClusterState view, String indexName, int shardNumber, CommitManifest known) throws IOException {
        return readerOps.openReader(view, indexName, shardNumber, known);
    }

    /** What {@link #releaseReader} did. */
    public enum ReaderRelease {
        /** It was a reader, and it is closed. */
        RELEASED,
        /** It is a reader and a query is running against it; left open. */
        BUSY,
        /** It is not a reader, or not the one the caller judged: nothing was touched. */
        NOT_A_READER
    }

    /** Closes a shard only if it is still the reader the caller judged it to be. */
    public ReaderRelease releaseReader(ShardId shardId, CommitManifest servingCommit, String reason) {
        return readerOps.releaseReader(shardId, servingCommit, reason);
    }

    /**
     * Striped monitors over registering a shard as a reader and releasing it, so "is it a reader" and "close it"
     * cannot be split by a writer taking its place. Striped rather than one per shard, so a node that has cycled
     * through a million shards holds 64 objects rather than a million.
     */
    final Object[] lifecycleStripes = newStripes(64);

    private static Object[] newStripes(int count) {
        final Object[] stripes = new Object[count];
        for (int i = 0; i < count; i++) {
            stripes[i] = new Object();
        }
        return stripes;
    }

    Object lifecycle(ShardId shardId) {
        return lifecycleStripes[Math.floorMod(shardId.hashCode(), lifecycleStripes.length)];
    }

    /** The commit each reader was opened at, so a pass can tell when it has gone stale. */
    final java.util.Map<ShardId, CommitManifest> readerCommits = new ConcurrentHashMap<>();

    /** Returns the commit a reader is serving. */
    public Optional<CommitManifest> readerCommit(ShardId shardId) {
        return readerOps.readerCommit(shardId);
    }

    /** Opens one shard of a frozen view, refusing a view taken over an earlier index of the same name. */
    public ShardId openFrozenReader(IndexMetadata indexMetadata, PointInTime pit, int shardNumber, DiscoveryNodes nodes)
        throws IOException {
        return readerOps.openFrozenReader(indexMetadata, pit, shardNumber, nodes);
    }

    /** Opens a shard at a commit that is not the current one, under an identity of its own. */
    public ShardId openFrozenReader(
        IndexMetadata indexMetadata,
        String viewId,
        int shardNumber,
        CommitManifest manifest,
        DiscoveryNodes nodes
    ) throws IOException {
        return readerOps.openFrozenReader(indexMetadata, viewId, shardNumber, manifest, nodes);
    }

    /** Closes a frozen view, releasing the shards it opened and deleting what they left on disk. */
    public int closeFrozenReader(String viewId) {
        return readerOps.closeFrozenReader(viewId);
    }

    final Set<ShardId> frozenViews = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Returns the shards open here as readers.
     *
     * @return the reader shard ids
     */
    public Set<ShardId> readerShards() {
        return Set.copyOf(readers);
    }

    /**
     * Marks a query as running against a shard, so a release waits for it.
     *
     * <p>Bracketed with {@link #exit} around every local {@code ShardQuery.execute}. A pass that found a
     * reader's commit superseded used to close it under a running aggregation; the query failed with a
     * closed reader, which looks like corruption and is a scheduling choice.
     *
     * @param shardId the shard
     * @throws org.opensearch.index.shard.IndexShardClosedException if it closed before admission
     */
    public void enter(ShardId shardId) {
        synchronized (lifecycle(shardId)) {
            if (open.containsKey(shardId) == false) {
                throw new org.opensearch.index.shard.IndexShardClosedException(shardId);
            }
            inFlight.computeIfAbsent(shardId, ignored -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
        }
    }

    /**
     * Marks a query against a shard as finished.
     *
     * @param shardId the shard
     */
    public void exit(ShardId shardId) {
        synchronized (lifecycle(shardId)) {
            final java.util.concurrent.atomic.AtomicInteger count = inFlight.get(shardId);
            if (count != null && count.decrementAndGet() <= 0) {
                inFlight.remove(shardId, count);
            }
        }
    }

    /**
     * Returns how many queries are running against a shard.
     *
     * @param shardId the shard
     * @return the count
     */
    public int inFlight(ShardId shardId) {
        final java.util.concurrent.atomic.AtomicInteger count = inFlight.get(shardId);
        return count == null ? 0 : Math.max(0, count.get());
    }

    /**
     * Closes one shard. This is the <em>only</em> way a shard closes, and the caller must have learned
     * from a shard-head that this node no longer owns it.
     *
     * <p>Takes the shard out of the {@code IndexService} whether or not this reconciler had recorded it as
     * open: a shard whose activation failed part-way is in the service and not in the record, and leaving
     * it there refuses every later attempt with "already exists".
     *
     * <p>A shard with a query running against it is left for the next pass: the caller learned the shard
     * is not ours from truth that will still say so then, and a query mid-flight losing its reader is the
     * failure this refuses to cause.
     *
     * @param shardId the shard to release
     * @param reason why, for the log
     */
    public void releaseShard(ShardId shardId, String reason) {
        releaseShard(shardId, reason, false);
    }

    /** @return true if the shard was released; false if a query is running against it and it was left */
    boolean releaseShard(ShardId shardId, String reason, boolean deleteStore) {
        synchronized (lifecycle(shardId)) {
            return releaseShardLocked(shardId, reason, deleteStore);
        }
    }

    /**
     * Stops counting an open reader as a reader, leaving it open: the state a reader passed through between being opened
     * and being recorded, before readers were recorded first. For tests of what the write path does in that state.
     *
     * @param shardId the reader
     */
    public void unrecordReaderForTesting(ShardId shardId) {
        readers.remove(shardId);
    }

    /** Removes only the engine that failed. */
    public FailedShardRelease releaseFailedShard(IndexShard failed, String reason) {
        return releasing.releaseFailedShard(failed, reason);
    }

    boolean releaseShardLocked(ShardId shardId, String reason, boolean deleteStore) {
        return releasing.releaseShardLocked(shardId, reason, deleteStore);
    }

    boolean releaseShardLocked(ShardId shardId, String reason, boolean deleteStore, boolean failed) {
        return releasing.releaseShardLocked(shardId, reason, deleteStore, failed);
    }

    void closeIndexIfUnused(Index index, String reason, boolean deleteStore) {
        releasing.closeIndexIfUnused(index, reason, deleteStore);
    }

    /**
     * Returns the shard for an id, or null if this node does not hold it.
     *
     * @param shardId the shard
     * @return the shard, or null
     */
    public IndexShard shard(ShardId shardId) {
        return open.get(shardId);
    }

    /**
     * Returns a live serving shard by its complete identity, excluding frozen views.
     *
     * @param shardId the index name, UUID and shard number
     * @return the shard, or null when not held as a live reader or writer
     */
    public IndexShard servingShard(ShardId shardId) {
        return frozenViews.contains(shardId) ? null : open.get(shardId);
    }

    /**
     * Returns a writer by its complete identity without building snapshots of the held shards.
     *
     * @param shardId the index name, UUID and shard number
     * @return the writer, or null for an absent shard, a reader or a frozen view
     */
    public IndexShard writerShard(ShardId shardId) {
        final IndexShard held = servingShard(shardId);
        return held == null || readers.contains(shardId) ? null : held;
    }

    /**
     * Checks reader membership without copying the full reader set.
     *
     * @param shardId the shard to check
     * @return whether it is held as a reader
     */
    public boolean isReader(ShardId shardId) {
        return readers.contains(shardId);
    }

    /**
     * Records that something a user asked for touched this shard.
     *
     * <p><b>Called from the request paths and nowhere else</b>, which is the whole point. The obvious
     * place to put this is {@link #shard(ShardId)}, and that would be wrong: publication, heartbeats and
     * the garbage collector all reach for a shard, so a shard would count as busy because the node was
     * maintaining it. Only work somebody asked for should keep a shard resident.
     *
     * @param shardId the shard used
     * @param nowMillis when
     */
    public void markUsed(ShardId shardId, long nowMillis) {
        if (open.containsKey(shardId)) {
            lastUsed.put(shardId, nowMillis);
        }
    }

    /**
     * When this shard was last used by a request, or empty if it has never been.
     *
     * <p>A shard that was opened and never asked for anything has no entry, and is treated as having been
     * used when it opened — see {@code BackgroundReconciler#releaseIdle}. Reporting "never" as "infinitely
     * idle" would release a shard the instant after activating it, which is the loop a demand-driven node
     * would then spin in forever.
     *
     * @param shardId the shard
     * @return the timestamp, or empty
     */
    public java.util.OptionalLong lastUsed(ShardId shardId) {
        final Long at = lastUsed.get(shardId);
        return at == null ? java.util.OptionalLong.empty() : java.util.OptionalLong.of(at);
    }

    /**
     * Returns the shards this node is serving as itself.
     *
     * <p><b>Frozen views are not among them, and leaving them in was a bug with a long reach.</b> Every
     * caller of this finds a shard by index name and shard number, because until a view existed a node
     * could not hold two shards of one index name. A view has the name of the index it is a view of, so
     * a document write looking for "shard 0 of paged" could match the view instead -- a reader -- and
     * conclude the node was not serving the shard it had been writing to a moment earlier. It answered
     * "activation in progress, retry" for a shard that was open, owned, and healthy, and only sometimes,
     * because it depended on which of the two a set iterated first.
     *
     * <p>Fixing the seven lookups would have left the eighth to write. This is the one definition they
     * all read, so it is the one that changes: a view is not something this node serves, it is something
     * a caller is holding, and {@link #frozenShards()} is where it is counted.
     *
     * @return the open shard ids, excluding frozen views
     */
    public Set<ShardId> openShards() {
        final Set<ShardId> serving = new java.util.HashSet<>(open.keySet());
        serving.removeAll(frozenViews);
        return Set.copyOf(serving);
    }

    /**
     * Tests a full shard identity without copying the frozen view inventory.
     *
     * @param shardId the index name, view uuid and shard number
     * @return whether the shard is held as a frozen view
     */
    public boolean isFrozen(ShardId shardId) {
        return frozenViews.contains(shardId);
    }

    /**
     * Returns the frozen views open on this node.
     *
     * <p>They are held, they cost memory and file handles, and nothing releases them but the caller or
     * expiry -- so they are counted against what a node is holding even though they are never a candidate
     * for eviction. Evicting one would be sound (it can be reopened from the record), but it would also
     * be pointless churn: the caller is by definition still using it.
     *
     * @return the shard ids of open frozen views
     */
    public Set<ShardId> frozenShards() {
        return Set.copyOf(frozenViews);
    }

    /**
     * Returns every shard open on this node, views included.
     *
     * @return all held shard ids
     */
    public Set<ShardId> heldShards() {
        return Set.copyOf(open.keySet());
    }
}
