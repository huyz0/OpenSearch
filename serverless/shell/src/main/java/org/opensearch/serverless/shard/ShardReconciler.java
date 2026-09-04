/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.MergedSegmentWarmerFactory;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Opens and closes shards on this node.
 *
 * <p><b>The invariant this class exists to hold</b>, established by measurement in
 * {@code s1-findings.md} rather than by argument:
 *
 * <blockquote>Absence from a projected view is never a removal signal. A shard closes when the
 * shard-head says this node no longer owns it, and never because a locally-computed view failed to
 * mention it.</blockquote>
 *
 * <p>S1 measured both halves of why that matters. Applying a view naming no indices at all left a shard
 * STARTED and still serving, so projection is harmless on its own. But the close-set a
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

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(ShardReconciler.class);

    private final IndicesService indicesService;
    private final DiscoveryNode localNode;
    private final Map<ShardId, IndexShard> open = new ConcurrentHashMap<>();
    /** Queries running against each shard right now, so a release does not close a reader mid-query. */
    private final Map<ShardId, java.util.concurrent.atomic.AtomicInteger> inFlight = new ConcurrentHashMap<>();
    private final Set<ShardId> readers = ConcurrentHashMap.newKeySet();
    private volatile java.util.function.Function<ShardId, SegmentPublisher> publishers;
    private volatile java.util.function.Function<ShardId, org.opensearch.serverless.store.WalStore> walStores;
    private final Map<ShardId, org.opensearch.serverless.store.WalStore> walCache = new ConcurrentHashMap<>();
    /** One publisher per open shard, so it can remember the generation of its own last publish. */
    private final Map<ShardId, SegmentPublisher> publisherCache = new ConcurrentHashMap<>();

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

    /**
     * Supplies the object-store publisher for each shard. Without one, shards open from local disk only,
     * which is the phase 2 behaviour and is what the projection tests still exercise.
     *
     * @param publishers index name and shard number to publisher
     */
    public void setSegmentPublishers(java.util.function.Function<ShardId, SegmentPublisher> publishers) {
        this.publishers = publishers;
    }

    /**
     * Supplies the write-ahead log for each shard. Without one, writes are durable only once published,
     * which is the pre-M10 behaviour.
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
    private final Map<ShardId, ReentrantReadWriteLock> writeGuards = new ConcurrentHashMap<>();

    private ReentrantReadWriteLock guardFor(ShardId shardId) {
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

    /**
     * Publishes a shard's current commit to the object store.
     *
     * @param shardId the shard
     * @param term the owning writer's term, which fences a zombie
     * @return the manifest published
     * @throws IOException if the shard is not held here, or publication fails
     */
    public CommitManifest publish(ShardId shardId, long term) throws IOException {
        final IndexShard shard = open.get(shardId);
        if (shard == null) {
            throw new IOException("cannot publish " + shardId + ": not open on " + localNode.getId());
        }
        if (publishers == null) {
            throw new IOException("cannot publish " + shardId + ": no object store configured");
        }
        if (readers.contains(shardId)) {
            // The read-only engine already makes this impossible; this refuses earlier and says why,
            // rather than surfacing as some Lucene-level complaint about a closed writer.
            throw new IOException("cannot publish " + shardId + ": it is open as a reader and owns no head");
        }
        // A commit must exist before there is anything to publish; an unflushed shard has its data only
        // in the translog, which this does not upload. Under the write guard, so the commit cannot split
        // an apply from its append -- see writeGuard for the lie that would tell.
        final ReentrantReadWriteLock.WriteLock exclusive = guardFor(shardId).writeLock();
        exclusive.lock();
        try {
            shard.flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true).waitIfOngoing(true));
        } finally {
            exclusive.unlock();
        }
        final CommitManifest manifest;
        // The commit is pinned for the length of the upload. Without the pin, a periodic flush landing
        // mid-upload let the deletion policy remove a file only the older commit referenced, and the
        // upload failed on a file that had been there a moment ago.
        try (GatedCloseable<IndexCommit> commit = shard.acquireLastIndexCommit(false)) {
            // Named, so a manifest says who wrote it. A term is not an identity: the publish fence refuses
            // a newer term and cannot tell two nodes holding the same one apart.
            manifest = publisherCache.computeIfAbsent(shardId, publishers).publish(shard.store(), commit.get(), term, localNode.getId());
        }
        final var walForPublish = wal(shardId);
        if (walForPublish != null) {
            // Only after the commit is durable in the object store. Truncation drops what the previous
            // publish saw, never what this one did -- see WalStore for why that gap is load-bearing.
            walForPublish.onPublished(term);
        }
        return manifest;
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
            if (open.containsKey(shardId)) {
                continue;
            }
            // A writer opens lazily too. It used to download every published file first, which made a
            // cold start proportional to the shard rather than to what the first write touches; a merge
            // reads its inputs through the block cache and writes its output locally, and the publisher
            // inherits unchanged files by name, so nothing about publishing needed the download.
            openAndStart(onBlockCache(indexMetadata), shardId, assignment.term(), view.nodes(), true, null);
            opened.add(shardId);
        }
        return opened;
    }

    /**
     * The four calls S0 measured, in order. Step two is the one that is easy to omit: createIndex does
     * not apply the mapping, and without it the first write returns MAPPING_UPDATE_REQUIRED as a
     * <em>result value</em> rather than throwing (see {@code s0-findings.md} F5).
     */
    /** When each open shard was last touched by a request. */
    private final java.util.concurrent.ConcurrentHashMap<ShardId, Long> lastUsed = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * One monitor per index, guarding the create-or-update of its {@link IndexService}.
     *
     * <p>Evicted when the index's last shard closes, under the monitor itself, so a node that churns
     * through many indices — every frozen view is one — does not keep a lock object per index it ever
     * held. A thread that took the monitor from the map just before it was evicted re-reads the map after
     * acquiring it and goes round again, which is the only way a removed monitor can be told apart from a
     * live one.
     */
    private final java.util.concurrent.ConcurrentHashMap<Index, Object> indexLocks = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Shards between {@code createShard} and being recorded as open, so an index's service is not closed
     * underneath a shard of it that is still being built.
     */
    private final Set<ShardId> opening = ConcurrentHashMap.newKeySet();

    /**
     * The commit each shard currently being opened is opening at, for the directory factory.
     *
     * <p>The reconciler reads a shard's manifest once to decide how to open it; the directory the store
     * then builds needs the same manifest, and used to read it again. Armed here from before the shard is
     * created until it is open, so the factory can take the one already in hand — see
     * {@link #pendingManifest}.
     */
    private final Map<ShardId, CommitManifest> pendingManifests = new ConcurrentHashMap<>();

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
    private final java.util.concurrent.ConcurrentHashMap<ShardId, org.opensearch.serverless.store.WalStore> pendingReplay =
        new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The cutoff each pending replay is bounded by, snapshotted when the replay was armed.
     *
     * <p>See {@link org.opensearch.serverless.store.WalStore#position()} for what the cutoff is and why a
     * successor needs one. It is held next to the log rather than inside it because a {@code WalStore} is
     * the shard's live log, shared with the write path, and a cutoff belongs to one act of recovery.
     */
    private final java.util.concurrent.ConcurrentHashMap<ShardId, java.util.Map<Long, String>> pendingCutoff =
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
        final org.opensearch.serverless.store.WalStore wal = pendingReplay.get(shardId);
        if (wal == null) {
            return java.util.List.of();
        }
        return wal.replayable(pendingCutoff.get(shardId));
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
    private static IndexMetadata onBlockCache(IndexMetadata indexMetadata) {
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

    /**
     * @param knownCommit the commit to recover from, when the caller already knows it; null to ask the
     *     publisher for the current one. A frozen view has to pass it, for two reasons: the commit it
     *     wants is by definition not the current one, and its synthetic index uuid has no manifest
     *     register of its own, so asking would answer "nothing published" and start an empty shard --
     *     which is a wrong answer that looks exactly like a right one. A reader passes the one it read to
     *     decide whether it could open at all, so an open reads the register once.
     */
    private IndexShard openAndStart(
        IndexMetadata indexMetadata,
        ShardId shardId,
        long shardHeadTerm,
        DiscoveryNodes nodes,
        boolean replayWal,
        CommitManifest knownCommit
    ) throws IOException {
        opening.add(shardId);
        try {
            final IndexService indexService = indexServiceFor(indexMetadata);

            // Whether anything was published decides the recovery source, and the decision must be made
            // before the shard is created because the source is baked into its routing entry.
            final SegmentPublisher publisher = publishers == null ? null : publishers.apply(shardId);
            final Optional<CommitManifest> published = knownCommit != null
                ? Optional.of(knownCommit)
                : (publisher == null ? Optional.empty() : publisher.readManifest());
            final boolean restoring = published.isPresent() && published.get().files().isEmpty() == false;

            final ShardRouting initializing = ShardRouting.newUnassigned(
                shardId,
                true,
                restoring ? RecoverySource.ExistingStoreRecoverySource.INSTANCE : RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "serverless activation")
            ).initialize(localNode.getId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

            final IndexShard shard;
            published.ifPresent(manifest -> pendingManifests.put(shardId, manifest));
            try {
                shard = indexService.createShard(
                    initializing,
                    ignored -> {},
                    RetentionLeaseSyncer.EMPTY,
                    null,
                    null,
                    null,
                    localNode,
                    null,
                    nodes,
                    new MergedSegmentWarmerFactory(null, null, null),
                    null,
                    null
                );
            } finally {
                pendingManifests.remove(shardId);
            }

            // From here the IndexService holds a shard that is not yet ours. Anything that fails before
            // it is must take the shard back out, or the next attempt is refused with "already exists"
            // for as long as the process lives -- while the head still names this node, so nobody else
            // can take it either. That was a permanent single-shard outage from one object-store hiccup.
            try {
                if (restoring) {
                    // Whatever the manifest names is the truth and is read from the object store; a
                    // same-named local file is this node's own earlier output and may not be the same
                    // bytes. Local commits go too, so the history bootstrap below starts from the
                    // published segments_N rather than from a stale local one that happens to be newer.
                    dropLocalFilesNamedBy(shard.store().directory(), published.get());
                    // A translog is still needed, because recovery reads one and a node taking a shard
                    // over has none.
                    bootstrapTranslogFor(shard);
                }

                if (replayWal && walStores != null) {
                    // Armed for exactly the window in which the engine is built and recovery runs, which is
                    // the only window in which core will accept operations carrying their own sequence
                    // numbers.
                    final org.opensearch.serverless.store.WalStore log = wal(shardId);
                    pendingReplay.put(shardId, log);
                    // Seal the log before recovery reads anything, so a predecessor that has not yet noticed
                    // it lost this shard cannot have its later appends replayed as acknowledged history. The
                    // seal is durable, so it binds every later successor too and not just this recovery --
                    // see WalStore#sealAt for why that distinction is the whole point.
                    //
                    // <b>The window this leaves is real and worth naming.</b> The shard-head was won earlier,
                    // in MetadataPlane#activate, and anything the predecessor appends between that moment and
                    // this one still falls inside the seal. Narrowing it further means sealing at the
                    // compare-and-swap, which the path that opens a shard from projected truth rather than
                    // from an acquisition does not have. What is closed here is the unbounded half: after
                    // this point the predecessor can append for as long as it likes and none of it is ever
                    // read, by anyone.
                    pendingCutoff.put(shardId, log.sealAt(shardHeadTerm));
                }
                try {
                    shard.markAsRecovering("serverless-store", new RecoveryState(initializing, localNode, null));
                    final PlainActionFuture<Boolean> recovered = PlainActionFuture.newFuture();
                    shard.recoverFromStore(recovered);
                    if (Boolean.TRUE.equals(recovered.actionGet()) == false) {
                        throw new IOException("recovery from store reported failure for " + shardId);
                    }
                } finally {
                    pendingReplay.remove(shardId);
                    pendingCutoff.remove(shardId);
                }

                final ShardRouting started = initializing.moveToStarted();
                // The applying version is the SHARD-HEAD TERM, not the projected view's version. S1
                // reproduced what happens when a per-node counter is used here: the update is silently
                // ignored, because ReplicationTracker gates on > against a value that may have crossed a
                // node boundary.
                shard.updateShardState(
                    started,
                    shardHeadTerm,
                    null,
                    shardHeadTerm,
                    Set.of(started.allocationId().getId()),
                    new IndexShardRoutingTable.Builder(shardId).addShard(started).build(),
                    nodes
                );

                if (replayWal && walStores != null) {
                    replayRecordsWithoutSequenceIdentity(shard, shardId);
                }
                open.put(shardId, shard);
                return shard;
            } catch (Exception e) {
                abandon(indexService, shardId, e);
                throw e;
            }
        } finally {
            opening.remove(shardId);
        }
    }

    /** Creates the index's service or refreshes its metadata, under the index's monitor. */
    private IndexService indexServiceFor(IndexMetadata indexMetadata) throws IOException {
        final Index index = indexMetadata.getIndex();
        // Locked per index, because this is a check-then-act and shards of one index now open at the same
        // time. Sequentially it was safe by luck; the moment the search fan-out became concurrent, three
        // shards of one index raced here and two lost with ResourceAlreadyExistsException -- reported as
        // two unreachable shards, which reads like a routing problem and is not one. IndexService.createShard
        // is itself synchronized, so only this acquisition needs guarding and shard opening stays parallel.
        while (true) {
            final Object lock = indexLocks.computeIfAbsent(index, ignored -> new Object());
            synchronized (lock) {
                if (indexLocks.get(index) != lock) {
                    // Evicted between the lookup and the acquisition; the current one is the monitor.
                    continue;
                }
                IndexService existing = indicesService.indexService(index);
                if (existing == null) {
                    existing = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
                    existing.updateMapping(null, indexMetadata);
                } else {
                    // The IndexService is created once per index but shards arrive one at a time, so by the
                    // time the second shard opens its metadata is stale -- in particular its per-shard
                    // primary terms. A shard created from stale metadata starts at the old term and then
                    // gets updateShardState at the new one, which the data plane rejects: "term is only
                    // increased as part of primary promotion". Found by the first multi-shard test; every
                    // earlier one had a single shard.
                    final IndexMetadata current = existing.getMetadata();
                    // At the same settings version the settings are, by definition, the same ones -- what
                    // this call carries is the terms. Handing core different settings at an unchanged
                    // version is an assertion failure in IndexService#updateMetadata, and the store type
                    // set on open is exactly such a difference once a settings refresh has been applied.
                    final IndexMetadata refreshed = current.getSettingsVersion() == indexMetadata.getSettingsVersion()
                        ? IndexMetadata.builder(indexMetadata).settings(current.getSettings()).build()
                        : indexMetadata;
                    existing.updateMetadata(current, refreshed);
                }
                return existing;
            }
        }
    }

    /** Takes a shard that never became ours back out of its IndexService, keeping the cause's own report. */
    private void abandon(IndexService indexService, ShardId shardId, Exception cause) {
        try {
            if (indexService.hasShard(shardId.id())) {
                indexService.removeShard(shardId.id(), "activation failed: " + cause.getMessage());
            }
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
        try {
            opening.remove(shardId);
            closeIndexIfUnused(shardId.getIndex(), "activation failed: " + cause.getMessage(), false);
        } catch (Exception e) {
            cause.addSuppressed(e);
        }
    }

    /**
     * Applies the records recovery could not replay for itself: those written before the log carried
     * sequence numbers. Everything else was replayed during recovery, as the operation it originally was,
     * by ServerlessWriterEngine -- which is what makes _seq_no survive a failover.
     *
     * <p>These go in the old way, as fresh primary operations, because a record that does not say which
     * operation it was cannot be replayed as that operation. They therefore get new sequence numbers,
     * exactly as every replayed record used to. Dropping them instead would turn a format change into
     * silent data loss.
     */
    private void replayRecordsWithoutSequenceIdentity(IndexShard shard, ShardId shardId) throws IOException {
        final var wal = wal(shardId);
        int replayed = 0;
        for (org.opensearch.serverless.store.WalRecord record : wal.replayable()) {
            if (record.hasSequenceIdentity()) {
                continue;
            }
            if (record.isDeletion()) {
                // Replayed in order with the writes, which is the only thing that makes the result
                // correct: a document written, deleted, and written again must end up present, and a
                // document written and deleted must end up gone. Applying all the writes and then all
                // the deletes would get the first case wrong.
                shard.applyDeleteOperationOnPrimary(
                    org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                    record.id(),
                    org.opensearch.index.VersionType.INTERNAL,
                    org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                    0
                );
                replayed++;
                continue;
            }
            shard.applyIndexOperationOnPrimary(
                org.opensearch.common.lucene.uid.Versions.MATCH_ANY,
                org.opensearch.index.VersionType.INTERNAL,
                new org.opensearch.index.mapper.SourceToParse(
                    shardId.getIndexName(),
                    record.id(),
                    new org.opensearch.core.common.bytes.BytesArray(record.source()),
                    org.opensearch.common.xcontent.XContentType.JSON
                ),
                org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                0,
                org.opensearch.action.index.IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false
            );
            replayed++;
        }
        if (replayed > 0) {
            // The replayed operations sit in the translog's write buffer, a page charged to the request breaker,
            // until synced; the object-store log is the durability, this gives the page back.
            shard.sync();
            shard.refresh("serverless-wal-replay");
        }
    }

    /**
     * Opens a shard as a reader: serving the published commit, owning nothing.
     *
     * <p>No compare-and-swap, no term bump, no entry in the shard-head. That is deliberate and is what
     * {@code rfc-serverless-metadata-plane.md} §6 means by readers being interchangeable caches — any
     * search node can serve any shard by reading its manifest, and no coordination is involved because
     * nothing can go wrong if two of them do it at once.
     *
     * <p>The term recorded on the shard is the manifest's — the term of the writer whose commit this is
     * serving. It is not a claim of ownership; the shard needs a positive term to start at all
     * ({@code s0-findings.md} F4), and the commit's own term is the only honest number available.
     *
     * @param view the node-local view, which must describe the index
     * @param indexName the index
     * @param shardNumber the shard
     * @return the shard id now open as a reader
     * @throws IOException if nothing has been published for the shard, or opening fails
     */
    public ShardId openReader(ClusterState view, String indexName, int shardNumber) throws IOException {
        return openReader(view, indexName, shardNumber, null);
    }

    /**
     * Opens a shard as a reader at a commit the caller has already read.
     *
     * <p>The caller that projects the view needs the manifest's term before it can project, so it has
     * read the register already; passing what it read is what makes a reader open cost one register read
     * rather than one per layer that needs it.
     *
     * @param view the node-local view, which must describe the index
     * @param indexName the index
     * @param shardNumber the shard
     * @param known the currently published commit, if the caller has read it; null to read it here
     * @return the shard id now open as a reader
     * @throws IOException if nothing has been published for the shard, or opening fails
     */
    public ShardId openReader(ClusterState view, String indexName, int shardNumber, CommitManifest known) throws IOException {
        final IndexMetadata indexMetadata = view.metadata().index(indexName);
        if (indexMetadata == null) {
            throw new IllegalStateException("cannot open a reader for " + indexName + ": the view does not describe it");
        }
        if (publishers == null) {
            throw new IOException("cannot open a reader for " + indexName + ": no object store configured");
        }
        final ShardId shardId = new ShardId(indexMetadata.getIndex(), shardNumber);
        if (open.containsKey(shardId)) {
            return shardId;
        }
        final CommitManifest manifest = known != null
            ? known
            : publishers.apply(shardId)
                .readManifest()
                .orElseThrow(
                    () -> new IOException(
                        "cannot serve "
                            + shardId
                            + " as a reader: nothing has been published for it. "
                            + "An empty result here would be indistinguishable from an empty index."
                    )
                );
        // Readers read blocks; they do not download segments. The store type is set here rather than in
        // the descriptor because it is a property of how this node is serving the shard, not of the index.
        openAndStart(onBlockCache(indexMetadata), shardId, manifest.term(), view.nodes(), false, manifest);
        readers.add(shardId);
        // What it is a cache of. A reader never changes commit while it is open -- a ReadOnlyEngine is
        // opened on one and core offers no way to move it -- so the only way to notice the commit has
        // moved on is to remember which one this is and compare.
        readerCommits.put(shardId, manifest);
        return shardId;
    }

    /** The commit each reader was opened at, so a pass can tell when it has gone stale. */
    private final java.util.Map<ShardId, CommitManifest> readerCommits = new ConcurrentHashMap<>();

    /**
     * Returns the commit a reader is serving.
     *
     * @param shardId the shard
     * @return the commit it was opened at, or empty if it is not a reader
     */
    public Optional<CommitManifest> readerCommit(ShardId shardId) {
        return Optional.ofNullable(readerCommits.get(shardId));
    }

    /**
     * Opens one shard of a frozen view, refusing a view taken over an earlier index of the same name.
     *
     * <p>A view records the uuid of the index it froze. The name it was taken under may since have been
     * deleted and created again, and the new index's first segments have the same names as the old one's
     * did — so a view opened by name against the new index would serve the new index's bytes under the old
     * view's id, or fail on a length mismatch if it was lucky. The uuid is what the view is a view of; a
     * view whose uuid is not the index's is no longer held, and is refused as such.
     *
     * @param indexMetadata the index the view was taken over, as it stands now
     * @param pit the view
     * @param shardNumber the shard
     * @param nodes the node view the shard is started against
     * @return the shard id the view was opened under
     * @throws IOException if the shard cannot be opened
     * @throws IllegalArgumentException if the view is of a different incarnation of the name, or did not
     *     freeze this shard
     */
    public ShardId openFrozenReader(IndexMetadata indexMetadata, PointInTime pit, int shardNumber, DiscoveryNodes nodes)
        throws IOException {
        if (pit.indexUuid() != null && pit.indexUuid().equals(indexMetadata.getIndexUUID()) == false) {
            throw new IllegalArgumentException(
                "the point in time ["
                    + pit.id()
                    + "] was taken over an earlier index named ["
                    + pit.index()
                    + "] that has since been deleted; it is no longer held"
            );
        }
        final CommitManifest manifest = pit.shards().get(shardNumber);
        if (manifest == null) {
            throw new IllegalArgumentException("the point in time [" + pit.id() + "] did not freeze shard " + shardNumber);
        }
        return openFrozenReader(indexMetadata, pit.id(), shardNumber, manifest, nodes);
    }

    /**
     * Opens a shard at a commit that is not the current one, under an identity of its own.
     *
     * <p><b>Why a separate identity.</b> A node holds at most one shard per {@code ShardId}, and it may
     * well already be serving the live one — the whole point of a frozen view is to read it while writing
     * continues. So the view is opened as an index whose uuid is the view's identifier, which makes it a
     * different shard as far as everything below here is concerned. The bytes still live under the real
     * index's uuid, which is why the shard carries
     * {@link org.opensearch.serverless.shell.ServerlessNode#STORAGE_UUID_SETTING}.
     *
     * <p><b>And why the manifest is passed in rather than read.</b> Reading it here would read the current
     * one, which is the thing this exists not to do.
     *
     * @param indexMetadata the index this is a view of
     * @param viewId the view's identifier, which becomes the synthetic index uuid
     * @param shardNumber the shard
     * @param manifest the commit to open
     * @param nodes the node view the shard is started against
     * @return the shard id the view was opened under
     * @throws IOException if the shard cannot be opened
     */
    public ShardId openFrozenReader(
        IndexMetadata indexMetadata,
        String viewId,
        int shardNumber,
        CommitManifest manifest,
        DiscoveryNodes nodes
    ) throws IOException {
        final org.opensearch.core.index.Index viewIndex = new org.opensearch.core.index.Index(indexMetadata.getIndex().getName(), viewId);
        final ShardId shardId = new ShardId(viewIndex, shardNumber);
        if (open.containsKey(shardId)) {
            return shardId;
        }
        // Built from the given metadata rather than from a fresh builder, so the per-shard primary terms
        // survive. A fresh builder gave every shard but this one a term of zero, and opening the second
        // shard of a view then rewrote the first one's term downwards -- "term is only increased as part
        // of primary promotion", from a code path that never promotes anything.
        final IndexMetadata frozen = IndexMetadata.builder(indexMetadata)
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put(IndexMetadata.SETTING_INDEX_UUID, viewId)
                    .put("index.store.type", org.opensearch.serverless.shell.ServerlessNode.BLOCK_CACHE_STORE_TYPE)
                    .put(org.opensearch.serverless.shell.ServerlessNode.STORAGE_UUID_SETTING, indexMetadata.getIndexUUID())
                    .build()
            )
            .build();
        openAndStart(frozen, shardId, manifest.term(), nodes, false, manifest);
        readers.add(shardId);
        // A view serves a commit like any other reader, so it records which one. It is exempt from the
        // staleness pass because it is a view, not because it forgot -- and the difference matters: the
        // exemption was at first protected only by this line's absence, which is an accident rather than a
        // guarantee and the canary for it could not fail.
        readerCommits.put(shardId, manifest);
        frozenViews.add(shardId);
        return shardId;
    }

    /**
     * Closes a frozen view, releasing the shards it opened and deleting what they left on disk.
     *
     * <p>A view's shards live under the view's own uuid, so nothing shares their directory and nothing
     * will ever open it again: the record it served is gone or expired. Left behind, every point in time
     * ever taken cost a directory — index and translog — for the life of the node's disk.
     *
     * @param viewId the view's identifier
     * @return how many shards were closed
     */
    public int closeFrozenReader(String viewId) {
        int closed = 0;
        for (ShardId shardId : Set.copyOf(frozenViews)) {
            if (shardId.getIndex().getUUID().equals(viewId) == false) {
                continue;
            }
            // A shard a query is still paging through stays a view, so the next pass finds it and closes
            // it then; forgetting it here would leave it open and uncounted for the life of the process.
            if (releaseShard(shardId, "the frozen view it served was released", true)) {
                frozenViews.remove(shardId);
                closed++;
            }
        }
        return closed;
    }

    private final Set<ShardId> frozenViews = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * Returns the shards open here as readers.
     *
     * @return the reader shard ids
     */
    public Set<ShardId> readerShards() {
        return Set.copyOf(readers);
    }

    /**
     * Removes every local file the published commit names, and every local commit.
     *
     * <p>Local disk is this node's own output and it may be stale: the counter that mints segment names
     * travels in the commit, so a successor restoring this node's last published commit mints the same
     * next name this node minted for a segment it flushed and never published. A directory that preferred
     * the local copy would serve those bytes under the successor's manifest. Everything the manifest names
     * is therefore read from the object store, and every local {@code segments_N} goes so the history
     * bootstrap starts from the published commit rather than from a local one that happens to be newer.
     * Whatever else is local and unreferenced, Lucene removes on open.
     */
    private static void dropLocalFilesNamedBy(org.apache.lucene.store.Directory directory, CommitManifest manifest) throws IOException {
        for (String name : directory.listAll()) {
            final boolean commit = name.startsWith("segments_") || name.startsWith("pending_segments_");
            if (commit == false && manifest.files().containsKey(name) == false) {
                continue;
            }
            try {
                // The block-cache directory ignores a delete of a name that is remote only, so this
                // removes exactly the local copies and leaves the published files where they are.
                directory.deleteFile(name);
            } catch (java.nio.file.NoSuchFileException | java.io.FileNotFoundException ignored) {
                // remote-only, which is the state we want
            }
        }
    }

    /**
     * Gives a restored commit a fresh, empty translog.
     *
     * <p>Restoring segments is not enough on its own: recovery reads a translog whose UUID matches the
     * commit, and a node taking a shard over has neither. Without this the shard fails recovery with a
     * corrupt-translog error naming a file that was never written.
     *
     * <p>The sequence is the one {@code StoreRecovery} uses for snapshot restore, for the same reason —
     * segments arrived from somewhere other than a peer, so the history has to be re-established rather
     * than continued. Operations the previous writer had accepted but not committed are replayed from the
     * write-ahead log, never from a local translog: this deletes the local one, on every open, which is
     * why fsyncing it per operation buys nothing.
     */
    private void bootstrapTranslogFor(IndexShard shard) throws IOException {
        shard.store().bootstrapNewHistory();
        final org.apache.lucene.index.SegmentInfos segmentInfos = shard.store().readLastCommittedSegmentsInfo();
        final long localCheckpoint = Long.parseLong(
            segmentInfos.userData.get(org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY)
        );
        final String translogUUID = segmentInfos.getUserData().get(org.opensearch.index.translog.Translog.TRANSLOG_UUID_KEY);
        org.opensearch.index.translog.Translog.createEmptyTranslog(
            shard.shardPath().resolveTranslog(),
            shard.shardId(),
            localCheckpoint,
            shard.getPendingPrimaryTerm(),
            translogUUID,
            java.nio.channels.FileChannel::open
        );
    }

    /**
     * Marks a query as running against a shard, so a release waits for it.
     *
     * <p>Bracketed with {@link #exit} around every local {@code ShardQuery.execute}. A pass that found a
     * reader's commit superseded used to close it under a running aggregation; the query failed with a
     * closed reader, which looks like corruption and is a scheduling choice.
     *
     * @param shardId the shard
     */
    public void enter(ShardId shardId) {
        inFlight.computeIfAbsent(shardId, ignored -> new java.util.concurrent.atomic.AtomicInteger()).incrementAndGet();
    }

    /**
     * Marks a query against a shard as finished.
     *
     * @param shardId the shard
     */
    public void exit(ShardId shardId) {
        final java.util.concurrent.atomic.AtomicInteger count = inFlight.get(shardId);
        if (count != null && count.decrementAndGet() <= 0) {
            inFlight.remove(shardId, count);
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
    private boolean releaseShard(ShardId shardId, String reason, boolean deleteStore) {
        final int busy = inFlight(shardId);
        // Readers only. A writer being released has lost its head, or is about to give it up, and a
        // writer that stays open past that point would go on acknowledging writes nobody will replay; a
        // query against it failing on a closed reader is the lesser harm, and the one this always risked.
        if (busy > 0 && readers.contains(shardId)) {
            logger.debug(
                "not releasing {} ({}): {} quer{} still running against it",
                shardId,
                reason,
                busy,
                busy == 1 ? "y is" : "ies are"
            );
            return false;
        }
        readers.remove(shardId);
        readerCommits.remove(shardId);
        walCache.remove(shardId);
        publisherCache.remove(shardId);
        lastUsed.remove(shardId);
        writeGuards.remove(shardId);
        final IndexShard shard = open.remove(shardId);
        final Index index = shardId.getIndex();
        final IndexService indexService = indicesService.indexService(index);
        boolean removed = false;
        if (indexService != null && indexService.hasShard(shardId.id())) {
            indexService.removeShard(shardId.id(), reason);
            removed = true;
        }
        if (shard == null && removed == false) {
            return true;
        }
        closeIndexIfUnused(index, reason, deleteStore);
        return true;
    }

    /** Closes the index's service once nothing of ours is open or opening under it, and evicts its monitor. */
    private void closeIndexIfUnused(Index index, String reason, boolean deleteStore) {
        while (true) {
            final Object lock = indexLocks.computeIfAbsent(index, ignored -> new Object());
            synchronized (lock) {
                if (indexLocks.get(index) != lock) {
                    continue;
                }
                final boolean inUse = open.keySet().stream().anyMatch(id -> id.getIndex().equals(index))
                    || opening.stream().anyMatch(id -> id.getIndex().equals(index));
                if (inUse == false) {
                    // DELETED wipes the directory; NO_LONGER_ASSIGNED closes and keeps it. A view's
                    // directory is never opened again, a live shard's is a cache the next open uses.
                    indicesService.removeIndex(
                        index,
                        deleteStore ? IndexRemovalReason.DELETED : IndexRemovalReason.NO_LONGER_ASSIGNED,
                        reason
                    );
                    indexLocks.remove(index, lock);
                }
                return;
            }
        }
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
     * Applies a changed mapping to whatever this node already has open for an index.
     *
     * <p><b>Why this is needed at all, rather than letting the next reconcile pass do it.</b> The pass opens
     * shards that are missing and closes shards that are no longer ours; it has never had a reason to notice
     * that an index it already holds says something different than it did. A mapping update is the first
     * change to an index that a node must apply to a shard it is already serving, and the node that accepted
     * the update is the one node guaranteed to be holding stale metadata the instant it succeeds.
     *
     * <p>Core does the applying: {@code IndexService#updateMapping} merges under
     * {@code MergeReason.MAPPING_RECOVERY} and gates on the mapping version, which is why the descriptor
     * carries one. A node with nothing open for this index has nothing to do — it will read the new
     * descriptor when it next opens a shard, which is the only moment the mapping matters to it.
     *
     * @param indexName the index whose mapping changed
     * @param descriptor the index as it now stands
     * @throws IOException if core rejects the new mapping
     */
    public void refreshMapping(String indexName, org.opensearch.serverless.cluster.IndexDescriptor descriptor) throws IOException {
        final Index index = openShards().stream()
            .map(ShardId::getIndex)
            .filter(each -> each.getName().equals(indexName))
            .findFirst()
            .orElse(null);
        if (index == null) {
            return;
        }
        final IndexService indexService = indicesService.indexService(index);
        if (indexService == null) {
            return;
        }
        if (indexService.getMetadata().getMappingVersion() >= descriptor.mappingVersion()) {
            // Already holding this mapping or a newer one. Core asserts that a mapping re-applied at the
            // same version is byte-identical to what it holds, and the descriptor's rendering is not.
            return;
        }
        indexService.updateMapping(indexService.getMetadata(), descriptor.toIndexMetadata(java.util.Map.of()));
    }

    /**
     * Applies changed settings to whatever this node already has open for an index.
     *
     * <p>The counterpart of {@link #refreshMapping}, and needed for the same reason: the reconcile pass opens
     * and closes shards, and has never had a reason to notice that an index it already holds says something
     * different than it did. Core does the applying — {@code IndexService#updateMetadata} pushes the new
     * settings into {@code IndexSettings}, notifies each shard, and re-times the refresh and fsync tasks —
     * and it gates on the settings version, which is why the descriptor carries one.
     *
     * @param indexName the index whose settings changed
     * @param descriptor the index as it now stands
     * @throws IOException if the new metadata cannot be built
     */
    public void refreshSettings(String indexName, org.opensearch.serverless.cluster.IndexDescriptor descriptor) throws IOException {
        final Index index = openShards().stream()
            .map(ShardId::getIndex)
            .filter(each -> each.getName().equals(indexName))
            .findFirst()
            .orElse(null);
        if (index == null) {
            return;
        }
        final IndexService indexService = indicesService.indexService(index);
        if (indexService == null) {
            return;
        }
        if (indexService.getMetadata().getSettingsVersion() >= descriptor.settingsVersion()) {
            return;
        }
        // The store type is how this node serves the index, not a property of the index; it travels with
        // the refreshed settings so the service's settings keep saying what its directory is.
        indexService.updateMetadata(indexService.getMetadata(), onBlockCache(descriptor.toIndexMetadata(java.util.Map.of())));
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
