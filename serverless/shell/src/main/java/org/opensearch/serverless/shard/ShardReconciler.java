/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
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
 */
public final class ShardReconciler {

    private final IndicesService indicesService;
    private final DiscoveryNode localNode;
    private final Map<ShardId, IndexShard> open = new ConcurrentHashMap<>();
    private final Set<ShardId> readers = ConcurrentHashMap.newKeySet();
    private volatile java.util.function.Function<ShardId, SegmentPublisher> publishers;
    private volatile java.util.function.Function<ShardId, org.opensearch.serverless.store.WalStore> walStores;
    private final Map<ShardId, org.opensearch.serverless.store.WalStore> walCache = new ConcurrentHashMap<>();

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
        // in the translog, which this does not upload.
        shard.flush(new org.opensearch.action.admin.indices.flush.FlushRequest().force(true).waitIfOngoing(true));
        // Named, so a manifest says who wrote it. A term is not an identity: the publish fence refuses a
        // newer term and cannot tell two nodes holding the same one apart.
        final CommitManifest manifest = publishers.apply(shardId).publish(shard.store(), term, localNode.getId());
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
            open.put(shardId, openAndStart(indexMetadata, shardId, assignment.term(), view.nodes(), true, false));
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

    /** One monitor per index, guarding the create-or-update of its {@link IndexService}. */
    private final java.util.concurrent.ConcurrentHashMap<Index, Object> indexLocks = new java.util.concurrent.ConcurrentHashMap<>();

    private IndexShard openAndStart(
        IndexMetadata indexMetadata,
        ShardId shardId,
        long shardHeadTerm,
        DiscoveryNodes nodes,
        boolean replayWal,
        boolean lazy
    ) throws IOException {
        return openAndStart(indexMetadata, shardId, shardHeadTerm, nodes, replayWal, lazy, null);
    }

    /**
     * @param knownCommit the commit to recover from, when the caller already knows it; null to ask the
     *     publisher for the current one. A frozen view has to pass it, for two reasons: the commit it
     *     wants is by definition not the current one, and its synthetic index uuid has no manifest
     *     register of its own, so asking would answer "nothing published" and start an empty shard --
     *     which is a wrong answer that looks exactly like a right one.
     */
    private IndexShard openAndStart(
        IndexMetadata indexMetadata,
        ShardId shardId,
        long shardHeadTerm,
        DiscoveryNodes nodes,
        boolean replayWal,
        boolean lazy,
        CommitManifest knownCommit
    ) throws IOException {
        final Index index = indexMetadata.getIndex();
        final IndexService indexService;
        // Locked per index, because this is a check-then-act and shards of one index now open at the same
        // time. Sequentially it was safe by luck; the moment the search fan-out became concurrent, three
        // shards of one index raced here and two lost with ResourceAlreadyExistsException -- reported as
        // two unreachable shards, which reads like a routing problem and is not one. IndexService.createShard
        // is itself synchronized, so only this acquisition needs guarding and shard opening stays parallel.
        synchronized (indexLocks.computeIfAbsent(index, ignored -> new Object())) {
            IndexService existing = indicesService.indexService(index);
            if (existing == null) {
                existing = indicesService.createIndex(indexMetadata, Collections.emptyList(), false);
                existing.updateMapping(null, indexMetadata);
            } else {
                // The IndexService is created once per index but shards arrive one at a time, so by the
                // time the second shard opens its metadata is stale -- in particular its per-shard primary
                // terms. A shard created from stale metadata starts at the old term and then gets
                // updateShardState at the new one, which the data plane rejects: "term is only increased
                // as part of primary promotion". Found by the first multi-shard test; every earlier one
                // had a single shard.
                existing.updateMetadata(existing.getMetadata(), indexMetadata);
            }
            indexService = existing;
        }

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

        final IndexShard shard = indexService.createShard(
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

        if (restoring) {
            if (lazy == false) {
                // A writer needs a local, writable copy it can merge into, so it downloads. Between
                // createShard and recovery: the store exists but nothing has opened it yet. Using
                // EMPTY_STORE here instead would call Store#createEmpty and delete exactly this.
                publisher.restoreInto(shard.store().directory(), shardId);
            }
            // A reader skips the download entirely: its directory already sees the published files and
            // fetches the blocks a query touches. Both still need a translog, because recovery reads
            // one and a node taking a shard over has none.
            bootstrapTranslogFor(shard);
        }

        shard.markAsRecovering("serverless-store", new RecoveryState(initializing, localNode, null));
        final PlainActionFuture<Boolean> recovered = PlainActionFuture.newFuture();
        shard.recoverFromStore(recovered);
        if (Boolean.TRUE.equals(recovered.actionGet()) == false) {
            throw new IOException("recovery from store reported failure for " + shardId);
        }

        final ShardRouting started = initializing.moveToStarted();
        // The applying version is the SHARD-HEAD TERM, not the projected view's version. S1 reproduced
        // what happens when a per-node counter is used here: the update is silently ignored, because
        // ReplicationTracker gates on > against a value that may have crossed a node boundary.
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
            // Everything the previous writer acknowledged but never published. Applied after the shard
            // is STARTED because that is when it accepts writes; replay is idempotent, so applying a
            // record that was already in the restored commit changes nothing.
            final var wal = wal(shardId);
            int replayed = 0;
            for (org.opensearch.serverless.store.WalRecord record : wal.replayable()) {
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
                shard.sync();
                shard.refresh("serverless-wal-replay");
            }
        }
        return shard;
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
        final CommitManifest manifest = publishers.apply(shardId)
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
        // the descriptor because it is a property of how this node is serving the shard, not of the
        // index -- the same index has writers that need a local, writable copy.
        final IndexMetadata lazyMetadata = IndexMetadata.builder(indexMetadata)
            .settings(
                org.opensearch.common.settings.Settings.builder()
                    .put(indexMetadata.getSettings())
                    .put("index.store.type", org.opensearch.serverless.shell.ServerlessNode.BLOCK_CACHE_STORE_TYPE)
                    .build()
            )
            .build();
        open.put(shardId, openAndStart(lazyMetadata, shardId, manifest.term(), view.nodes(), false, true));
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
        open.put(shardId, openAndStart(frozen, shardId, manifest.term(), nodes, false, true, manifest));
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
     * Closes a frozen view, releasing the shards it opened.
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
            releaseShard(shardId, "the frozen view it served was released");
            frozenViews.remove(shardId);
            closed++;
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
     * Gives a restored commit a fresh, empty translog.
     *
     * <p>Restoring segments is not enough on its own: recovery reads a translog whose UUID matches the
     * commit, and a node taking a shard over has neither. Without this the shard fails recovery with a
     * corrupt-translog error naming a file that was never written.
     *
     * <p>The sequence is the one {@code StoreRecovery} uses for snapshot restore, for the same reason —
     * segments arrived from somewhere other than a peer, so the history has to be re-established rather
     * than continued. Operations the previous writer had accepted but not committed are lost here; that
     * is what a write-ahead log is for, and this phase does not have one (see phase 4's notes).
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
     * Closes one shard. This is the <em>only</em> way a shard closes, and the caller must have learned
     * from a shard-head that this node no longer owns it.
     *
     * @param shardId the shard to release
     * @param reason why, for the log
     */
    public void releaseShard(ShardId shardId, String reason) {
        readers.remove(shardId);
        readerCommits.remove(shardId);
        walCache.remove(shardId);
        lastUsed.remove(shardId);
        final IndexShard shard = open.remove(shardId);
        if (shard == null) {
            return;
        }
        final Index index = shardId.getIndex();
        final IndexService indexService = indicesService.indexService(index);
        if (indexService != null && indexService.hasShard(shardId.id())) {
            indexService.removeShard(shardId.id(), reason);
        }
        if (open.keySet().stream().noneMatch(id -> id.getIndex().equals(index))) {
            indicesService.removeIndex(index, IndexRemovalReason.NO_LONGER_ASSIGNED, reason);
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
