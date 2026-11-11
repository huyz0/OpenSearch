/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.NodeLogTakeover;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Settles a shard whose writer is gone or has let go: whether anything it acknowledged is still unpublished, in its
 * own write log or a node log, and when the shard can be forgotten. Split out of {@link MetadataPlane}, which keeps the
 * public entry points.
 */
final class ShardSettlement {

    private final BlobStore blobStore;
    private final BlobPath base;
    private final LongSupplier clock;
    private final ShardHeadStore heads;
    private final MetadataPlane plane;

    ShardSettlement(BlobStore blobStore, BlobPath base, LongSupplier clock, ShardHeadStore heads, MetadataPlane plane) {
        this.blobStore = blobStore;
        this.base = base;
        this.clock = clock;
        this.heads = heads;
        this.plane = plane;
    }

    /**
     * Cleans up after a dead owner of one shard without taking the shard, when nothing it wrote is unpublished.
     *
     * <p><b>Why.</b> A node that crashes leaves every shard it held named to it: the head, its claim, and a rollup mark
     * that keeps the shard from ever being ruled out of a wide search. The only cleanup there was, was a takeover --
     * open the shard, replay its log, hold it -- which put a dead node's few hundred shards, most of them idle and
     * fully published, on survivors already near their cap, and left the marks of anything nobody took forever. A
     * fleet that had crashed a few times could not rule 47,000 indices out of a search for next month.
     *
     * <p><b>How, safely.</b> The head is given back on the owner's behalf the way a takeover takes it -- the owner must
     * not be live, its lease is revoked, the term moves on -- and the older terms are sealed, so a writer that was
     * not as dead as it looked can no longer add history. Only then is the log compared with the published commit:
     * if nothing landed past what the commit covers, every acknowledged write is published, and the mark and the claim
     * go. Otherwise the shard is left unowned and marked, and the caller should have it taken and replayed. A commit
     * published before commits recorded how far into the log they reach cannot be judged by a listing, and counts as
     * needing a replay.
     *
     * @param indexName the index
     * @param shard the shard number
     * @param deadNodeId the owner believed dead
     * @return what was done
     * @throws IOException if the store cannot be read or written
     */
    public MetadataPlane.Settled settleAbandoned(String indexName, int shard, String deadNodeId) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(indexName);
        if (descriptor.isEmpty()) {
            return MetadataPlane.Settled.NOT_ABANDONED;
        }
        final String uuid = descriptor.get().uuid();
        final Optional<ShardHeadStore.Abandoned> abandoned = heads.releaseAbandoned(indexName, shard, deadNodeId);
        if (abandoned.isEmpty()) {
            return MetadataPlane.Settled.NOT_ABANDONED;
        }
        final ShardHead previous = abandoned.get().previous();
        if (previous.indexUuid() != null && previous.indexUuid().equals(uuid) == false) {
            // A head left by a deleted incarnation: nothing of this one to settle, and the old one's bytes are the
            // sweep's. Given back is all it needed.
            plane.forgetAssignment(deadNodeId, indexName, shard);
            return MetadataPlane.Settled.RELEASED_CLEAN;
        }
        final org.opensearch.serverless.store.WalStore log = plane.walStore(indexName, uuid, shard);
        log.fenceOlderTerms(abandoned.get().released().term());
        final Optional<org.opensearch.serverless.store.CommitManifest> commit = plane.segmentPublisher(indexName, uuid, shard)
            .readManifest();
        if (unpublished(indexName, uuid, shard, commit, log)
            || nodeLogHolds(abandoned.get().released().writers(), indexName, uuid, shard, abandoned.get().released().term(), commit)) {
            return MetadataPlane.Settled.NEEDS_REPLAY;
        }
        // Widened to the published commit as the mark goes: the dead writer's publishes skipped the rollup under it.
        plane.rollups()
            .clearOwnedOf(
                indexName,
                uuid,
                shard,
                previous.term(),
                commit.map(org.opensearch.serverless.store.CommitManifest::digest).orElse(null)
            );
        plane.forgetAssignment(deadNodeId, indexName, shard);
        return MetadataPlane.Settled.RELEASED_CLEAN;
    }

    /**
     * Clears the owned mark of a shard nobody owns any more, when everything it holds is published.
     *
     * <p>For a mark its owner gave back the head under but could not clear -- the clear failed, or the owner's lease
     * lapsed and it released without publishing. Nothing is fenced: a head with no owner has no writer to fence.
     *
     * @param indexName the index
     * @param shard the shard number
     * @param markedTerm the term the mark records
     * @return {@link MetadataPlane.Settled#RELEASED_CLEAN} if the mark was cleared, {@link MetadataPlane.Settled#NEEDS_REPLAY} if the log is ahead
     *     of the commit, {@link MetadataPlane.Settled#NOT_ABANDONED} if the shard has an owner or the index is gone
     * @throws IOException if the store cannot be read or written
     */
    public MetadataPlane.Settled settleReleased(String indexName, int shard, long markedTerm) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(indexName);
        if (descriptor.isEmpty()) {
            return MetadataPlane.Settled.NOT_ABANDONED;
        }
        final Optional<ShardHead> head = heads.read(indexName, shard);
        if (head.isEmpty() || head.get().ownerNodeId() != null) {
            return MetadataPlane.Settled.NOT_ABANDONED;
        }
        final String uuid = descriptor.get().uuid();
        final Optional<org.opensearch.serverless.store.CommitManifest> commit = plane.segmentPublisher(indexName, uuid, shard)
            .readManifest();
        if (unpublished(indexName, uuid, shard, commit, plane.walStore(indexName, uuid, shard))
            || nodeLogHolds(head.get().writers(), indexName, uuid, shard, head.get().term(), commit)) {
            return MetadataPlane.Settled.NEEDS_REPLAY;
        }
        // A writer that takes the shard meanwhile marks it at a higher term first, which this clear leaves alone. Widened
        // to the published commit as the mark goes, as in settleAbandoned.
        plane.rollups()
            .clearOwnedOf(
                indexName,
                uuid,
                shard,
                markedTerm,
                commit.map(org.opensearch.serverless.store.CommitManifest::digest).orElse(null)
            );
        return MetadataPlane.Settled.RELEASED_CLEAN;
    }

    /**
     * Whether the shard's log holds a write its published commit does not cover. A commit that records how far into
     * the log it reaches is judged by a listing; an older one by its commit point's highest sequence number against
     * the log's, as a reader judges it; one that records neither counts as behind.
     */
    boolean unpublished(String indexName, String uuid, int shard, org.opensearch.serverless.store.WalStore log) throws IOException {
        return unpublished(indexName, uuid, shard, plane.segmentPublisher(indexName, uuid, shard).readManifest(), log);
    }

    /** The same, judged against a manifest the caller has already read. */
    boolean unpublished(
        String indexName,
        String uuid,
        int shard,
        Optional<org.opensearch.serverless.store.CommitManifest> commit,
        org.opensearch.serverless.store.WalStore log
    ) throws IOException {
        final org.opensearch.serverless.store.SegmentPublisher publisher = plane.segmentPublisher(indexName, uuid, shard);
        if (commit.isEmpty()) {
            return log.landedPast(0L, -1L);
        }
        if (commit.get().walOrdinal() >= 0L) {
            return log.landedPast(commit.get().term(), commit.get().walOrdinal());
        }
        final java.util.OptionalLong committed = publisher.committedMaxSeqNo(commit.get());
        return committed.isEmpty() || log.maxSeqNoFromTerm(commit.get().term()) > committed.getAsLong();
    }

    /**
     * Whether any of the given nodes holds records of a shard in its write log that a published commit does not cover.
     *
     * <p>A record is covered when its sequence number is at or below the commit's local checkpoint. With no commit to
     * judge against, any record the log names counts. The answer errs towards a replay: a log is judged by its blob
     * headers alone, and a commit that records no checkpoint covers nothing.
     *
     * @param nodeIds the nodes, as {@link ShardHead#writers} names them, or a dead owner
     * @param indexName the index
     * @param uuid the index's uuid
     * @param shard the shard number
     * @param term the term recorded in a fence, for diagnosis
     * @param commit the shard's published commit, if any
     * @return true if any of their logs may hold records of the shard the commit does not cover
     * @throws IOException if a lease, a log or the commit cannot be read, or a fence fails
     */
    public boolean nodeLogHolds(
        List<String> nodeIds,
        String indexName,
        String uuid,
        int shard,
        long term,
        Optional<org.opensearch.serverless.store.CommitManifest> commit
    ) throws IOException {
        java.util.OptionalLong covered = null;
        final NodeLogBlob.ShardKey key = new NodeLogBlob.ShardKey(uuid, shard);
        for (String nodeId : nodeIds) {
            final NodeLogTakeover reader = nodeLogReader(nodeId, term);
            if (reader == null || reader.holds(key) == false) {
                continue;
            }
            if (commit.isEmpty()) {
                return true;
            }
            final long held = reader.maxSeqNoOf(key);
            if (held == org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO) {
                continue;
            }
            if (covered == null) {
                covered = plane.segmentPublisher(indexName, uuid, shard).committedLocalCheckpoint(commit.get());
            }
            if (covered.isEmpty() || held > covered.getAsLong()) {
                return true;
            }
        }
        return false;
    }

    /**
     * A reader of a node's write log, or null if its lease names no epochs.
     *
     * <p>A node that is live let go of the shard itself and writes no more of it, so its log is read up to each epoch's
     * first free slot and nothing is fenced: fencing would fail the writes of every shard it still holds. A node that is
     * not live may not be as dead as it looks, and may still land records of the shard in an epoch it opened while it
     * held it, so its epochs are fenced first, as a takeover fences them.
     */
    NodeLogTakeover nodeLogReader(String nodeId, long term) throws IOException {
        if (nodeId == null) {
            return null;
        }
        final Optional<NodeLease> lease = plane.membership().read(nodeId);
        final List<NodeLogEpoch> epochs = lease.map(NodeLease::nodeLog).orElse(List.of());
        if (epochs.isEmpty()) {
            return null;
        }
        final NodeLogStore log = new NodeLogStore(blobStore, base, nodeId);
        final NodeLogTakeover.LeaseReader reread = () -> plane.membership().read(nodeId).map(NodeLease::nodeLog).orElse(List.of());
        return isLive(lease.get())
            ? NodeLogTakeover.unfenced(log, epochs, reread)
            : new NodeLogTakeover(log, epochs, "settled-by-survivor", term, reread);
    }

    /**
     * Deletes the write log of a node that is not live, once every shard it names has published past what it holds.
     *
     * <p><b>Why.</b> A lease that names epochs of a write log is never pruned, since it is the only place a successor
     * finds them, and nothing else deletes a dead node's blobs: without this both stay for ever.
     *
     * <p><b>When it is safe.</b> Every epoch is fenced first, so the node, if it is not as dead as it looks, can add
     * nothing after what is read. Each shard the last headers name is then judged: an index deleted or recreated
     * needs nothing; otherwise its published commit's local checkpoint must reach the highest sequence number this log holds of it,
     * which is read from blob headers alone. A shard whose new owner has not yet replayed and published keeps the log.
     * Only then does the lease stop naming the epochs, by a swap that a node coming back wins, and the blobs go.
     *
     * <p>A blob left behind by a delete that fails, or above a second fence, costs storage, not records: the
     * bucket's lifecycle rule on the log prefix removes it.
     *
     * @param nodeId the node
     * @return what was done
     * @throws IOException if the store cannot be read or written
     */
    public MetadataPlane.NodeLogCollected collectNodeLog(String nodeId) throws IOException {
        final Optional<NodeLease> lease = plane.membership().read(nodeId);
        if (lease.isEmpty() || lease.get().nodeLog().isEmpty() || isLive(lease.get())) {
            return MetadataPlane.NodeLogCollected.NOTHING;
        }
        final List<NodeLogEpoch> epochs = lease.get().nodeLog();
        final NodeLogStore log = new NodeLogStore(blobStore, base, nodeId);
        final NodeLogTakeover reader = new NodeLogTakeover(
            log,
            epochs,
            "collected-by-survivor",
            0L,
            () -> plane.membership().read(nodeId).map(NodeLease::nodeLog).orElse(List.of())
        );
        for (NodeLogBlob.ShardKey shard : reader.named()) {
            if (publishedPast(nodeId, shard, reader) == false) {
                return MetadataPlane.NodeLogCollected.STILL_NEEDED;
            }
        }
        final Map<Long, Long> ends = reader.ends();
        if (plane.membership().forgetNodeLog(nodeId, epochs) == false) {
            return MetadataPlane.NodeLogCollected.STILL_NEEDED;
        }
        for (NodeLogEpoch epoch : epochs) {
            // From the trim point the lease recorded: the node deleted below it itself, up to a renewal's worth.
            log.deleteRange(epoch.epoch(), epoch.trimPoint(), ends.getOrDefault(epoch.epoch(), epoch.landed()) + 1);
        }
        return MetadataPlane.NodeLogCollected.COLLECTED;
    }

    /** Whether a shard's published commit holds everything a node's log holds of it. */
    boolean publishedPast(String nodeId, NodeLogBlob.ShardKey shard, NodeLogTakeover reader) throws IOException {
        if (shard.indexName() == null) {
            return false;
        }
        final Optional<IndexDescriptor> descriptor = plane.describe(shard.indexName());
        if (descriptor.isEmpty() || descriptor.get().uuid().equals(shard.indexUuid()) == false) {
            // Deleted, or recreated under the name: nothing of this incarnation is read again.
            return true;
        }
        final Optional<ShardHead> head = heads.read(shard.indexName(), shard.shard());
        if (head.isPresent() && nodeId.equals(head.get().ownerNodeId())) {
            // Still the dead node's own: settled or taken first, and judged then.
            return false;
        }
        final long held = reader.maxSeqNoOf(shard);
        if (held == org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO) {
            return true;
        }
        final org.opensearch.serverless.store.SegmentPublisher publisher = plane.segmentPublisher(
            shard.indexName(),
            shard.indexUuid(),
            shard.shard()
        );
        final Optional<org.opensearch.serverless.store.CommitManifest> commit = publisher.readManifest();
        if (commit.isEmpty()) {
            return false;
        }
        final java.util.OptionalLong committed = publisher.committedLocalCheckpoint(commit.get());
        return committed.isPresent() && committed.getAsLong() >= held;
    }

    /**
     * Whether a lease names a node that is live now: neither revoked nor expired on this plane's clock.
     *
     * @param lease the lease
     * @return true if the node it names is live
     */
    public boolean isLive(NodeLease lease) {
        return lease.revoked() == false && lease.isExpiredAt(clock.getAsLong()) == false;
    }

    /**
     * Takes an index out of its group's rollups if it holds nothing: no shard owned, published or logged to.
     *
     * <p>See {@link DigestRollups#removeIfNothing}. Every shard is checked here -- head, manifest, log -- and the
     * removal itself refuses an entry a writer has marked since, so a writer arriving at any point keeps its entry.
     *
     * @param indexName the index
     * @return true if the index had nothing and is no longer entered
     * @throws IOException if the store cannot be read or written
     */
    public boolean forgetIfNothing(String indexName) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(indexName);
        if (descriptor.isEmpty()) {
            return false;
        }
        final String uuid = descriptor.get().uuid();
        for (int shard = 0; shard < descriptor.get().numberOfShards(); shard++) {
            final Optional<ShardHead> head = heads.read(indexName, shard);
            if (head.isPresent() && head.get().ownerNodeId() != null) {
                return false;
            }
            if (plane.segmentPublisher(indexName, uuid, shard).readManifest().isPresent()) {
                return false;
            }
            if (plane.walStore(indexName, uuid, shard).landedPast(0L, -1L)) {
                return false;
            }
            if (head.isPresent() && nodeLogHolds(head.get().writers(), indexName, uuid, shard, head.get().term(), Optional.empty())) {
                return false;
            }
        }
        return plane.rollups().removeIfNothing(indexName, uuid);
    }

    /**
     * Gives an entry that records no digest the digest its shards' published commits carry, where nobody owns them.
     *
     * <p>For an index {@link #forgetIfNothing} must keep because its shards hold something: a commit, or writes still
     * in the log. A commit that carries a digest widens the entry at once; one published before commits carried
     * digests, or a log ahead of the commit, needs the shard replayed and published again, and those shards are
     * returned for the caller to replay.
     *
     * @param indexName the index
     * @return the shards that need a replay before the entry can rule anything out; empty if none, or if a shard is owned
     * @throws IOException if the store cannot be read or written
     */
    public List<Integer> backfillDigest(String indexName) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(indexName);
        if (descriptor.isEmpty()) {
            return List.of();
        }
        final String uuid = descriptor.get().uuid();
        final int shards = descriptor.get().numberOfShards();
        final List<Integer> replay = new ArrayList<>();
        for (int shard = 0; shard < shards; shard++) {
            final Optional<ShardHead> head = heads.read(indexName, shard);
            if (head.isPresent() && head.get().ownerNodeId() != null) {
                // Marked by its writer, so never ruled out; the writer's clear widens the entry when it lets go.
                return List.of();
            }
            if (unpublished(indexName, uuid, shard, plane.walStore(indexName, uuid, shard))
                || (head.isPresent()
                    && nodeLogHolds(
                        head.get().writers(),
                        indexName,
                        uuid,
                        shard,
                        head.get().term(),
                        plane.segmentPublisher(indexName, uuid, shard).readManifest()
                    ))) {
                replay.add(shard);
                continue;
            }
            final Optional<org.opensearch.serverless.store.CommitManifest> commit = plane.segmentPublisher(indexName, uuid, shard)
                .readManifest();
            if (commit.isEmpty()) {
                continue;
            }
            if (commit.get().digest().isEmpty()) {
                replay.add(shard);
            } else {
                plane.rollups().widen(indexName, uuid, shards, shard, commit.get().digest());
            }
        }
        return replay;
    }
}
