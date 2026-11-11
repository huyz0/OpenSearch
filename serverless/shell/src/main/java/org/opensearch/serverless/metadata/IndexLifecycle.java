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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Creates and deletes indices: the descriptor, the queued reclaim of a deleted incarnation, and the purge of its shard
 * data once no snapshot or point in time still pins it. Split out of {@link MetadataPlane}, which keeps the public
 * entry points.
 */
final class IndexLifecycle {

    private final BlobStore blobStore;
    private final BlobPath base;
    private final LongSupplier clock;
    private final DescriptorStore descriptors;
    private final ShardHeadStore heads;
    private final ReclaimQueue reclaimQueue;
    private final AliasMutations aliases;
    private final MetadataPlane plane;

    IndexLifecycle(
        BlobStore blobStore,
        BlobPath base,
        LongSupplier clock,
        DescriptorStore descriptors,
        ShardHeadStore heads,
        ReclaimQueue reclaimQueue,
        AliasMutations aliases,
        MetadataPlane plane
    ) {
        this.blobStore = blobStore;
        this.base = base;
        this.clock = clock;
        this.descriptors = descriptors;
        this.heads = heads;
        this.reclaimQueue = reclaimQueue;
        this.aliases = aliases;
        this.plane = plane;
    }

    /**
     * Creates an index. One put-if-absent, whose cost does not depend on how many indices already exist.
     *
     * @param descriptor the index to create
     * @return the descriptor register's generation
     * @throws IndexAlreadyExistsException if the name is taken
     * @throws IOException if the write fails
     */
    public long createIndex(IndexDescriptor descriptor) throws IOException {
        // The longest shard key includes the UUID and the largest shard number. Refuse an unusable
        // layout before acknowledging its descriptor, without creating directories or probing the store.
        blobStore.validateKey(
            RegisterMap.shardData(base, descriptor.name(), descriptor.uuid(), descriptor.numberOfShards() - 1),
            org.opensearch.serverless.store.SegmentPublisher.MANIFEST
        );
        // No clearing of the path here, and that is the point of putting the uuid in it: a uuid is minted
        // per create, so a new index's storage is somewhere no previous index of that name ever wrote.
        // Emptiness is a property of the path rather than of a sweep having finished.
        return descriptors.create(descriptor);
    }

    /**
     * Removes every byte a shard wrote: its segments, its manifests and its log — except a shard a live
     * shallow snapshot is still pinning.
     *
     * <p><b>A shallow snapshot names these bytes directly rather than a copy of them.</b> Its whole
     * argument for costing nothing at capture time is that a captured commit's blobs are the same ones the
     * live shard already wrote. Deleting the index unconditionally
     * would delete exactly those blobs out from under a snapshot nobody had touched, which is not a corner
     * case: taking a snapshot and then deleting the index it was taken from is the normal shape a backup
     * is used for. A standard (non-shallow) snapshot already copied its own bytes into the repository's own
     * storage at capture time, so it needs no such check — this only ever changes behaviour for a shallow
     * one.
     *
     * <p>The check costs one listing of every live snapshot in the deployment, done once per delete rather
     * than once per shard, and only when at least one snapshot exists anywhere — the same conditional cost
     * {@link org.opensearch.serverless.reconcile.GarbageCollector} already pays for the identical reason.
     * A shard this skips is not leaked forever: it becomes ordinary orphaned storage the moment its last
     * pinning snapshot is deleted, reclaimable the same way any other orphan is (see "the whole-deployment
     * orphan sweep" in {@code serverless-status.md}'s open items).
     *
     * @param indexName the index
     * @param uuid the index's uuid, which a live snapshot's pin is keyed on
     * @param shards how many shards it has
     */
    void purgeShardData(String indexName, String uuid, int shards) throws IOException {
        final List<SnapshotRecord> snapshots = plane.liveSnapshots();
        // And the frozen views, read once per delete: a point in time names a shard's files the same way a
        // shallow snapshot does, and deleting the index used to remove them before the orphan sweep -- which
        // does honour views -- ever saw them. A view the collector could not read pins everything, for the
        // reason livePointsInTime gives.
        final List<PointInTime> views = plane.livePointsInTime(clock.getAsLong());
        for (int shard = 0; shard < shards; shard++) {
            if (views.isEmpty() == false && pinnedByView(views, indexName, uuid, shard)) {
                MetadataPlane.LOGGER.info(
                    "leaving shard " + shard + " of " + indexName + " on disk: a live point in time still names its files directly"
                );
                continue;
            }
            if (snapshots.isEmpty() == false && pinnedBy(snapshots, uuid, shard)) {
                MetadataPlane.LOGGER.info(
                    "leaving shard "
                        + shard
                        + " of "
                        + indexName
                        + " on disk: a live snapshot still names its blobs directly, "
                        + "and deleting the index must not delete what a snapshot is holding"
                );
                continue;
            }
            try {
                blobStore.blobContainer(RegisterMap.shardData(base, indexName, uuid, shard)).delete();
            } catch (Exception e) {
                // Best effort, and it has to be: this is called on paths that may not exist at all, and a
                // store that cannot delete must not turn creating an index into a failure. What it costs
                // when it fails is storage, which is recoverable, against a create that is not.
                MetadataPlane.LOGGER.warn("could not clear storage for shard " + shard + " of " + indexName, e);
            }
        }
    }

    static boolean pinnedByView(List<PointInTime> views, String indexName, String uuid, int shard) {
        for (PointInTime view : views) {
            if (view.isPlaceholder()) {
                return true;
            }
            if (view.pins(indexName, uuid) == false) {
                continue;
            }
            // A freeze in progress pins every shard of the index it names: it has read none of them yet
            // and is about to read all of them. See GarbageCollector#pinnedByView, which mirrors this, and
            // pinnedBy, which has had the same branch for a snapshot capture all along.
            if (view.isCapturing() || view.shards().containsKey(shard)) {
                return true;
            }
        }
        return false;
    }

    static boolean pinnedBy(List<SnapshotRecord> snapshots, String uuid, int shard) {
        for (SnapshotRecord snapshot : snapshots) {
            if (snapshot.referencedBlobs(uuid, shard).isEmpty() == false) {
                return true;
            }
            for (SnapshotRecord.SnapshottedIndex index : snapshot.indices().values()) {
                // A capture still in flight has named the index and not yet its commits: it is reading
                // those manifests now, or copying the blobs they name. Everything of the index is held
                // until the record is finished -- see capturing() for why that is the same as a pin.
                if (index.uuid().equals(uuid) && capturing(index) && shard < index.numberOfShards()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Reports whether a snapshot record describes a capture that has not finished.
     *
     * <p>A snapshot's record is written <em>before</em> its manifests are read, naming every index it will
     * capture and none of their commits, and rewritten with the commits once every one has been read (and,
     * for a standard repository, copied). The record in between is the provisional one, and it is what
     * makes a concurrent index deletion leave the shard data alone: without it, a delete landing after the
     * manifests were read and before the record existed purged the very blobs a {@code SUCCESS} snapshot
     * then named, and the first restore was the first anyone heard of it.
     *
     * <p>Told apart by shape rather than by a flag: a finished capture records every shard of every index
     * it names or leaves the index out, so an index with a shard count and no shards can only be one still
     * being taken.
     *
     * @param index an index entry of a snapshot record
     * @return true while the capture of that index has not finished
     */
    public static boolean capturing(SnapshotRecord.SnapshottedIndex index) {
        return index.shards().isEmpty() && index.numberOfShards() > 0;
    }

    /**
     * Reports whether a snapshot is still being taken.
     *
     * @param snapshot the record
     * @return true if any index it names is still being captured
     */
    public static boolean inProgress(SnapshotRecord snapshot) {
        for (SnapshotRecord.SnapshottedIndex index : snapshot.indices().values()) {
            if (capturing(index)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Deletes an index and every shard-head belonging to it.
     *
     * <p>Read the descriptor and heads first, then order the descriptor away with compare-and-swap before
     * removing the captured heads. Conditional head cleanup uses their captured generations, and data
     * cleanup uses the captured UUID, to preserve a replacement index.
     *
     * @param indexName the index to delete
     * @return true if the index existed
     * @throws IOException if the delete fails
     */
    public boolean deleteIndex(String indexName) throws IOException {
        return deleteIndex(indexName, null);
    }

    /**
     * Deletes only the incarnation the caller selected, preserving a replacement under the same name.
     *
     * @param indexName the index name
     * @param expectedUuid the selected UUID, or null to select the current incarnation
     * @return true if the selected incarnation was deleted
     * @throws IOException if deletion fails
     */
    public boolean deleteIndex(String indexName, String expectedUuid) throws IOException {
        // The descriptor and the generation it was read at, from one read: read separately, a change
        // landing between the two handed the delete a fresh generation for a stale descriptor.
        final DescriptorStore.Resolution resolved = descriptors.resolve(indexName);
        final Optional<IndexDescriptor> descriptor = Optional.ofNullable(resolved.index());
        if (descriptor.isEmpty() || (expectedUuid != null && expectedUuid.equals(descriptor.get().uuid()) == false)) {
            return false;
        }
        final boolean conditional = descriptors.conditionalDelete();
        // This incarnation's heads, read once: whether any shard has a writer decides when the bytes are
        // purged, and they are deleted below at exactly the generations read here.
        final List<ShardHeadStore.HeadAt> owned = conditional
            ? heads.readAllOf(indexName, descriptor.get().numberOfShards(), descriptor.get().uuid())
            : List.of();
        final boolean hadWriter = owned.stream().anyMatch(head -> head.head().ownerNodeId() != null);
        if (conditional) {
            // Before anything is removed, so no crash after this point can leave heads or bytes that
            // nothing names. A delete that then loses leaves an intent naming a live index, which the
            // reclaimer drops. See ReclaimQueue.
            reclaimQueue.enqueue(
                new ReclaimQueue.Intent(indexName, descriptor.get().uuid(), descriptor.get().numberOfShards(), hadWriter),
                clock.getAsLong()
            );
        }
        // Order the delete against any concurrent lifecycle change before removing anything. If the
        // descriptor moved underneath us, someone else is mid-operation and this delete loses.
        if (descriptors.deleteIfUnchanged(indexName, resolved.generation(), clock.getAsLong()) == false) {
            return false;
        }
        // Heads only after the descriptor is ordered away: the shard count needed to enumerate them
        // lives in the descriptor, and it is bounded by IndexDescriptor.MAX_SHARDS, so this is a
        // bounded loop rather than a listing. Where the store deletes conditionally, only this
        // incarnation's heads and only as they were read: the name is free from the moment the
        // descriptor went, and a head deleted by name could be a new incarnation's (see deleteAllOf).
        if (conditional) {
            heads.deleteIfUnchanged(owned);
        } else {
            heads.releaseAllOf(indexName, descriptor.get().numberOfShards(), descriptor.get().uuid());
        }
        // The aliases that named it stop naming it, as core's delete does. Left in place, the alias
        // record still listed the name while the new index of that name (created with an empty hint) said
        // nothing named it -- GET /_alias/x and a search through x included the newcomer, GET /i/_alias
        // said {} -- which is exactly the under-approximation the hint is documented never to make.
        aliases.detachFromAliases(descriptor.get());
        // And the bytes. Heads first, so a writer cannot renew and the node holding it closes the shard on
        // its next tick; the data after, so a deleted index stops being paid for.
        //
        // <b>The residual race is real, and it leaks rather than resolving itself.</b> Publishing is fenced
        // by the manifest register's term rather than by the head, so a writer that has lost its head can
        // still complete a publish it had already begun, and leave blobs behind this sweep.
        //
        // They are unreachable, which is the part that holds: no descriptor and no head, under a uuid no
        // later index can be minted with, so nothing serves them and no answer is wrong because of them.
        //
        // What is NOT true -- this comment used to say it was -- is that the next index of this name
        // clears them. It cannot: createIndex deliberately clears nothing, because the uuid in the path
        // means a new index of the same name writes somewhere else entirely, where these blobs are neither
        // in the way nor visible. So they are storage nothing automatic reclaims: the per-shard sweep runs
        // only for shards of live indices, and GarbageCollector#collectOrphanedShards, which would find
        // them, is the operator's sweep and is on no loop. See serverless/STATUS.md.
        //
        // Where the delete is conditional and a shard had a writer, not now: that writer may yet publish
        // before it notices, and purging now would only have to be done again once it has stopped. The
        // reclaim does it then, once. With no writer there is nothing to wait for, and the bytes go now.
        if (conditional == false || hadWriter == false) {
            purgeShardData(indexName, descriptor.get().uuid(), descriptor.get().numberOfShards());
        }
        return true;
    }

    /**
     * Finishes a delete whose intent has fallen due: removes whatever outlived it.
     *
     * <p>Every step is safe to repeat and safe against a recreated name. An intent naming an index that
     * still exists under that uuid is a delete that never happened, and is dropped. Otherwise the deleted
     * incarnation's heads are removed conditionally and only while they name its uuid -- a writer that had
     * not noticed the delete may have re-created one -- and its bytes are purged again, since that writer
     * may have completed a publish after the first purge. The path of those bytes carries the uuid, which no
     * later index can be minted with.
     *
     * @param intent the due intent
     * @throws IOException if the store cannot be reached; the intent is kept and tried again
     */
    public void reclaim(ReclaimQueue.Intent intent) throws IOException {
        final DescriptorStore.Resolution current = descriptors.resolve(intent.name());
        if (current.index() != null && intent.uuid().equals(current.index().uuid())) {
            return;
        }
        // A head of the dead uuid found now was re-created by a writer that missed the delete, which may
        // have published too; that is the one sign a delete with no writer at the time left anything behind.
        final int reappeared = heads.deleteAllOf(intent.name(), intent.shards(), intent.uuid());
        if (intent.hadWriter() || reappeared > 0) {
            purgeShardData(intent.name(), intent.uuid(), intent.shards());
        }
    }
}
