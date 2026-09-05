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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.PointInTime;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Reclaims segment blobs no longer referenced by a shard's published commit.
 *
 * <p>Most of what this collects is a zombie's work. A writer that lost its lease and kept going wrote
 * real files under its own term container, could never publish them, and left them there — inert, but
 * not free. Merges leave the same kind of residue: a segment superseded by a later commit stops being
 * referenced the moment the manifest moves on.
 *
 * <p><b>The rule, and it is the whole safety argument:</b> a blob is deleted only when both hold —
 *
 * <ol>
 *   <li>it lives in a term container <em>strictly older</em> than the published manifest's term, and</li>
 *   <li>nothing names it: not the published manifest, not any frozen view somebody may still be
 *       paging through, and not any snapshot.</li>
 * </ol>
 *
 * <p>Condition 1 exists because publishing is not atomic: a writer uploads files and <em>then</em>
 * swaps the manifest, so at the current term there is a window in which live files are not yet
 * referenced. Deleting them would break an in-flight publish. Older terms have no such window, because
 * a writer at an older term can no longer publish at all — that is what the fence in
 * {@link SegmentPublisher} guarantees.
 *
 * <p>Condition 2 exists because a failover <em>inherits</em> files rather than re-uploading them
 * (phase 4), so files a live commit depends on routinely live in older term containers. Collecting by
 * term alone would delete exactly the data the current writer is serving. That is the mistake this
 * class is shaped to make impossible, and the one its tests are pointed at.
 *
 * <p><b>A pin that cannot be read pins everything.</b> The plane stands in for a view record it could
 * not read with a placeholder that names no shards. Adding its (empty) references to the set and
 * sweeping on would be treating "unreadable" as "holds nothing", which is the opposite of what the
 * placeholder means; a sweep that sees one deletes nothing and says so.
 */
public final class GarbageCollector {

    private static final Logger logger = LogManager.getLogger(GarbageCollector.class);

    /**
     * The floor a production sweep should set: a blob first seen unreferenced less than this long ago is
     * kept whatever the pass count says.
     *
     * <p>The two-sweep grace is counted in passes, and a pass is however long the loop takes to come
     * round -- under a burst of publishes, two passes can be a second apart. Freezing a view reads one
     * manifest per shard and then writes the record; a two-hundred-shard freeze on a slow store takes
     * longer than that, and the files its first shards froze were swept before the record existed. A
     * floor in wall-clock time is what makes "two sweeps" mean "long enough".
     */
    public static final long DEFAULT_MINIMUM_UNREFERENCED_MILLIS = 60_000L;

    private final BlobStore blobStore;
    private final org.opensearch.common.blobstore.BlobPath base;
    /** Per {@code index#shard}, when each candidate was first seen unreferenced, in the plane's clock. */
    private final Map<String, Map<String, Long>> firstSeenUnreferenced = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile long minimumUnreferencedMillis = 0L;

    /**
     * Creates a collector.
     *
     * @param blobStore the backing store
     * @param base the deployment's base path
     */
    public GarbageCollector(BlobStore blobStore, org.opensearch.common.blobstore.BlobPath base) {
        this.blobStore = blobStore;
        this.base = base;
    }

    /**
     * Sets how long a blob must have been unreferenced, by the plane's clock, before a graced sweep may
     * delete it -- on top of the pass count the caller keeps.
     *
     * <p>Zero, the default, keeps the grace a pure pass count. The floor is remembered on this instance,
     * so a caller that wants it must keep one collector across passes rather than build one per pass; a
     * fresh collector has never seen anything and would start every clock again.
     *
     * @param millis the floor; see {@link #DEFAULT_MINIMUM_UNREFERENCED_MILLIS}
     * @return this, for chaining
     */
    public GarbageCollector setMinimumUnreferencedMillis(long millis) {
        this.minimumUnreferencedMillis = Math.max(0L, millis);
        return this;
    }

    /**
     * Collects unreferenced segment blobs for one shard.
     *
     * @param plane the metadata plane, for the shard's manifest
     * @param indexName the index
     * @param shardId the shard number
     * @return the blob names deleted, qualified by their term container
     * @throws IOException if listing or deleting fails
     */
    public List<String> collectShard(MetadataPlane plane, String indexName, int shardId) throws IOException {
        // No grace: everything unreferenced goes now. Right for a sweep an operator asked for, and for
        // one that follows a deletion, where there is nothing left to be reading.
        return sweepShard(plane, indexName, shardId, null).deleted();
    }

    /**
     * Collects one shard with the two-sweep grace, remembering between calls what it saw unreferenced.
     *
     * <p>{@link #collectShard(MetadataPlane, String, int)} deletes on first sight, which is right after a
     * deletion and wrong for a sweep over live indices: a reader on another node serves the previous
     * commit until its next backstop, and its files are exactly what the first sight finds unreferenced.
     * A caller that keeps the memory across sweeps — a scheduled whole-deployment sweep — gets the same
     * grace the per-node sweep has: a blob goes only when two consecutive sweeps found it unreferenced.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shardId the shard number
     * @param memory what earlier sweeps saw unreferenced, keyed by {@code index#shard}; updated in place.
     *     Null for no grace.
     * @return the blob names deleted, qualified by their term container
     * @throws IOException if listing or deleting fails
     */
    public List<String> collectShard(MetadataPlane plane, String indexName, int shardId, Map<String, Set<String>> memory)
        throws IOException {
        if (memory == null) {
            return collectShard(plane, indexName, shardId);
        }
        final String key = indexName + RegisterMap.SHARD_SEPARATOR + shardId;
        final ShardSweep sweep = sweepShard(plane, indexName, shardId, memory.getOrDefault(key, Set.of()));
        if (sweep.candidates().isEmpty()) {
            memory.remove(key);
        } else {
            memory.put(key, sweep.candidates());
        }
        return sweep.deleted();
    }

    /**
     * What one sweep of a shard did, and what it is watching.
     *
     * @param deleted the blobs deleted, qualified by term container
     * @param candidates the blobs that were unreferenced this time, which a later sweep may delete
     */
    public record ShardSweep(List<String> deleted, Set<String> candidates) {
    }

    /**
     * Sweeps one shard, deleting only what was already unreferenced last time.
     *
     * <p><b>Why a blob has to be unreferenced twice before it goes.</b> A reader is a cache of one commit,
     * and it goes on serving that commit until a reconcile pass notices the commit has moved and lets it
     * go. In between, files the current commit no longer names are still being read — and those files are
     * exactly what the rule in this class marks as collectable. Sweeping on a schedule would turn that
     * into a reader failing mid-query, which looks like corruption and is not.
     *
     * <p>So a blob must be seen unreferenced by two consecutive sweeps of the same shard. The interval
     * between them is the grace, and it is set by whatever drives the sweep — which must be at least the
     * interval on which readers refresh, or the grace is not one.
     *
     * <p><b>The candidate set is deliberately in memory and deliberately per-owner.</b> A node that has
     * just taken the shard over starts with nothing, so its first sweep deletes nothing: a new owner
     * cannot know how long a blob has been unreferenced, and assuming "long enough" is the one assumption
     * that loses data. Forgetting always delays a deletion and never causes one.
     *
     * @param plane the metadata plane, for the shard's manifest
     * @param indexName the index
     * @param shardId the shard number
     * @param previousCandidates what the last sweep of this shard found unreferenced, or null to delete
     *     everything unreferenced now
     * @return what was deleted, and what to pass in next time
     * @throws IOException if listing or deleting fails
     */
    public ShardSweep sweepShard(MetadataPlane plane, String indexName, int shardId, Set<String> previousCandidates) throws IOException {
        return sweepShard(
            plane,
            indexName,
            shardId,
            previousCandidates,
            plane.livePointsInTime(plane.clock().getAsLong()),
            plane.liveSnapshots()
        );
    }

    /**
     * Sweeps one shard against views and snapshots the caller has already read.
     *
     * <p>Read before this is called and before this lists anything, which keeps the safety rule the
     * per-shard form documents: a view or snapshot taken while a sweep runs is seen or not seen, never
     * half-seen. What changes is who pays for the read -- once per pass rather than once per shard.
     *
     * @param plane the metadata plane
     * @param indexName the index
     * @param shardId the shard number
     * @param previousCandidates what a previous sweep saw unreferenced, or null for everything
     * @param views every live view
     * @param snapshots every live snapshot
     * @return what was deleted and what is now a candidate
     * @throws IOException if a listing or a delete fails
     */
    public ShardSweep sweepShard(
        MetadataPlane plane,
        String indexName,
        int shardId,
        Set<String> previousCandidates,
        List<PointInTime> views,
        List<SnapshotRecord> snapshots
    ) throws IOException {
        for (PointInTime pit : views) {
            if (pit.isPlaceholder()) {
                // A record the plane could not read. What it holds is unknowable, so nothing is deleted
                // while it stands; what was on watch stays on watch, so the grace is not restarted when
                // the record is repaired or removed.
                logger.warn(
                    "not sweeping shard {} of {}: the point in time [{}] could not be read and may hold any of its files",
                    shardId,
                    indexName,
                    pit.id()
                );
                return new ShardSweep(List.of(), previousCandidates == null ? Set.of() : Set.copyOf(previousCandidates));
            }
        }

        final SegmentPublisher publisher = plane.segmentPublisher(indexName, shardId);
        final Optional<CommitManifest> manifest = publisher.readManifest();
        if (manifest.isEmpty()) {
            // Nothing published means nothing is safe to judge: a writer may be mid-first-publish, and
            // every file present is a candidate for the commit it is about to make.
            return new ShardSweep(List.of(), Set.of());
        }
        final long liveTerm = manifest.get().term();

        // Referenced as (term container, file name) pairs. A name alone is not enough: the same segment
        // name can exist in two term containers after a history bootstrap, and only one is referenced.
        final Set<String> referenced = new HashSet<>();
        for (Map.Entry<String, String> file : manifest.get().files().entrySet()) {
            referenced.add(file.getValue() + "/" + file.getKey());
        }

        // The index's uuid, read only when something to check against it exists: a snapshot pins by uuid,
        // and a view that recorded one is matched by it rather than by a name that may since have been
        // reused. A deployment with neither pays nothing extra for this.
        String uuid = null;
        boolean uuidNeeded = snapshots.isEmpty() == false;
        for (PointInTime pit : views) {
            uuidNeeded |= pit.index().equals(indexName) && pit.indexUuid() != null;
        }
        if (uuidNeeded) {
            uuid = plane.describe(indexName).map(IndexDescriptor::uuid).orElse(null);
        }

        // And what any frozen view is still holding.
        //
        // <b>This is the second half of the safety rule and it is not optional.</b> The sweep deletes a
        // blob when it is unreferenced by the live commit and belongs to a dead term -- which is exactly
        // what the files of a point in time are, a few seconds after the writer publishes again. Without
        // this, a caller paging through a frozen view would find it dissolving underneath them, and the
        // failure would look like corruption rather than like a deletion.
        //
        // Read once per sweep of a shard rather than once per blob, and read *before* the listing below:
        // a view taken while this sweep is running is one whose files this sweep may already have listed
        // as orphans, so it must be seen first or not at all.
        for (PointInTime pit : views) {
            if (pit.pins(indexName, uuid)) {
                referenced.addAll(pit.referencedBlobs(shardId));
            }
        }

        // And what any snapshot is still holding -- the same rule, checked once per sweep of a shard
        // rather than once per blob, and read before the listing for the same reason the point-in-time
        // read above is: a snapshot taken while this sweep is running must be seen or not seen, never
        // half-seen. Keyed by the index's uuid rather than its name -- see SnapshotRecord#referencedBlobs.
        if (uuid != null) {
            for (SnapshotRecord snapshot : snapshots) {
                referenced.addAll(snapshot.referencedBlobs(uuid, shardId));
                for (SnapshotRecord.SnapshottedIndex captured : snapshot.indices().values()) {
                    if (captured.uuid().equals(uuid) && capturing(captured) && captured.numberOfShards() > shardId) {
                        // A capture that has named this index and not yet read its manifests. Which commit
                        // it will record is not known until it has, so every file of this shard may be one
                        // it is about to name; nothing goes until the record is finalised.
                        return new ShardSweep(List.of(), previousCandidates == null ? Set.of() : Set.copyOf(previousCandidates));
                    }
                }
            }
        }

        final List<String> deleted = new ArrayList<>();
        final Set<String> candidates = new HashSet<>();
        // The wall-clock half of the grace, applied only to a graced sweep: an ungraced one is an operator
        // or a deletion, where there is nothing left to be reading.
        final String shardKey = indexName + RegisterMap.SHARD_SEPARATOR + shardId;
        final Map<String, Long> firstSeen = previousCandidates == null
            ? null
            : firstSeenUnreferenced.computeIfAbsent(shardKey, ignored -> new java.util.concurrent.ConcurrentHashMap<>());
        final long now = plane.clock().getAsLong();
        final BlobContainer shardContainer = blobStore.blobContainer(plane.shardData(indexName, shardId));
        for (Map.Entry<String, BlobContainer> child : shardContainer.children().entrySet()) {
            final String termDir = child.getKey();
            final Long term = parseTerm(termDir);
            if (term == null || term >= liveTerm) {
                // Not a term container, or the live term, or a higher one. A higher term container means
                // a newer writer is mid-publish and about to become the truth; leave it entirely alone.
                continue;
            }
            final List<String> orphans = new ArrayList<>();
            for (String blobName : child.getValue().listBlobs().keySet()) {
                final String qualified = termDir + "/" + blobName;
                if (referenced.contains(qualified)) {
                    if (firstSeen != null) {
                        // Referenced again -- a view or snapshot now names it -- so its clock restarts if
                        // it ever becomes unreferenced once more.
                        firstSeen.remove(qualified);
                    }
                    continue;
                }
                candidates.add(qualified);
                if (previousCandidates == null) {
                    orphans.add(blobName);
                    continue;
                }
                final long since = firstSeen.computeIfAbsent(qualified, ignored -> now);
                if (previousCandidates.contains(qualified) && now - since >= minimumUnreferencedMillis) {
                    orphans.add(blobName);
                }
            }
            if (orphans.isEmpty() == false) {
                child.getValue().deleteBlobsIgnoringIfNotExists(orphans);
                for (String orphan : orphans) {
                    deleted.add(termDir + "/" + orphan);
                }
            }
        }
        // What was deleted is not watched any more; what survived is what the next sweep compares against.
        candidates.removeAll(deleted);
        if (firstSeen != null) {
            firstSeen.keySet().retainAll(candidates);
            if (firstSeen.isEmpty()) {
                firstSeenUnreferenced.remove(shardKey);
            }
        }
        return new ShardSweep(deleted, candidates);
    }

    /**
     * Sweeps every shard of every index, deleting on first sight.
     *
     * <p>Walks the whole deployment one page at a time via {@link #collectPage}. A sweep genuinely does
     * intend to visit everything, so being proportional to the population is correct here in a way it
     * never is on a request path — but it must still be resumable and sliceable, which is why the paged
     * form is the real one and this is a convenience over it.
     *
     * <p><b>No grace</b>, which is right for a sweep an operator runs once after a deletion and wrong for
     * one run on a schedule over live indices: see {@link #collectAll(MetadataPlane, Map)}.
     *
     * @param plane the metadata plane
     * @return blob names deleted, keyed by {@code index#shard}
     * @throws IOException if listing or deleting fails
     */
    public Map<String, List<String>> collectAll(MetadataPlane plane) throws IOException {
        return collectAll(plane, null);
    }

    /**
     * Sweeps every shard of every index, with the two-sweep grace carried in the caller's memory.
     *
     * <p>A reader on another node serves the commit it opened until its next backstop, so the files a
     * sweep first finds unreferenced may still be being read somewhere. A caller that keeps the memory
     * from one sweep to the next deletes a blob only once two consecutive sweeps found it unreferenced,
     * which is the same grace {@code BackgroundReconciler} gives the shards it publishes.
     *
     * @param plane the metadata plane
     * @param memory what earlier sweeps saw unreferenced, keyed by {@code index#shard}, updated in place;
     *     null for no grace
     * @return blob names deleted, keyed by {@code index#shard}
     * @throws IOException if listing or deleting fails
     */
    public Map<String, List<String>> collectAll(MetadataPlane plane, Map<String, Set<String>> memory) throws IOException {
        final Map<String, List<String>> deleted = new java.util.LinkedHashMap<>();
        String after = null;
        do {
            after = collectPage(plane, after, 500, deleted, memory);
        } while (after != null);
        return deleted;
    }

    /**
     * Collects one bounded slice of the deployment, deleting on first sight.
     *
     * <p>This is the shape a sweep at target scale has to have, and {@link #collectAll} is a loop over
     * it kept for small deployments and tests. A real fleet runs many workers each taking a slice, which
     * is what {@code rfc-serverless-metadata-plane.md} §6 means by control logic being many small
     * idempotent loops: nothing here is privileged, and two workers collecting the same slice at once is
     * wasteful rather than wrong.
     *
     * @param plane the metadata plane
     * @param after resume point, or null to start
     * @param limit how many indices this slice covers
     * @param into accumulates deletions, keyed by {@code index#shard}
     * @return the cursor to resume from, or null when the sweep is complete
     * @throws IOException if listing or deleting fails
     */
    public String collectPage(MetadataPlane plane, String after, int limit, Map<String, List<String>> into) throws IOException {
        return collectPage(plane, after, limit, into, null);
    }

    /**
     * Collects one bounded slice of the deployment, with the two-sweep grace carried in the caller's memory.
     *
     * @param plane the metadata plane
     * @param after resume point, or null to start
     * @param limit how many indices this slice covers
     * @param into accumulates deletions, keyed by {@code index#shard}
     * @param memory what earlier sweeps saw unreferenced, keyed by {@code index#shard}; null for no grace
     * @return the cursor to resume from, or null when the sweep is complete
     * @throws IOException if listing or deleting fails
     */
    public String collectPage(MetadataPlane plane, String after, int limit, Map<String, List<String>> into, Map<String, Set<String>> memory)
        throws IOException {
        final var page = plane.descriptors().listPage(after, limit);
        for (Map.Entry<String, IndexDescriptor> index : page.descriptors().entrySet()) {
            for (int shard = 0; shard < index.getValue().numberOfShards(); shard++) {
                final List<String> orphans = collectShard(plane, index.getKey(), shard, memory);
                if (orphans.isEmpty() == false) {
                    into.put(index.getKey() + RegisterMap.SHARD_SEPARATOR + shard, orphans);
                }
            }
        }
        return page.nextAfter();
    }

    /**
     * Removes descriptor tombstones older than the quarantine.
     *
     * <p>A deleted index leaves its descriptor register as a tombstone so a later index of the same name
     * does not restart at the generation numbers the old one had — see
     * {@code DescriptorStore#deleteIfUnchanged}. Kept forever, they are read on every listing and count
     * against every prefix pattern's cap; a name created and deleted daily poisoned {@code logs-*} after a
     * year. A compare-and-swap carrying a generation read hours ago is not an in-flight operation, so a
     * tombstone that old has done its job.
     *
     * @param plane the metadata plane
     * @param nowMillis the plane's clock
     * @return the names whose tombstones were removed
     * @throws IOException if listing, reading or deleting fails
     */
    public List<String> collectTombstones(MetadataPlane plane, long nowMillis) throws IOException {
        return plane.descriptors().sweepTombstones(nowMillis, DescriptorStore.DEFAULT_TOMBSTONE_QUARANTINE_MILLIS);
    }

    /**
     * Names every pin in the deployment, cheaply enough to compare between passes.
     *
     * <p>The per-node sweep gate runs a shard's sweep when that node can see something became
     * collectable: a manifest it published lost a file, or a view it reaped freed one. What it cannot see
     * is a view reaped on another node or a snapshot deleted anywhere, and a quiet index held the files
     * such a pin had covered until its next merge. This is the fingerprint the gate compares: two
     * listings and no reads, one name per live view record and one per snapshot record. A pin that came
     * or went between two readings changes the set, and a changed set is the signal to sweep every open
     * writer once.
     *
     * @return the pin names, sorted, prefixed by kind
     * @throws IOException if a listing fails
     */
    public Set<String> pins() throws IOException {
        final Set<String> pins = new TreeSet<>();
        for (String name : blobStore.blobContainer(RegisterMap.pointsInTime(base)).listBlobs().keySet()) {
            if (PointInTime.looksLikeAnId(name)) {
                pins.add("view:" + name);
            }
        }
        for (String name : blobStore.blobContainer(RegisterMap.snapshots(base)).listBlobs().keySet()) {
            pins.add("snapshot:" + name);
        }
        return pins;
    }

    /**
     * Deletes the storage of shards no index owns any more, unless a snapshot or a view still holds it.
     *
     * <p><b>What leaves an orphan.</b> Publishing is fenced by the manifest register's term, not by the
     * shard-head, so a writer that has lost its head can still finish a publish it had already begun — and
     * land bytes after the delete that removed the index swept its container. Those bytes are under the
     * dead index's uuid, so nothing will ever read them and nothing will ever reuse that path. Without this
     * they are paid for forever.
     *
     * <p><b>Absent means absent.</b> A container is deleted only when the register for its index says the
     * index is not there, or is there under a different uuid. A register read that fails throws rather than
     * answering "absent", so a store having a bad minute cannot be mistaken for an index having been
     * deleted — which is the mistake that would turn a garbage collector into data loss.
     *
     * <p><b>Pinned is not orphaned.</b> Deleting an index deliberately leaves the shards a live shallow
     * snapshot references (see {@code MetadataPlane#purgeShardData}), and a view taken before the delete
     * still names its files; both look exactly like orphans to a sweep that consults only descriptors. The
     * same pins index deletion honours are honoured here, read once per sweep, and a view record that
     * cannot be read stops the sweep outright, since what it holds is unknowable.
     *
     * <p><b>A container this does not recognise is left alone.</b> Anything whose name is not
     * {@code index#uuid#shard} was not written by this system, and a sweep that deletes what it cannot
     * parse is a sweep that eventually deletes somebody else's bucket.
     *
     * <p><b>This lists every shard container in the deployment</b>, which is proportional to the population
     * and is the thing §6.3 forbids on a request path. It is allowed here for the same reason
     * {@link #collectAll} is: a sweep genuinely intends to visit everything, and nothing waits on it. What
     * it is not is resumable, unlike the per-shard sweep — the object-store listing API this is built on
     * offers children, not pages of them, so a deployment large enough for that to matter needs a listing
     * that can be sliced before this can be.
     *
     * @param plane the metadata plane
     * @return the container names deleted
     * @throws IOException if listing fails
     */
    public List<String> collectOrphanedShards(MetadataPlane plane) throws IOException {
        final List<SnapshotRecord> snapshots = plane.liveSnapshots();
        final List<PointInTime> views = plane.livePointsInTime(plane.clock().getAsLong());
        for (PointInTime pit : views) {
            if (pit.isPlaceholder()) {
                logger.warn("not sweeping orphaned shards: the point in time [{}] could not be read and may hold any of them", pit.id());
                return List.of();
            }
        }

        final BlobContainer segments = blobStore.blobContainer(base.add("segments"));
        final List<String> deleted = new ArrayList<>();
        for (Map.Entry<String, BlobContainer> child : segments.children().entrySet()) {
            final String container = child.getKey();
            // index#uuid#shard, split from the right, because an index name may contain the separator in
            // no version of this system but the uuid and shard cannot.
            final int lastSeparator = container.lastIndexOf(RegisterMap.SHARD_SEPARATOR);
            final int uuidSeparator = lastSeparator < 0 ? -1 : container.lastIndexOf(RegisterMap.SHARD_SEPARATOR, lastSeparator - 1);
            if (uuidSeparator <= 0) {
                continue;
            }
            final String indexName = container.substring(0, uuidSeparator);
            final String uuid = container.substring(uuidSeparator + 1, lastSeparator);
            final int shard;
            try {
                shard = Integer.parseInt(container.substring(lastSeparator + 1));
            } catch (NumberFormatException e) {
                continue;
            }

            final Optional<IndexDescriptor> descriptor = plane.describe(indexName);
            if (descriptor.isPresent() && descriptor.get().uuid().equals(uuid)) {
                continue;
            }
            if (pinnedBySnapshot(snapshots, uuid, shard) || pinnedByView(views, indexName, uuid, shard)) {
                logger.info("leaving {} in place: a live snapshot or point in time still names its blobs", container);
                continue;
            }
            child.getValue().delete();
            deleted.add(container);
        }
        return deleted;
    }

    private static boolean pinnedBySnapshot(List<SnapshotRecord> snapshots, String uuid, int shard) {
        for (SnapshotRecord snapshot : snapshots) {
            if (snapshot.referencedBlobs(uuid, shard).isEmpty() == false) {
                return true;
            }
            for (SnapshotRecord.SnapshottedIndex captured : snapshot.indices().values()) {
                // A capture in progress names the index and none of its shards yet: it is about to read
                // -- or copy from -- exactly this container, and must find it there.
                if (captured.uuid().equals(uuid) && capturing(captured) && captured.numberOfShards() > shard) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Whether a snapshot's index entry is a capture still being taken: a shard count and no shards. The
     * same reading {@code MetadataPlane.capturing} gives the record; kept here so the collector's safety
     * argument does not depend on the plane's.
     */
    private static boolean capturing(SnapshotRecord.SnapshottedIndex captured) {
        return captured.shards().isEmpty() && captured.numberOfShards() > 0;
    }

    private static boolean pinnedByView(List<PointInTime> views, String indexName, String uuid, int shard) {
        for (PointInTime pit : views) {
            if (pit.pins(indexName, uuid) && pit.shards().containsKey(shard)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Removes node leases that have expired.
     *
     * <p>Tidiness rather than correctness: an expired lease is already filtered out on read, so leaving
     * it changes no answer. It is collected because a deployment that has cycled through nodes for a
     * year should not have to list a year of dead ones to find the live few.
     *
     * @param plane the metadata plane
     * @param nowMillis the observer's clock
     * @return the node ids whose leases were removed
     * @throws IOException if listing or deleting fails
     */
    public List<String> collectExpiredLeases(MetadataPlane plane, long nowMillis) throws IOException {
        final BlobContainer members = blobStore.blobContainer(RegisterMap.members(base));
        final List<String> removed = new ArrayList<>();
        for (String blobName : members.listBlobsByPrefix(org.opensearch.serverless.membership.BlobLeaseMembership.LEASE_PREFIX).keySet()) {
            final String nodeId = blobName.substring(org.opensearch.serverless.membership.BlobLeaseMembership.LEASE_PREFIX.length());
            final var lease = plane.membership().read(nodeId);
            if (lease.isPresent() && lease.get().isExpiredAt(nowMillis)) {
                members.deleteBlobsIgnoringIfNotExists(List.of(blobName));
                removed.add(nodeId);
            }
        }
        return removed;
    }

    private static Long parseTerm(String containerName) {
        if (containerName.startsWith("t=") == false) {
            return null;
        }
        try {
            return Long.parseLong(containerName.substring(2));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
