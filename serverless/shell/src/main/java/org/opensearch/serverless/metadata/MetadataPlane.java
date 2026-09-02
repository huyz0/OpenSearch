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
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.membership.BlobLeaseMembership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * The whole truth layer behind one object: descriptors, shard-heads and node leases.
 *
 * <p>Nothing here consults a cluster-manager, and nothing here is published. Index creation is one
 * put-if-absent; shard activation is one compare-and-swap; membership is a listing. The register map is
 * in {@link RegisterMap}, and the reason it is a map rather than a single object is in that class's
 * documentation.
 *
 * <p><b>Decision D5:</b> everything here is exercised against {@code FsBlobContainer} only. Passing
 * there says nothing about whether a provider's conditional write is genuinely linearizable, which is
 * R11 and the thing the entire safety argument rests on. No durability claim is made for S3, GCS or
 * Azure until that conformance suite runs.
 */
public final class MetadataPlane {

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(MetadataPlane.class);

    private final DescriptorStore descriptors;
    private final ShardHeadStore heads;
    private final ClusterConfig clusterConfig;
    private final BlobLeaseMembership membershipField;
    private final BlobStore blobStore;
    private final BlobPath base;
    private final long leaseTtlMillis;
    private final LongSupplier clock;

    /**
     * Creates a metadata plane over a blob store.
     *
     * <p><b>Liveness comes from node leases. There is no longer another way.</b> Shard-heads used to be
     * able to carry their own renewable expiry, which cost a compare-and-swap per shard per tick — 34
     * implied S3 requests at eight shards against 18 — for a guarantee one node-lease renewal already
     * provides. Two modes meant two sets of failure behaviour to reason about and only one of them was
     * ever measured; the expensive one was also, for most of this project's life, the default.
     *
     * <p>The head still stamps an expiry and it is still load-bearing, as a <em>floor</em>: a head is held
     * if that stamp has not passed or its owner's lease says the owner is alive. The stamp covers the
     * moments between winning a head and publishing a lease — without it, two contenders holding no
     * leases both win — and the lease covers everything after it lapses. The stamp is never renewed,
     * which is what keeps one renewal covering every shard.
     *
     * @param blobStore the backing store
     * @param base the deployment's base path within it
     * @param clock source of wall-clock millis
     * @param leaseTtlMillis lease duration for shard-heads and node leases
     */
    public MetadataPlane(BlobStore blobStore, BlobPath base, LongSupplier clock, long leaseTtlMillis) {

        this.descriptors = new DescriptorStore(blobStore.blobContainer(RegisterMap.indices(base)));
        final BlobLeaseMembership leases = new BlobLeaseMembership(
            blobStore.blobContainer(RegisterMap.members(base)),
            clock,
            leaseTtlMillis
        );
        final LivenessOracle oracle = (nodeId, ephemeralId) -> {
            try {
                final var lease = leases.read(nodeId);
                // An ephemeral id that has moved on means the process that took the shard is gone, even
                // though a node with the same name is back. It does not inherit the claim.
                return lease.isPresent()
                    && lease.get().isExpiredAt(clock.getAsLong()) == false
                    && (ephemeralId == null || ephemeralId.equals(lease.get().ephemeralId()));
            } catch (java.io.IOException e) {
                // Unreadable is not "dead": refusing to acquire is the safe answer, since the cost is a
                // retry and the cost of the alternative is two writers.
                return true;
            }
        };
        this.heads = new ShardHeadStore(blobStore.blobContainer(RegisterMap.shards(base)), clock, leaseTtlMillis, oracle);
        this.clusterConfig = new ClusterConfig(blobStore.blobContainer(RegisterMap.clusterConfig(base)));
        this.membershipField = leases;
        this.blobStore = blobStore;
        this.base = base;
        this.leaseTtlMillis = leaseTtlMillis;
        this.clock = clock;
    }

    /**
     * Returns how long a lease stays valid after a renewal.
     *
     * <p>Exposed because the renewal schedule is derived from it and must not be guessed independently:
     * a renewal interval chosen without reference to the TTL is a lease that lapses under a node that
     * believes it is healthy.
     *
     * @return the lease TTL in milliseconds
     */
    public long leaseTtlMillis() {
        return leaseTtlMillis;
    }

    /**
     * Returns the clock this plane stamps and compares expiries with.
     *
     * @return the clock
     */
    public LongSupplier clock() {
        return clock;
    }

    /**
     * Returns the write-ahead log for one shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the WAL store
     */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, int shardId) throws IOException {
        return walStore(indexName, uuidOf(indexName), shardId);
    }

    /**
     * Returns the write-ahead log for one shard, for a caller that already knows the index's uuid.
     *
     * @param indexName the index
     * @param uuid the index's uuid
     * @param shardId the shard number
     * @return the WAL store
     */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, String uuid, int shardId) {
        return new org.opensearch.serverless.store.WalStore(blobStore, RegisterMap.shardData(base, indexName, uuid, shardId));
    }

    /**
     * Returns where one shard's bytes live.
     *
     * <p>Resolving the uuid costs a descriptor read, which is why every caller on a hot path takes the
     * overload that supplies one. This exists for the maintenance paths, where a read is already happening
     * and one more is not the cost that matters.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the container path
     * @throws IOException if the descriptor cannot be read
     */
    public BlobPath shardData(String indexName, int shardId) throws IOException {
        return RegisterMap.shardData(base, indexName, uuidOf(indexName), shardId);
    }

    private String uuidOf(String indexName) throws IOException {
        return descriptors.get(indexName)
            .orElseThrow(() -> new IOException("no such index: " + indexName + "; its storage cannot be located without its uuid"))
            .uuid();
    }

    /**
     * Returns the segment publisher for one shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the publisher
     */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, int shardId) throws IOException {
        return segmentPublisher(indexName, uuidOf(indexName), shardId);
    }

    /**
     * Returns the segment publisher for one shard, for a caller that already knows the index's uuid.
     *
     * @param indexName the index
     * @param uuid the index's uuid
     * @param shardId the shard number
     * @return the publisher
     */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, String uuid, int shardId) {
        return new org.opensearch.serverless.store.SegmentPublisher(blobStore, RegisterMap.shardData(base, indexName, uuid, shardId));
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
        // No clearing of the path here, and that is the point of putting the uuid in it: a uuid is minted
        // per create, so a new index's storage is somewhere no previous index of that name ever wrote.
        // Emptiness is a property of the path rather than of a sweep having finished.
        return descriptors.create(descriptor);
    }

    /**
     * Removes every byte a shard wrote: its segments, its manifests and its log.
     *
     * @param indexName the index
     * @param shards how many shards it has
     */
    private void purgeShardData(String indexName, String uuid, int shards) {
        for (int shard = 0; shard < shards; shard++) {
            try {
                blobStore.blobContainer(RegisterMap.shardData(base, indexName, uuid, shard)).delete();
            } catch (Exception e) {
                // Best effort, and it has to be: this is called on paths that may not exist at all, and a
                // store that cannot delete must not turn creating an index into a failure. What it costs
                // when it fails is storage, which is recoverable, against a create that is not.
                LOGGER.warn("could not clear storage for shard " + shard + " of " + indexName, e);
            }
        }
    }

    /**
     * Deletes an index and every shard-head belonging to it.
     *
     * <p>Heads first, then the descriptor. The order matters and the reason is worth stating: deleting
     * the descriptor first would leave heads that no reader can interpret, since the shard count needed
     * to enumerate them lives in the descriptor. Orphaned heads with no descriptor are the kind of
     * garbage that is discovered years later.
     *
     * @param indexName the index to delete
     * @return true if the index existed
     * @throws IOException if the delete fails
     */
    public boolean deleteIndex(String indexName) throws IOException {
        final long generation = descriptors.generationOf(indexName);
        final Optional<IndexDescriptor> descriptor = descriptors.get(indexName);
        if (descriptor.isEmpty()) {
            return false;
        }
        // Order the delete against any concurrent lifecycle change before removing anything. If the
        // descriptor moved underneath us, someone else is mid-operation and this delete loses.
        if (descriptors.deleteIfUnchanged(indexName, generation) == false) {
            return false;
        }
        // Heads only after the descriptor is ordered away: the shard count needed to enumerate them
        // lives in the descriptor, and it is bounded by IndexDescriptor.MAX_SHARDS, so this is a
        // bounded loop rather than a listing.
        heads.deleteAllFor(indexName, descriptor.get().numberOfShards());
        // And the bytes. Heads first, so a writer cannot renew and the node holding it closes the shard on
        // its next tick; the data after, so a deleted index stops being paid for.
        //
        // <b>The residual race is real and bounded.</b> Publishing is fenced by the manifest register's
        // term rather than by the head, so a writer that has lost its head can still complete a publish it
        // had already begun, and leave blobs behind this sweep. They are unreachable -- no descriptor, no
        // head -- and the next index of this name clears them on creation, which is why that is where the
        // correctness argument lives rather than here.
        purgeShardData(indexName, descriptor.get().uuid(), descriptor.get().numberOfShards());
        return true;
    }

    /**
     * Creates an alias standing for some indices.
     *
     * <p>Refused if the name is taken by an index or another alias, which the object store decides rather
     * than a check here — see {@link DescriptorStore#createAlias}.
     *
     * @param alias the alias to create
     * @return the generation the register now holds
     * @throws IOException if the write fails
     */
    public long createAlias(org.opensearch.serverless.cluster.AliasRecord alias) throws IOException {
        return descriptors.createAlias(alias);
    }

    /**
     * Reads what a name stands for: an index, an alias, or nothing.
     *
     * @param name the name
     * @return what it names
     * @throws IOException if the read fails
     */
    public DescriptorStore.Resolution resolve(String name) throws IOException {
        return descriptors.resolve(name);
    }

    /**
     * Removes an alias.
     *
     * <p>Nothing to clean up behind it: an alias owns no shards and no bytes, which is the difference
     * between deleting one and deleting an index.
     *
     * @param name the alias
     * @return true if there was one
     * @throws IOException if the delete fails
     */
    public boolean deleteAlias(String name) throws IOException {
        final long generation = descriptors.generationOf(name);
        if (descriptors.resolve(name).alias() == null) {
            return false;
        }
        return descriptors.deleteIfUnchanged(name, generation);
    }

    /**
     * Freezes the current commit of every shard of an index.
     *
     * <p>Written before it is used and read by the collector, which is the ordering that matters: a view
     * that existed only in the memory of the node that took it would be a promise the sweep never heard.
     *
     * @param pit the view to record
     * @throws IOException if the write fails
     */
    public void createPointInTime(PointInTime pit) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.pointsInTime(base));
        final var bytes = pit.toBytes();
        container.writeBlob(pit.id(), bytes.streamInput(), bytes.length(), true);
    }

    /**
     * Reads a frozen view.
     *
     * @param id the identifier
     * @return the view, or empty if it never existed or has been released
     * @throws IOException if the read fails
     */
    public java.util.Optional<PointInTime> pointInTime(String id) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.pointsInTime(base));
        if (container.blobExists(id) == false) {
            return java.util.Optional.empty();
        }
        try (java.io.InputStream in = container.readBlob(id)) {
            return java.util.Optional.of(PointInTime.fromStream(in));
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            // Released between the check and the read, which is ordinary rather than exceptional.
            return java.util.Optional.empty();
        }
    }

    /**
     * Releases a frozen view, so the collector stops holding its files.
     *
     * @param id the identifier
     * @return true if there was one
     * @throws IOException if the delete fails
     */
    public boolean releasePointInTime(String id) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.pointsInTime(base));
        if (container.blobExists(id) == false) {
            return false;
        }
        container.deleteBlobsIgnoringIfNotExists(java.util.List.of(id));
        return true;
    }

    /**
     * Reads every frozen view that has not expired.
     *
     * <p>For the collector, which has to know what is being held before it deletes anything. Expired ones
     * are skipped rather than deleted here: deciding they are gone and removing them are two different
     * jobs, and doing both in a method a sweep calls would mean a read path with a side effect.
     *
     * @param nowMillis the current time
     * @return the live views
     * @throws IOException if listing or reading fails
     */
    public java.util.List<PointInTime> livePointsInTime(long nowMillis) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.pointsInTime(base));
        final java.util.List<PointInTime> live = new java.util.ArrayList<>();
        for (String name : container.listBlobs().keySet()) {
            if (PointInTime.looksLikeAnId(name) == false) {
                // Not a name this system mints, so not a record of ours. Ignored rather than treated as a
                // pin: one stray object in this container -- a console upload, a backup tool, a test
                // filesystem's scratch file -- would otherwise turn off the garbage collector for the whole
                // deployment, and do it silently. Found by a test filesystem doing exactly that.
                LOGGER.warn("ignoring [" + name + "] in the points-in-time container: it is not a name this system writes");
                continue;
            }
            try (java.io.InputStream in = container.readBlob(name)) {
                final PointInTime pit = PointInTime.fromStream(in);
                if (pit.expiredAt(nowMillis) == false) {
                    live.add(pit);
                }
            } catch (Exception e) {
                // A view the collector cannot read is one it must assume is holding something, because the
                // alternative is deleting files somebody is paging through. A half-written record still has
                // the name we gave it, so this is the case that treatment exists for. Treated as live and
                // left for a human, rather than swept because it was unreadable.
                LOGGER.warn("could not read the point in time " + name + "; treating it as live", e);
                live.add(new PointInTime(name, "", Long.MAX_VALUE, java.util.Map.of()));
            }
        }
        return live;
    }

    /**
     * Removes frozen views that have expired.
     *
     * @param nowMillis the current time
     * @return how many were removed
     * @throws IOException if listing or deleting fails
     */
    public int reapPointsInTime(long nowMillis) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.pointsInTime(base));
        final java.util.List<String> expired = new java.util.ArrayList<>();
        for (String name : container.listBlobs().keySet()) {
            if (PointInTime.looksLikeAnId(name) == false) {
                // Not ours to read and not ours to delete. The sweep says so once, in the log, and leaves
                // it exactly where it is.
                continue;
            }
            try (java.io.InputStream in = container.readBlob(name)) {
                if (PointInTime.fromStream(in).expiredAt(nowMillis)) {
                    expired.add(name);
                }
            } catch (Exception e) {
                LOGGER.warn("could not read the point in time " + name + "; leaving it alone", e);
            }
        }
        if (expired.isEmpty() == false) {
            container.deleteBlobsIgnoringIfNotExists(expired);
        }
        return expired.size();
    }

    /**
     * The most names an index pattern may match before it is refused.
     *
     * <p><b>Chosen to keep a pattern to one request.</b> S3's {@code ListObjectsV2} returns at most a
     * thousand keys per round trip, and this asks for the cap plus one, so anything below a thousand is a
     * single listing. Five hundred leaves room and is already far past what a caller can do anything
     * useful with: a search fanning out over five hundred indices is not a search anybody debugs.
     */
    public static final int DEFAULT_PATTERN_CAP = 500;

    /**
     * Returns the index and alias names beginning with a prefix.
     *
     * @param prefix the prefix, empty for every name
     * @param cap the most names to return before refusing
     * @return the matching names, in lexicographic order
     * @throws DescriptorStore.TooManyMatchesException if more than {@code cap} match
     * @throws IOException if the listing fails
     */
    public java.util.List<String> namesWithPrefix(String prefix, int cap) throws IOException {
        return descriptors.namesWithPrefix(prefix, cap);
    }

    /**
     * Reads an index descriptor.
     *
     * @param indexName the index
     * @return the descriptor, or empty
     * @throws IOException if the read fails
     */
    public Optional<IndexDescriptor> describe(String indexName) throws IOException {
        return descriptors.get(indexName);
    }

    /**
     * Attempts to take ownership of a shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the acquiring node
     * @param ephemeralId the acquiring node's ephemeral id
     * @return the outcome, carrying the winner's head either way
     * @throws IOException if the register cannot be read or written
     */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId) throws IOException {
        final Acquisition acquisition = heads.acquire(indexName, shardId, nodeId, ephemeralId);
        if (acquisition.acquired()) {
            // Record the claim so this node can find it again without reading the world. Written after
            // the compare-and-swap, never before: if the process dies in between, the node simply does
            // not see the shard on its next read and re-acquires, which self-heals. Writing it first
            // would instead advertise a claim that was never won.
            blobStore.blobContainer(RegisterMap.assignments(base, nodeId))
                .writeBlob(RegisterMap.assignmentBlob(indexName, shardId), new java.io.ByteArrayInputStream(new byte[0]), 0L, false);
        }
        return acquisition;
    }

    /**
     * Forgets a node's claim on a shard. Idempotent, and safe to skip — a stale entry costs one wasted
     * head read, because every entry is verified before it is believed.
     *
     * @param nodeId the node
     * @param indexName the index
     * @param shardId the shard number
     * @throws IOException if the delete fails
     */
    public void forgetAssignment(String nodeId, String indexName, int shardId) throws IOException {
        blobStore.blobContainer(RegisterMap.assignments(base, nodeId))
            .deleteBlobsIgnoringIfNotExists(java.util.List.of(RegisterMap.assignmentBlob(indexName, shardId)));
    }

    /**
     * Reads everything one node needs in order to serve.
     *
     * <p>This is the loop that replaces cluster-state publication. It returns only the indices this node
     * owns a shard of, so a node's residency tracks its working set rather than the size of the world.
     *
     * @param nodeId the node to read truth for
     * @return that node's descriptors and shard assignments
     * @throws IOException if the object store cannot be read
     */
    public Truth truthFor(String nodeId) throws IOException {
        final Map<String, IndexDescriptor> hosted = new LinkedHashMap<>();
        final List<ShardAssignment> assignments = new ArrayList<>();

        // One listing of this node's own claims, rather than a sweep of every index in the deployment.
        // Phase 9 measured the difference: the old form cost 2N+1 operations in the population, on the
        // steady-state path, on every node -- the exact O(population) shape this design exists to remove.
        for (String claim : blobStore.blobContainer(RegisterMap.assignments(base, nodeId)).listBlobs().keySet()) {
            final int separator = claim.lastIndexOf(RegisterMap.SHARD_SEPARATOR);
            if (separator < 0) {
                continue;
            }
            final String indexName = claim.substring(0, separator);
            final int shard;
            try {
                shard = Integer.parseInt(claim.substring(separator + RegisterMap.SHARD_SEPARATOR.length()));
            } catch (NumberFormatException e) {
                continue;
            }

            // The claim is only a hint. The head decides, so a stale entry costs one read and is then
            // ignored -- which is what makes it safe for the listing to lag reality.
            final Optional<ShardHead> head = heads.read(indexName, shard);
            if (head.isEmpty() || nodeId.equals(head.get().ownerNodeId()) == false) {
                continue;
            }
            final Optional<IndexDescriptor> descriptor = hosted.containsKey(indexName)
                ? Optional.of(hosted.get(indexName))
                : descriptors.get(indexName);
            if (descriptor.isEmpty()) {
                // The index was deleted underneath us. Not an error: the shard is going away too.
                continue;
            }
            // The term comes from the head, which is the one monotonic number every node touching this
            // shard agrees on. Never a per-node counter -- see s1-findings.md.
            assignments.add(new ShardAssignment(indexName, shard, head.get().term()));
            hosted.put(indexName, descriptor.get());
        }
        return new Truth(hosted.values(), assignments);
    }

    /**
     * Registers a repository: a namespace within this deployment's own object store to take snapshots
     * under. One put-if-absent, the same atomicity {@link #createIndex} and {@link #createPointInTime}
     * both rely on, arbitrated by the object store rather than by a cluster-manager.
     *
     * <p>Immutable once created and deleted wholesale rather than ever mutated in place, the same shape a
     * {@link PointInTime} has and an {@link IndexDescriptor} does not — so this is a plain blob write, not
     * a CAS register: nothing here is ever compare-and-swapped again after creation.
     *
     * @param descriptor the repository to register
     * @throws RepositoryAlreadyExistsException if the name is taken
     * @throws IOException if the write fails
     */
    public void createRepository(RepositoryDescriptor descriptor) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.repositories(base));
        final var bytes = descriptor.toBytes();
        try {
            container.writeBlob(descriptor.name(), bytes.streamInput(), bytes.length(), true);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            throw new RepositoryAlreadyExistsException(descriptor.name());
        }
    }

    /**
     * Reads a repository's record.
     *
     * @param name the repository
     * @return the record, or empty if not registered
     * @throws IOException if the read fails
     */
    public Optional<RepositoryDescriptor> describeRepository(String name) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.repositories(base));
        if (container.blobExists(name) == false) {
            return Optional.empty();
        }
        try (java.io.InputStream in = container.readBlob(name)) {
            return Optional.of(RepositoryDescriptor.fromStream(in));
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            return Optional.empty();
        }
    }

    /**
     * Removes a repository, refusing while it still holds snapshots.
     *
     * <p>The refusal is what a real {@code _snapshot} API's own delete-repository endpoint always enforces
     * — a repository is a namespace for durable records, and deleting it out from under snapshots that
     * still name it would leave them with nowhere to describe or restore from.
     *
     * @param name the repository
     * @return true if it existed
     * @throws RepositoryInUseException if it still holds snapshots
     * @throws IOException if listing, the read, or the delete fails
     */
    public boolean deleteRepository(String name) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.repositories(base));
        if (container.blobExists(name) == false) {
            return false;
        }
        final var held = listSnapshots(name);
        if (held.isEmpty() == false) {
            throw new RepositoryInUseException(name, held.size());
        }
        container.deleteBlobsIgnoringIfNotExists(java.util.List.of(name));
        return true;
    }

    /**
     * Records a snapshot. One put-if-absent; the caller has already gathered every captured manifest by
     * the time this is called, so this is a single write regardless of how many indices or shards it
     * names — the same "take the cost once, up front" shape {@link #createPointInTime} has.
     *
     * @param snapshot the snapshot to record
     * @throws RepositoryMissingException if its repository is not registered
     * @throws SnapshotAlreadyExistsException if the name is taken within that repository
     * @throws IOException if the write fails
     */
    public void createSnapshot(SnapshotRecord snapshot) throws IOException {
        if (describeRepository(snapshot.repo()).isEmpty()) {
            throw new RepositoryMissingException(snapshot.repo());
        }
        final var container = blobStore.blobContainer(RegisterMap.snapshots(base));
        final var bytes = snapshot.toBytes();
        try {
            container.writeBlob(snapshot.key(), bytes.streamInput(), bytes.length(), true);
        } catch (java.nio.file.FileAlreadyExistsException e) {
            throw new SnapshotAlreadyExistsException(snapshot.repo(), snapshot.name());
        }
    }

    /**
     * Reads one snapshot's record.
     *
     * @param repo the repository
     * @param name the snapshot
     * @return the record, or empty if it does not exist
     * @throws IOException if the read fails
     */
    public Optional<SnapshotRecord> snapshot(String repo, String name) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.snapshots(base));
        final String key = RegisterMap.snapshotKey(repo, name);
        if (container.blobExists(key) == false) {
            return Optional.empty();
        }
        try (java.io.InputStream in = container.readBlob(key)) {
            return Optional.of(SnapshotRecord.fromStream(in));
        } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
            return Optional.empty();
        }
    }

    /**
     * Removes a snapshot's record, so the collector stops treating its blobs as referenced.
     *
     * @param repo the repository
     * @param name the snapshot
     * @return true if it existed
     * @throws IOException if the delete fails
     */
    public boolean deleteSnapshot(String repo, String name) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.snapshots(base));
        final String key = RegisterMap.snapshotKey(repo, name);
        if (container.blobExists(key) == false) {
            return false;
        }
        container.deleteBlobsIgnoringIfNotExists(java.util.List.of(key));
        return true;
    }

    /**
     * Lists every snapshot within one repository.
     *
     * <p>An operator-triggered listing, the same exception every other listing in this class already is:
     * registering, deleting or describing a repository happens at human scale, and this is what a real
     * {@code _snapshot} API's own "list snapshots" endpoint always was — never on a serving path.
     *
     * @param repo the repository
     * @return every snapshot it holds, skipping — and logging — any record that cannot be read
     * @throws IOException if listing fails
     */
    public List<SnapshotRecord> listSnapshots(String repo) throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.snapshots(base));
        final List<SnapshotRecord> found = new ArrayList<>();
        for (String key : container.listBlobsByPrefix(repo + RegisterMap.SHARD_SEPARATOR).keySet()) {
            try (java.io.InputStream in = container.readBlob(key)) {
                found.add(SnapshotRecord.fromStream(in));
            } catch (Exception e) {
                LOGGER.warn("could not read the snapshot [" + key + "]; leaving it out of the listing", e);
            }
        }
        return found;
    }

    /**
     * Reads every snapshot in the deployment, across every repository.
     *
     * <p>For the collector, which sweeps one shard at a time and has to know what every snapshot,
     * anywhere, is still pinning before it deletes anything — the same reason {@link #livePointsInTime}
     * exists. A record that cannot be read is <b>skipped</b>, not treated as pinning everything the way an
     * unreadable {@link PointInTime} is: a point in time's conservative fallback exists because a paging
     * caller mid-request loses nothing but latency if the sweep is briefly more cautious than it needs to
     * be, and it can name only one shard at a time, so "assume it holds this shard" is a bounded,
     * affordable guess. A snapshot can name an unbounded number of indices and shards, so the equivalent
     * guess would be "assume it holds everything, deployment-wide" — which turns one corrupted record into
     * a garbage collector switched off everywhere, silently, forever. That trade is documented rather than
     * made silently: see {@code m45-snapshot-restore-notes.md}.
     *
     * @return every readable snapshot record
     * @throws IOException if listing fails
     */
    public List<SnapshotRecord> liveSnapshots() throws IOException {
        final var container = blobStore.blobContainer(RegisterMap.snapshots(base));
        final List<SnapshotRecord> found = new ArrayList<>();
        for (String key : container.listBlobs().keySet()) {
            try (java.io.InputStream in = container.readBlob(key)) {
                found.add(SnapshotRecord.fromStream(in));
            } catch (Exception e) {
                LOGGER.warn("could not read the snapshot [" + key + "]; its blobs will not be protected by this sweep", e);
            }
        }
        return found;
    }

    /**
     * Returns the backing blob store, for components that address shard data directly.
     *
     * @return the blob store
     */
    public BlobStore blobStore() {
        return blobStore;
    }

    /**
     * Returns the deployment's base path.
     *
     * @return the base path
     */
    public BlobPath basePath() {
        return base;
    }

    /**
     * Returns the descriptor store.
     *
     * @return the descriptor store
     */
    public DescriptorStore descriptors() {
        return descriptors;
    }

    /**
     * Returns the shard-head store.
     *
     * @return the shard-head store
     */
    public ShardHeadStore heads() {
        return heads;
    }

    /**
     * Returns the cluster config register.
     *
     * @return the register
     */
    public ClusterConfig clusterConfig() {
        return clusterConfig;
    }

    /**
     * Returns the lease-backed membership source.
     *
     * @return the membership source
     */
    public BlobLeaseMembership membership() {
        return membershipField;
    }
}
