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
import org.opensearch.serverless.membership.BlobLeaseMembership;
import org.opensearch.serverless.membership.NodeLease;

import java.io.IOException;
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
 * <p><b>Which stores this is verified on.</b> Passing on {@code FsBlobContainer} says nothing about whether
 * a provider's conditional write is genuinely linearizable, which is the thing the entire safety argument
 * rests on. No durability claim is made for a provider until the blob-store conformance suite has run
 * against it; see this package's documentation.
 */
public final class MetadataPlane {

    static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(MetadataPlane.class);

    private final DescriptorStore descriptors;
    private final ShardHeadStore heads;
    private final ReclaimQueue reclaimQueue;
    private final ClusterConfig clusterConfig;
    private final BlobLeaseMembership membershipField;
    private final BlobStore blobStore;
    private final BlobPath base;
    private final long leaseTtlMillis;
    private final LongSupplier clock;
    private final org.opensearch.serverless.bucket.DescriptorBucketHeads bucketHeads;
    private final ShardSettlement settlement;
    private final ShardActivation activation;
    private final AliasMutations aliases;
    private final IndexLifecycle lifecycle;
    private final PointsInTime pits;
    private final TemplateStores stores;
    private final ShardDataPaths paths;
    private final org.opensearch.serverless.bucket.BucketCommits bucketCommits;
    private final org.opensearch.serverless.bucket.BucketStore bucketStore;
    private final SnapshotRecords snapshots;

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
        this.routingCache = new RoutingCache(this::resolve, clock);

        this.descriptors = new DescriptorStore(
            blobStore.blobContainer(RegisterMap.indices(base)),
            blobStore,
            RegisterMap.tombstones(base),
            clock
        );
        // Every change this node makes drops the name from its own routing cache, so a client that
        // creates, rolls over or deletes and immediately writes sees its own change rather than a
        // resolution from a moment ago. Announced by the store rather than at each call site, because
        // there are eight of those and one forgetting is a stale route.
        this.descriptors.onChanged(this::forgetRouting);
        this.descriptors.probeAt(RegisterMap.probe(base));
        this.reclaimQueue = new ReclaimQueue(blobStore, RegisterMap.reclaim(base));
        this.rollups = new DigestRollups(blobStore, RegisterMap.rollups(base));
        final BlobLeaseMembership leases = new BlobLeaseMembership(
            blobStore.blobContainer(RegisterMap.members(base)),
            clock,
            leaseTtlMillis
        ).conditionalDeletes(descriptors::conditionalDelete);
        final LivenessOracle oracle = new LeaseLivenessOracle(leases, clock);
        this.heads = new ShardHeadStore(blobStore.blobContainer(RegisterMap.shards(base)), clock, leaseTtlMillis, oracle);
        this.bucketHeads = new org.opensearch.serverless.bucket.DescriptorBucketHeads(this::describeForRouting, clock);
        this.heads.setBucketHeads(bucketHeads);
        this.clusterConfig = new ClusterConfig(blobStore.blobContainer(RegisterMap.clusterConfig(base)));
        this.membershipField = leases;
        this.blobStore = blobStore;
        this.base = base;
        this.leaseTtlMillis = leaseTtlMillis;
        this.clock = clock;
        this.repositories = new RepositoryStore(blobStore.blobContainer(RegisterMap.repositories(base)), this::listSnapshots);
        this.transportSecretStore = new TransportSecretStore(blobStore.blobContainer(RegisterMap.clusterConfig(base)), clock);
        this.pointInTimeStore = new PointInTimeStore(
            blobStore.blobContainer(RegisterMap.pointsInTime(base)),
            blobStore.blobContainer(RegisterMap.pointInTimeStates(base)),
            clock
        );
        this.snapshotStore = new SnapshotStore(
            blobStore.blobContainer(RegisterMap.snapshots(base)),
            blobStore.blobContainer(RegisterMap.snapshotStates(base))
        );
        this.settlement = new ShardSettlement(blobStore, base, clock, heads, this);
        this.activation = new ShardActivation(blobStore, base, heads, descriptors, this);
        this.aliases = new AliasMutations(descriptors, rollups, clock, leaseTtlMillis, this);
        this.stores = new TemplateStores(blobStore, base);
        this.bucketStore = new org.opensearch.serverless.bucket.BucketStore(blobStore, base);
        this.bucketCommits = new org.opensearch.serverless.bucket.BucketCommits(bucketStore);
        this.paths = new ShardDataPaths(blobStore, base, descriptors, bucketHeads, bucketCommits);
        this.pits = new PointsInTime(pointInTimeStore, descriptors);
        this.snapshots = new SnapshotRecords(snapshotStore, blobStore, base, this);
        this.lifecycle = new IndexLifecycle(blobStore, base, clock, descriptors, heads, reclaimQueue, aliases, this);
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

    /** Returns the write-ahead log for one shard. */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, int shardId) throws IOException {
        return paths.walStore(indexName, shardId);
    }

    /** Returns the write-ahead log for one shard, for a caller that already knows the index's uuid. */
    public org.opensearch.serverless.store.WalStore walStore(String indexName, String uuid, int shardId) {
        return paths.walStore(indexName, uuid, shardId);
    }

    /** Returns where one shard's bytes live. */
    public BlobPath shardData(String indexName, int shardId) throws IOException {
        return paths.shardData(indexName, shardId);
    }

    /** Returns the segment publisher for one shard. */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, int shardId) throws IOException {
        return paths.segmentPublisher(indexName, shardId);
    }

    /** Returns the segment publisher for one shard, for a caller that already knows the index's uuid. */
    public org.opensearch.serverless.store.SegmentPublisher segmentPublisher(String indexName, String uuid, int shardId) {
        return paths.segmentPublisher(indexName, uuid, shardId);
    }

    /** Creates an index. */
    public long createIndex(IndexDescriptor descriptor) throws IOException {
        return lifecycle.createIndex(descriptor);
    }

    /** Reports whether a snapshot record describes a capture that has not finished. */
    public static boolean capturing(SnapshotRecord.SnapshottedIndex index) {
        return IndexLifecycle.capturing(index);
    }

    /** Reports whether a snapshot is still being taken. */
    public static boolean inProgress(SnapshotRecord snapshot) {
        return IndexLifecycle.inProgress(snapshot);
    }

    /** Deletes an index and every shard-head belonging to it. */
    public boolean deleteIndex(String indexName) throws IOException {
        final Optional<IndexDescriptor> before = describe(indexName);
        forgetBucket(indexName);
        final boolean deleted = lifecycle.deleteIndex(indexName);
        leaveBucket(deleted, before);
        return deleted;
    }

    /** Deletes only the incarnation the caller selected, preserving a replacement under the same name. */
    public boolean deleteIndex(String indexName, String expectedUuid) throws IOException {
        final Optional<IndexDescriptor> before = describe(indexName).filter(d -> expectedUuid == null || expectedUuid.equals(d.uuid()));
        forgetBucket(indexName);
        final boolean deleted = lifecycle.deleteIndex(indexName, expectedUuid);
        leaveBucket(deleted, before);
        return deleted;
    }

    /** Takes a deleted member out of its bucket's manifest. Best effort: a member with no descriptor is never served. */
    private void leaveBucket(boolean deleted, Optional<IndexDescriptor> before) {
        if (deleted == false || before.isEmpty() || before.get().bucket() == null) {
            return;
        }
        try {
            new org.opensearch.serverless.bucket.BucketAssigner(buckets(), 1).leave(before.get().bucket(), before.get().uuid());
        } catch (IOException e) {
            LOGGER.warn("could not take deleted index [{}] out of bucket [{}]", before.get().name(), before.get().bucket(), e);
        }
    }

    private void forgetBucket(String indexName) {
        bucketHeads.forget(indexName);
    }

    /** Reads an alias and its generation together, briefly waiting for a pending mutation. */
    public Optional<AliasAtGeneration> aliasWithGeneration(String name) throws IOException {
        return aliases.aliasWithGeneration(name);
    }

    /** Returns the same record naming different indices, with a data stream's fields carried through. */
    public static org.opensearch.serverless.cluster.AliasRecord withIndices(
        org.opensearch.serverless.cluster.AliasRecord record,
        java.util.List<String> indices
    ) {
        return AliasMutations.withIndices(record, indices);
    }

    /** Replaces an alias under a compare-and-swap, for a caller that has not read it. */
    public Optional<Long> updateAlias(org.opensearch.serverless.cluster.AliasRecord alias, long expectedGeneration) throws IOException {
        return aliases.updateAlias(alias, expectedGeneration);
    }

    /** Replaces an alias under a compare-and-swap. */
    public Optional<Long> updateAlias(
        org.opensearch.serverless.cluster.AliasRecord before,
        org.opensearch.serverless.cluster.AliasRecord alias,
        long expectedGeneration
    ) throws IOException {
        return aliases.updateAlias(before, alias, expectedGeneration);
    }

    /** Creates an alias, and lists it on each index it names. */
    public long createAlias(org.opensearch.serverless.cluster.AliasRecord alias) throws IOException {
        return aliases.createAlias(alias);
    }

    /** Answers which aliases name an index, verified. */
    public java.util.List<String> aliasesOf(IndexDescriptor descriptor) throws IOException {
        return aliases.aliasesOf(descriptor);
    }

    /**
     * Resolves the name a write is addressed to: an index by name, or a data stream's newest backing index.
     *
     * <p>A plain alias is not written through: an alias here names indices for reading, and which of
     * several a write should land in is the question a data stream's generation answers and an alias does
     * not.
     *
     * @param name the name
     * @return the target, or empty when the name is neither an index nor a data stream
     * @throws IOException if a register cannot be read
     */
    public Optional<WriteTarget> writeTarget(String name) throws IOException {
        final DescriptorResolution resolved = resolveForRouting(name);
        if (resolved.index() != null) {
            return Optional.of(new WriteTarget(resolved.index(), null));
        }
        if (resolved.alias() != null && resolved.alias().dataStream()) {
            // The backing index through the same cache: it was a second descriptor read on every write to
            // a data stream, and the uuid check where the write lands covers it exactly as it covers a name.
            return describeForRouting(resolved.alias().writeIndex()).map(backing -> new WriteTarget(backing, resolved.alias()));
        }
        return Optional.empty();
    }

    /**
     * Reads an index's descriptor for <em>routing</em> -- finding its shards to read, search or write --
     * which may be answered from a recent read. See {@link #resolveForRouting} for what that can and
     * cannot get wrong; a caller that goes on to swap the descriptor uses {@link #describe}.
     *
     * @param indexName the index
     * @return the descriptor, or empty if the name is not an index
     * @throws IOException if the register cannot be read or parsed
     */
    public Optional<IndexDescriptor> describeForRouting(String indexName) throws IOException {
        return Optional.ofNullable(resolveForRouting(indexName).index());
    }

    /**
     * Reads what a name stands for: an index, an alias, or nothing.
     *
     * @param name the name
     * @return what it names
     * @throws IOException if the read fails
     */
    public DescriptorResolution resolve(String name) throws IOException {
        final var resolved = aliases.recoverAlias(name);
        return resolved.alias() != null && resolved.alias().creationPending()
            ? new DescriptorResolution(null, null, resolved.generation())
            : resolved;
    }

    /** Reads an alias for mutation, briefly backing off while another writer prepares its hints. */
    public DescriptorResolution resolveForAliasMutation(String name) throws IOException {
        return aliases.resolveForAliasMutation(name);
    }

    /** How long a routing lookup may answer from a resolution it read earlier, by default. See {@link RoutingCache}. */
    public static final long DEFAULT_ROUTING_CACHE_MILLIS = RoutingCache.DEFAULT_ROUTING_CACHE_MILLIS;

    /** Approximate retained memory budget, including the lazily rendered routing metadata. */
    public static final long DEFAULT_ROUTING_CACHE_MAXIMUM_WEIGHT = RoutingCache.DEFAULT_ROUTING_CACHE_MAXIMUM_WEIGHT;

    /** Past this many names, routing-cache entries older than the window are dropped. */
    public static final int DEFAULT_ROUTING_CACHE_PRUNE_ABOVE = RoutingCache.DEFAULT_ROUTING_CACHE_PRUNE_ABOVE;

    /** What routing lookups may answer from; the methods below forward to it. */
    private final RoutingCache routingCache;

    /**
     * Resolves a name for <em>routing</em>, which may be answered from a recent read. See
     * {@link RoutingCache#resolveForRouting} for why only routing may, and what a stale answer can do.
     *
     * @param name the index or alias name
     * @return what it names
     * @throws IOException if the register cannot be read or parsed
     */
    public DescriptorResolution resolveForRouting(String name) throws IOException {
        return routingCache.resolveForRouting(name);
    }

    /**
     * When this node last read an index's descriptor from the store and found it at this uuid, for the incarnation
     * fence. See {@link RoutingCache#readConfirmingIncarnation}.
     *
     * @param indexName the index
     * @param uuid the incarnation
     * @return what the read found and when it began, or empty if no cached read found this index at this uuid
     */
    public java.util.Optional<ConfirmingRead> readConfirmingIncarnation(String indexName, String uuid) {
        return routingCache.readConfirmingIncarnation(indexName, uuid);
    }

    /**
     * Sets how many names the routing cache may hold before expired ones are dropped.
     *
     * @param entries the threshold
     * @return this, for chaining
     */
    public MetadataPlane setRoutingCachePruneAbove(int entries) {
        routingCache.setRoutingCachePruneAbove(entries);
        return this;
    }

    /**
     * Returns how many names the routing cache holds, for tests that pin its bound.
     *
     * @return the entry count
     */
    public int routingCacheSize() {
        return routingCache.routingCacheSize();
    }

    /**
     * Drops a name from this node's routing cache, so a change this node just made is visible to its own next
     * request rather than a window later.
     *
     * @param name the index or alias name
     */
    public void forgetRouting(String name) {
        routingCache.forgetRouting(name);
    }

    /**
     * Sets the approximate retained-memory budget for routing resolutions.
     *
     * @param bytes the budget; an individual resolution exceeding it is not retained
     * @return this plane
     */
    public MetadataPlane setRoutingCacheMaximumWeight(long bytes) {
        routingCache.setRoutingCacheMaximumWeight(bytes);
        return this;
    }

    /**
     * Sets how long a routing lookup may serve an earlier read; zero reads through every time.
     *
     * @param millis the window
     * @return this, for chaining
     */
    public MetadataPlane setRoutingCacheMillis(long millis) {
        routingCache.setRoutingCacheMillis(millis);
        return this;
    }

    /** Removes an alias. */
    public boolean deleteAlias(String name) throws IOException {
        return aliases.deleteAlias(name);
    }

    /** Removes an alias the caller has read, only if it has not changed since. */
    public boolean deleteAlias(org.opensearch.serverless.cluster.AliasRecord alias, long expectedGeneration) throws IOException {
        return aliases.deleteAlias(alias, expectedGeneration);
    }

    /** Freezes the current commit of every shard of an index. */
    public void createPointInTime(PointInTime pit) throws IOException {
        pits.createPointInTime(pit);
    }

    /** Records that a freeze has begun, before any manifest is read. */
    public void beginPointInTime(PointInTime pit) throws IOException {
        pits.beginPointInTime(pit);
    }

    /** Checks an index after its provisional pin was published, before reading any captured files. */
    public void verifyCaptureIncarnation(String index, String uuid) throws IOException {
        pits.verifyCaptureIncarnation(index, uuid);
    }

    /** Replaces a capture marker with the finished view, under the same id. */
    public void finishPointInTime(PointInTime pit) throws IOException {
        pits.finishPointInTime(pit);
    }

    /** Moves a frozen view's deadline later. */
    public void extendPointInTime(PointInTime pit) throws IOException {
        pits.extendPointInTime(pit);
    }

    /** Atomically extends the view if its deadline still matches the caller's observation. */
    public boolean extendPointInTime(PointInTime pit, long expectedExpiresAtMillis) throws IOException {
        return pits.extendPointInTime(pit, expectedExpiresAtMillis);
    }

    /** Reads a frozen view. */
    public java.util.Optional<PointInTime> pointInTime(String id) throws IOException {
        return pits.pointInTime(id);
    }

    /** Releases a frozen view, so the collector stops holding its files. */
    public boolean releasePointInTime(String id) throws IOException {
        return pits.releasePointInTime(id);
    }

    /** Reads every frozen view that has not expired. */
    public java.util.List<PointInTime> livePointsInTime(long nowMillis) throws IOException {
        return pits.livePointsInTime(nowMillis);
    }

    /** Removes frozen views that have expired. */
    public int reapPointsInTime(long nowMillis) throws IOException {
        return pits.reapPointsInTime(nowMillis);
    }

    private final PointInTimeStore pointInTimeStore;
    private final SnapshotStore snapshotStore;

    private final DigestRollups rollups;

    /**
     * Returns the digest rollups: every index's pruning digests, a few registers per name group.
     *
     * @return the rollups
     */
    public DigestRollups rollups() {
        return rollups;
    }

    /**
     * The most names an index pattern may match before it is refused.
     *
     * <p><b>Set by what a search can afford, no longer by one listing.</b> It was five hundred so that a
     * pattern resolved in one {@code ListObjectsV2} request, because the listing could not be resumed. It can
     * now: resolution pages a thousand names at a time from where the last page stopped, so ten thousand is
     * eleven listings, and what bounds a wide search is the fanout window, the pruning digest and the
     * per-query activation budget rather than the pattern. The refusal past the cap stays -- an answer cut
     * off at a limit looks exactly like a complete one.
     */
    public static final int DEFAULT_PATTERN_CAP = 10_000;

    /**
     * Returns the index and alias names beginning with a prefix.
     *
     * @param prefix the prefix, empty for every name
     * @param cap the most names to return before refusing
     * @return the matching names, in lexicographic order
     * @throws TooManyMatchesException if more than {@code cap} match
     * @throws IOException if the listing fails
     */
    public java.util.List<String> namesWithPrefix(String prefix, int cap) throws IOException {
        return descriptors.namesWithPrefix(prefix, cap);
    }

    /** Returns the store holding index templates. */
    public TemplateStore indexTemplates() {
        return stores.indexTemplates();
    }

    /** Returns the store holding component templates. */
    public StoredScriptStore scripts() {
        return stores.scripts();
    }

    /** Returns the search-pipeline store. */
    public TemplateStore searchPipelines() {
        return stores.searchPipelines();
    }

    /** Returns the store holding ingest pipelines. */
    public TemplateStore pipelines() {
        return stores.pipelines();
    }

    /** Returns the store holding component templates. */
    public TemplateStore componentTemplates() {
        return stores.componentTemplates();
    }

    /**
     * Reads one index's descriptor.
     *
     * @param indexName the index
     * @return its descriptor, or empty if there is no such index
     * @throws IOException if the register cannot be read
     */
    public Optional<IndexDescriptor> describe(String indexName) throws IOException {
        final Optional<IndexDescriptor> descriptor = descriptors.get(indexName);
        descriptor.ifPresent(bucketHeads::note);
        return descriptor;
    }

    /**
     * Returns the generation an index's descriptor currently sits at, for a compare-and-swap.
     *
     * @param indexName the index
     * @return the generation, or the absent generation when there is no descriptor
     * @throws IOException if the read fails
     */
    public long descriptorGeneration(String indexName) throws IOException {
        return descriptors.generationOf(indexName);
    }

    /**
     * Replaces an index's descriptor, only if it has not changed underneath the caller.
     *
     * <p>A compare-and-swap rather than a write, because two clients updating a mapping at the same time is
     * exactly the race this design has an object store to arbitrate. A lost swap is reported as such so the
     * caller can re-read and retry against what is actually stored, rather than overwriting it.
     *
     * @param descriptor the new descriptor
     * @param expectedGeneration the generation the caller read
     * @return the new generation, or empty if another writer got there first
     * @throws IOException if the swap fails
     */
    public Optional<Long> updateDescriptor(IndexDescriptor descriptor, long expectedGeneration) throws IOException {
        return descriptors.update(descriptor, expectedGeneration);
    }

    /** Attempts to take ownership of a shard. */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId) throws IOException {
        return activation.activate(indexName, shardId, nodeId, ephemeralId);
    }

    /** Attempts to take ownership of a shard of one incarnation of an index. */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId, String indexUuid) throws IOException {
        return activation.activate(indexName, shardId, nodeId, ephemeralId, indexUuid);
    }

    /** Attempts to take ownership of a shard of one incarnation of an index, saying which write log the taker writes. */
    public Acquisition activate(String indexName, int shardId, String nodeId, String ephemeralId, String indexUuid, boolean writesNodeLog)
        throws IOException {
        return activation.activate(indexName, shardId, nodeId, ephemeralId, indexUuid, writesNodeLog);
    }

    /** Forgets a node's claim on a shard. */
    public void forgetAssignment(String nodeId, String indexName, int shardId) throws IOException {
        activation.forgetAssignment(nodeId, indexName, shardId);
    }

    /** Reads everything one node needs in order to serve. */
    public Truth truthFor(String nodeId) throws IOException {
        return activation.truthFor(nodeId);
    }

    /** What became of a dead owner's shard when a survivor looked at it. */
    public enum Settled {
        /** The head no longer names that node, the node is live, or the index is gone: nothing was done. */
        NOT_ABANDONED,
        /** Given back with nothing unpublished: unowned, its owned mark and the dead node's claim cleared. */
        RELEASED_CLEAN,
        /** Given back, but its log holds writes past its published commit: it needs a writer to replay and publish. */
        NEEDS_REPLAY
    }

    /** Cleans up after a dead owner of one shard without taking the shard, when nothing it wrote is unpublished. */
    public Settled settleAbandoned(String indexName, int shard, String deadNodeId) throws IOException {
        return settlement.settleAbandoned(indexName, shard, deadNodeId);
    }

    /** Clears the owned mark of a shard nobody owns any more, when everything it holds is published. */
    public Settled settleReleased(String indexName, int shard, long markedTerm) throws IOException {
        return settlement.settleReleased(indexName, shard, markedTerm);
    }

    /** Whether any of the given nodes holds records of a shard in its write log that a published commit does not cover. */
    public boolean nodeLogHolds(
        List<String> nodeIds,
        String indexName,
        String uuid,
        int shard,
        long term,
        Optional<org.opensearch.serverless.store.CommitManifest> commit
    ) throws IOException {
        return settlement.nodeLogHolds(nodeIds, indexName, uuid, shard, term, commit);
    }

    /** What became of a node's write log when a survivor looked at it. */
    public enum NodeLogCollected {
        /** The node is live, or names no epochs: nothing to do. */
        NOTHING,
        /** A shard named in it has not published what it holds: kept, to be looked at again. */
        STILL_NEEDED,
        /** Nothing in it was needed: the lease names no epochs any more, and the blobs were deleted. */
        COLLECTED
    }

    /** Deletes the write log of a node that is not live, once every shard it names has published past what it holds. */
    public NodeLogCollected collectNodeLog(String nodeId) throws IOException {
        return settlement.collectNodeLog(nodeId);
    }

    /** Whether a lease names a node that is live now: neither revoked nor expired on this plane's clock. */
    public boolean isLive(NodeLease lease) {
        return settlement.isLive(lease);
    }

    /** Takes an index out of its group's rollups if it holds nothing: no shard owned, published or logged to. */
    public boolean forgetIfNothing(String indexName) throws IOException {
        return settlement.forgetIfNothing(indexName);
    }

    /** Gives an entry that records no digest the digest its shards' published commits carry, where nobody owns them. */
    public List<Integer> backfillDigest(String indexName) throws IOException {
        return settlement.backfillDigest(indexName);
    }

    /** Lists every node that has, or once had, claims on shards -- live or long gone. */
    public List<String> nodesWithClaims() throws IOException {
        return activation.nodesWithClaims();
    }

    /** Lists the shards a node has claimed -- one listing, no head reads. */
    public List<Map.Entry<String, Integer>> claimsOf(String nodeId) throws IOException {
        return activation.claimsOf(nodeId);
    }

    /** Reads what one incarnation of a node owns, as the heads say. */
    public Truth truthFor(String nodeId, String ephemeralId) throws IOException {
        return activation.truthFor(nodeId, ephemeralId);
    }

    /** The registered snapshot repositories; the methods below forward to it. */
    private final RepositoryStore repositories;

    /**
     * Registers a repository, or updates one already registered. See {@link RepositoryStore#putRepository}.
     *
     * @param descriptor the repository to register
     * @throws IOException if the write fails
     */
    public void putRepository(RepositoryDescriptor descriptor) throws IOException {
        repositories.putRepository(descriptor);
    }

    /**
     * Reads a repository's record.
     *
     * @param name the repository
     * @return the record, or empty if not registered
     * @throws IOException if the read fails
     */
    public Optional<RepositoryDescriptor> describeRepository(String name) throws IOException {
        return repositories.describeRepository(name);
    }

    /**
     * Lists every registered repository.
     *
     * @return every registered repository, skipping — and logging — any record that cannot be read
     * @throws IOException if listing fails
     */
    public List<RepositoryDescriptor> listRepositories() throws IOException {
        return repositories.listRepositories();
    }

    /**
     * Removes a repository, refusing while it still holds snapshots.
     *
     * @param name the repository
     * @return true if it existed
     * @throws RepositoryInUseException if it still holds snapshots
     * @throws IOException if listing, the read, or the delete fails
     */
    public boolean deleteRepository(String name) throws IOException {
        return repositories.deleteRepository(name);
    }

    /** Reserves a snapshot name and publishes its provisional pin before capture reads any manifests. */
    public SnapshotRecord createSnapshot(SnapshotRecord snapshot) throws IOException {
        return snapshots.createSnapshot(snapshot);
    }

    /** Atomically completes the same capture lifetime that reserved the name. */
    public void finishSnapshot(SnapshotRecord snapshot) throws IOException {
        snapshots.finishSnapshot(snapshot);
    }

    /** Reads one snapshot's authoritative record. */
    public Optional<SnapshotRecord> snapshot(String repo, String name) throws IOException {
        return snapshots.snapshot(repo, name);
    }

    /** Deletes the lifetime observed at admission; a replacement under the same name is left alone. */
    public boolean deleteSnapshot(String repo, String name) throws IOException {
        return snapshots.deleteSnapshot(repo, name);
    }

    /** Cleans up exactly one capture, including after capture failed and the name may have been reused. */
    public boolean deleteSnapshot(SnapshotRecord expected) throws IOException {
        return snapshots.deleteSnapshot(expected);
    }

    private boolean deleteSnapshot(String repo, String name, SnapshotRecord expected) throws IOException {
        return snapshots.deleteSnapshot(repo, name, expected);
    }

    /** Lists active snapshot records within one repository, logging unreadable records. */
    public List<SnapshotRecord> listSnapshots(String repo) throws IOException {
        return snapshots.listSnapshots(repo);
    }

    /** Reads the active snapshots a collector must protect. */
    public List<SnapshotRecord> liveSnapshots() throws IOException {
        return snapshots.liveSnapshots();
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
     * Returns the reclaim queue, where deletes record what they still owe.
     *
     * @return the queue
     */
    public ReclaimQueue reclaimQueue() {
        return reclaimQueue;
    }

    /** Finishes a delete whose intent has fallen due: removes whatever outlived it. */
    public void reclaim(ReclaimQueue.Intent intent) throws IOException {
        lifecycle.reclaim(intent);
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
     * Returns the resolver that tells the shard-head store which members are owned through their bucket's head, for
     * nodes to register their open shards with. Always installed: ownership must resolve alike on every node, so it
     * follows the descriptor's bucket field and never a node's settings.
     *
     * @return the resolver
     */
    public org.opensearch.serverless.bucket.DescriptorBucketHeads bucketHeads() {
        return bucketHeads;
    }

    /**
     * Returns the writer of bucket members' commits, shared by every member publisher this plane makes.
     *
     * @return the writer
     */
    public org.opensearch.serverless.bucket.BucketCommits bucketCommits() {
        return bucketCommits;
    }

    /**
     * Returns the store of bucket manifests.
     *
     * @return the bucket store
     */
    public org.opensearch.serverless.bucket.BucketStore buckets() {
        return bucketStore;
    }

    /**
     * Returns the cluster config register.
     *
     * @return the register
     */
    /**
     * Returns the secret every node of this deployment presents on a forwarded transport request.
     *
     * <p>The object store is the trust root of this design: a node that can read it is a member. The
     * transport port used to trust its callers on the strength of reaching it, which is no strength at all
     * without TLS. The secret is minted once, by whichever node reads first, under put-if-absent, and every
     * node reads the same one; a forwarded request that does not carry it is refused. It is not a
     * substitute for TLS on the wire -- a network plugin supplies that -- but it is what stops a stranger
     * on the port from writing to a shard this node owns.
     *
     * @return the secret
     * @throws IOException if the register cannot be read or created
     */
    public String transportSecret() throws IOException {
        return transportSecrets().current();
    }

    /** Reads, mints and rotates the transport secret; the methods below forward to it. */
    private final TransportSecretStore transportSecretStore;

    /**
     * Reads the transport secrets, minting the first under put-if-absent when there are none.
     *
     * @return the secrets and the register generation they sit at
     * @throws IOException if the register cannot be read or created
     */
    public TransportSecrets transportSecrets() throws IOException {
        return transportSecretStore.transportSecrets();
    }

    /**
     * Rotates the transport secret: the current one becomes the previous, a fresh one becomes current. See
     * {@link TransportSecretStore#rotateTransportSecret}.
     *
     * @return the secrets as rotated
     * @throws IOException if the register cannot be read or the swap keeps losing
     */
    public TransportSecrets rotateTransportSecret() throws IOException {
        return transportSecretStore.rotateTransportSecret();
    }

    /**
     * Returns the deployment's cluster-settings register.
     *
     * @return the cluster config
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
