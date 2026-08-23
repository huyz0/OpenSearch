/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.collect.Tuple;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardRecoveryStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.FallbackBundleFileReader;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.resharding.BlobContainerInPlaceSplitRangeStore;
import org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger;
import org.opensearch.serverless.storage.resharding.InPlaceSplitRangeDescriptor;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * This plugin's {@link ShardRecoveryStrategy}: the object store is authoritative and a node's local
 * disk is a cache of it, so every question core asks on this seam has a different answer here than
 * core's own {@link org.opensearch.index.shard.LocalLuceneShardRecoveryStrategy} gives.
 *
 * <p>Registered node-wide under {@link #NAME} by {@code ServerlessStoragePlugin#getShardRecoveryStrategies},
 * and selected per index by {@code index.recovery.strategy}, which {@code
 * ServerlessStorageIndexSettingProvider} injects for every serverless-storage index alongside the
 * allocator it already injects there. One instance serves every index and shard on the node, exactly like
 * this plugin's {@code DirectoryFactory}: everything any method here needs is derivable from the shard's
 * own {@code (indexUuid, shardId)} through {@code containerResolver}, so there is nothing per-shard to
 * hold. That is also why these operations no longer live on {@code WriterEngineFactory}, which is built
 * per shard: they run before any engine exists, and half of them concern shards other than the one being
 * recovered (a split parent, two merged children).
 *
 * @opensearch.internal
 */
public final class ObjectStoreShardRecoveryStrategy implements ShardRecoveryStrategy {

    /** The name this strategy is registered and selected under. */
    public static final String NAME = "object-store";

    private final ShardCloner.ContainerResolver containerResolver;
    private final Supplier<EngineNativeSnapshotSupport> engineNativeSnapshotSupport;

    /**
     * @param containerResolver resolves any {@code (indexUuid, shardId)} in this cluster to its own blob
     *                          container -- the same resolution {@code ServerlessStoragePlugin#resolveBlobContainer}
     *                          performs for the engine itself, handed here unrestricted because the in-place
     *                          split path writes a child's first manifest/registers through it.
     * @param engineNativeSnapshotSupport supplies this plugin's engine-native snapshot restore/release
     *                                    pair, or {@code null} to declare the feature unsupported (same
     *                                    shape as every other optional feature in this plugin). A supplier
     *                                    rather than the instance because {@code getShardRecoveryStrategies}
     *                                    runs before {@code createComponents} has built one -- the same
     *                                    "construct now, read the field lazily" shape {@code
     *                                    ServerlessStorageLazyDirectoryFactory} already uses. Because
     *                                    presence is itself the capability answer core gates its remote
     *                                    probe on, there is no second flag that could fall out of lockstep
     *                                    with it.
     */
    public ObjectStoreShardRecoveryStrategy(
        ShardCloner.ContainerResolver containerResolver,
        Supplier<EngineNativeSnapshotSupport> engineNativeSnapshotSupport
    ) {
        this.containerResolver = containerResolver;
        this.engineNativeSnapshotSupport = engineNativeSnapshotSupport;
    }

    @Override
    public boolean recoverLocalStore(IndexShard indexShard, Store store, RecoverySource.Type recoverySourceType) throws IOException {
        switch (recoverySourceType) {
            case EXISTING_STORE:
                return recoverFromLatestManifest(indexShard, store);
            case IN_PLACE_SPLIT_SHARD:
                return recoverInPlaceSplitChild(indexShard, store);
            case IN_PLACE_MERGE_SHARD:
                return recoverInPlaceMergeParent(indexShard, store);
            default:
                // Core only ever asks about the three above; anything else is a recovery source this
                // strategy has no durable state to answer for, which is exactly what false means.
                return false;
        }
    }

    /**
     * Whether the local Lucene commit core just read is still the one this shard's head names.
     *
     * <h4>Why this is needed at all</h4>
     *
     * {@link #recoverFromLatestManifest} only runs when this node has <em>no</em> readable local
     * commit, which is the failover case it was written for. It says nothing about the case where the node
     * does have one and it is out of date -- and for this engine that is a real case, because the object
     * store is authoritative and a node's disk is a cache of it. A restore is how that happens on purpose:
     * an index is closed, its head is moved onto an earlier commit, and it is reopened on the same node,
     * whose local files still describe the pre-restore commit. Recovering from those files silently undoes
     * the restore, which is exactly what {@code ServerlessStorageRestoreToInstantIT}'s reopen test caught.
     *
     * <h4>The comparison</h4>
     *
     * By segments file name, which identifies a commit: the head's manifest records the segments file its
     * commit was published with, so a local commit naming a different one is a different commit. Cheaper
     * than a checksum comparison and stronger than a generation number, which the local files do not carry.
     *
     * <p>A shard with no head or nothing published yet is not stale -- there is no authoritative commit to
     * be behind, and answering "stale" would delete a local store with nothing to replace it.
     */
    @Override
    public boolean localStoreIsStale(IndexShard indexShard, String localSegmentsFileName) throws IOException {
        Optional<CommitManifest> head = readLatestManifest(indexShard.shardId().getIndex().getUUID(), indexShard.shardId().getId());
        return head.isPresent() && head.get().segmentsFileName().equals(localSegmentsFileName) == false;
    }

    /**
     * {@code true}: {@link ObjectStoreWriterEngine} publishes every commit as an object-store
     * manifest (rfc-serverless-opensearch.md &sect;8) referencing this shard's own segment files
     * directly -- that manifest publication already <em>is</em> this shard's durable remote copy,
     * so core's separate {@code RemoteStoreRefreshListener} upload path (engaged whenever {@code
     * index.remote_store.enabled: true}, which this plugin's own reader-shard support requires
     * regardless, per rfc-serverless-opensearch.md &sect;18 risk #10) would otherwise upload the
     * same segment bytes a second time into a remote-store repository nothing in this plugin's own
     * reader/GC/retention paths ever reads back from -- confirmed wasted work, not a hypothesis, by
     * {@code ServerlessStorageSearchOnlyReplicaIT}.
     *
     * <p>Answered per index rather than per shard copy now that this lives on the index-level
     * recovery strategy rather than on the writer shard's own {@code EngineFactory}. That is the same
     * answer in practice: core only consults this for a copy that is {@code isRemoteStoreEnabled()},
     * which requires {@code shardRouting.primary()}, and a serverless index's primary is always a
     * writer shard -- a search-only replica never reaches the check at all.
     */
    @Override
    public boolean ownsRemoteSegmentDurability() {
        return true;
    }

    @Override
    public Optional<EngineNativeSnapshots> engineNativeSnapshots() {
        return Optional.ofNullable(engineNativeSnapshotSupport.get());
    }

    /**
     * The store-population half of rfc-serverless-opensearch.md &sect;7.1.2's cross-node writer
     * failover design (the allocation half, {@code ServerlessStorageExistingShardsAllocator}, is
     * implemented separately). Called by {@code StoreRecovery} exactly when a genuine cross-node
     * failover looks, to local recovery, like this shard "should exist but doesn't" -- this
     * plugin's writer shards are "no peer recovery" by design (&sect;7.1): no node's local disk is
     * ever the shard's authoritative copy, so finding nothing here is the expected starting state,
     * not a corruption.
     *
     * <p>Materializes the shard's last durably-published manifest into {@code store}'s directory
     * (the same {@link ObjectStoreCommitMaterializer} technique {@code ObjectStoreReaderEngine#open}
     * already uses, just applied here instead of at the engine-construction level where an
     * earlier attempt at this failed -- see &sect;6.4's note on that attempt for why materializing
     * too late, after {@code StoreRecovery} already created a local translog, leaves a stale
     * translog-UUID reference behind). Because {@code EXISTING_STORE} recovery assumes a local
     * translog already exists and never creates one on its own (unlike {@code EMPTY_STORE}'s {@code
     * recoverEmptyStore}), this method also bootstraps and associates a fresh one here, at the one
     * point in the whole recovery sequence where doing so is still correct: after the manifest's
     * real segment files are in place (so the new translog is associated with the actual recovered
     * commit, not a stale or trivial one) but before {@code StoreRecovery} does anything else that
     * would assume translog files were already there.
     *
     * <p>Deliberately does <em>not</em> call {@code store.bootstrapNewHistory()}: the manifest carries
     * this shard's real prior history (its own {@code maxSeqNo}/{@code localCheckpoint}), which
     * must be preserved, not reset -- {@code bootstrapNewHistory} is for a genuinely fresh index
     * with no real history at all, which this is not.
     *
     * <p>Anything durably written to the WAL <em>after</em> this manifest -- the actual gap this
     * method doesn't attempt to close -- is what {@link ObjectStoreWriterEngine#engineRecoveryOperations()}
     * closes next, once the engine itself opens on top of what this method just materialized.
     *
     * @param indexShard the shard whose local store is missing and being recovered from the shard's last durable manifest
     * @param store the (empty) local store to materialize the shard's last durable manifest into
     * @return {@code true} if the store was materialized and a new local translog bootstrapped; {@code false} if
     *         no durable manifest exists yet to recover from
     */
    private boolean recoverFromLatestManifest(IndexShard indexShard, Store store) throws IOException {
        String indexUuid = indexShard.shardId().getIndex().getUUID();
        int shardId = indexShard.shardId().getId();
        Optional<CommitManifest> latestManifest = readLatestManifest(indexUuid, shardId);
        if (latestManifest.isEmpty()) {
            return false;
        }
        CommitManifest manifest = latestManifest.get();
        Directory directory = store.directory();
        materializerFor(indexUuid, shardId).materialize(manifest, directory);

        String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            manifest.localCheckpoint(),
            indexShard.shardId(),
            indexShard.getPendingPrimaryTerm()
        );
        store.associateIndexWithNewTranslog(translogUUID);
        return true;
    }

    /**
     * The store-population half of an in-place split's child shard activation
     * (dynamic-partitioning-plan.md Phase 0), analogous to {@link #recoverFromLatestManifest} but for
     * a shard with no manifest of its own <em>yet</em> rather than one whose manifest merely isn't
     * locally materialized. Called by {@code StoreRecovery#internalRecoverFromStore} before this
     * shard's local translog is created or its engine is opened -- the same careful ordering {@link
     * #recoverFromLatestManifest} itself requires (see the "Task 19" note in
     * dynamic-partitioning-progress.md for the bug this ordering fixes: a first attempt at this
     * attached the child to the parent's data only in the <em>object store</em>, after the engine had
     * already opened against an empty local Lucene index, leaving the two permanently out of sync).
     *
     * <p>In order: resolves this child's parent and hash range from {@code SplitShardsMetadata}
     * (core cluster metadata, reachable via {@code indexShard.indexSettings()} without any
     * additional plumbing), reuses {@link ShardCloner#clone}'s existing zero-copy recipe verbatim --
     * retargeted from "brand-new index" to "same index, sibling shard ID already reserved by core's
     * {@code SplitShardsMetadata}," needing no new manifest-write logic since {@code CloneLineage}'s
     * {@code (sourceIndexUuid, sourceShardId)} shape already tolerates a same-index source -- to attach
     * this child's manifest to the parent's data, then materializes that manifest into {@code store}'s
     * local Lucene commit and creates a matching local translog, exactly as {@link
     * #recoverFromLatestManifest} does for its own case.
     *
     * @param indexShard the child shard whose local store is empty and being attached to its parent's data
     * @param store the (empty) local store to materialize the newly-cloned manifest into
     * @return {@code true} if the store was materialized and a new local translog bootstrapped;
     *         {@code false} if {@code indexShard} isn't a recognized in-progress split child (e.g. a
     *         retried recovery after the split already committed via {@code
     *         MetadataInPlaceSplitShardCommitService})
     */
    private boolean recoverInPlaceSplitChild(IndexShard indexShard, Store store) throws IOException {
        String indexUuid = indexShard.shardId().getIndex().getUUID();
        int childShardId = indexShard.shardId().getId();
        IndexMetadata indexMetadata = indexShard.indexSettings().getIndexMetadata();
        Tuple<Integer, ShardRange> parentAndRange = indexMetadata.getSplitShardsMetadata().getParentAndRangeOfChild(childShardId);
        if (parentAndRange == null) {
            return false;
        }
        int parentShardId = parentAndRange.v1();
        ShardRange childRange = parentAndRange.v2();

        BlobContainer parentContainer = containerResolver.resolve(indexUuid, parentShardId);
        BlobContainerManifestStore parentManifestStore = new BlobContainerManifestStore(parentContainer);
        ShardStateStore parentShardStateStore = new BlobContainerShardStateStore(parentContainer);
        BlobContainerDurablePinRegistry parentPinRegistry = new BlobContainerDurablePinRegistry(parentContainer);

        BlobContainer childContainer = containerResolver.resolve(indexUuid, childShardId);
        BlobContainerManifestStore childManifestStore = new BlobContainerManifestStore(childContainer);
        ShardStateStore childShardStateStore = new BlobContainerShardStateStore(childContainer);
        BlobContainerCloneLineageStore childLineageStore = new BlobContainerCloneLineageStore(childContainer);
        BlobContainerInPlaceSplitRangeStore childRangeStore = new BlobContainerInPlaceSplitRangeStore(childContainer);
        InPlaceSplitRangeDescriptor descriptor = new InPlaceSplitRangeDescriptor(parentShardId, childRange.start(), childRange.end());

        ShardCloner.clone(
            indexUuid,
            parentShardId,
            parentManifestStore,
            parentShardStateStore,
            parentPinRegistry,
            indexUuid,
            childShardId,
            childManifestStore,
            childShardStateStore,
            childLineageStore,
            System.currentTimeMillis(),
            () -> childRangeStore.writeDescriptor(descriptor)
        );

        CommitManifest manifest = childManifestStore.readManifest(1, 1);
        Directory directory = store.directory();
        // The manifest's bundle files physically live in the PARENT's container -- ShardCloner never
        // copies bytes, only references -- so materialization must read from there, not from this
        // child's own (still-empty) container, which is what materializerFor would build for the very
        // different "re-read my own past publish" case #recoverFromLatestManifest handles. The parent
        // itself may be a clone/split child too (a split-of-a-split), in which case ITS manifest can
        // still reference bundles that only physically exist further back in the lineage -- walking the
        // full chain rather than reading only `parentContainer` avoids a NoSuchFileException on a bundle
        // that legitimately exists, just further upstream.
        new ObjectStoreCommitMaterializer(
            FallbackBundleFileReader.chain(lineageReaders(indexUuid, parentShardId, sameIndexLineageResolver("in-place split", indexUuid)))
        ).materialize(manifest, directory);

        String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            manifest.localCheckpoint(),
            indexShard.shardId(),
            indexShard.getPendingPrimaryTerm()
        );
        store.associateIndexWithNewTranslog(translogUUID);
        return true;
    }

    /**
     * The store-population half of an in-place shard <em>merge</em>'s revived-parent activation
     * (dynamic-partitioning-plan.md Phase 2 item 2.1) -- the reverse of {@link
     * #recoverInPlaceSplitChild}, and, like it, called by {@code
     * StoreRecovery#internalRecoverFromStore} before this shard's local translog is created or its
     * engine is opened (the same stale-translog-UUID ordering constraint, see {@link
     * #recoverFromLatestManifest}'s own javadoc).
     *
     * <p><b>Where the children come from.</b> Unlike a split child (which recovers <em>during</em> the
     * in-progress window and can still resolve its own parent and range from {@code
     * SplitShardsMetadata}), a merge's parent recovers <em>after</em> {@code
     * MetadataInPlaceMergeShardService} has already de-committed the split in the same cluster-state
     * update that revived this parent -- so the metadata no longer records which children it came from.
     * The retired children's {@link ShardRange}s (each carrying its own child shard id) are therefore
     * carried on the recovery source itself ({@link
     * org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource}), which this reads
     * back from {@code indexShard.recoveryState()}.
     *
     * <p><b>What it does.</b> For each child: resolves its blob container, reads its current published
     * manifest (via its own shard-head), and materializes it through a fallback read path (the child's
     * own container first, the parent's lineage as fallback for a pristine child that never published a
     * post-split commit and so still references the parent's cloned-by-reference base bundle). It then
     * folds every child's authoritative, range-filtered document slice into {@code store}'s directory
     * via {@link InPlaceSiblingMerger#merge} (a real {@link org.apache.lucene.index.IndexWriter#addIndexes}
     * of the filtered readers -- see that class's javadoc for why the union needs no bespoke
     * delete/version reconciliation), and creates a matching local translog at the merged checkpoint.
     * The revived parent's engine takes over from there, republishing this merged commit to the parent's
     * own container on its first flush.
     *
     * @param indexShard the parent shard being revived from its (now-retired) children's data.
     * @param store the (empty) local store to fold the merged commit into.
     * @return {@code true} if the store was materialized and a new local translog bootstrapped;
     *         {@code false} if this shard is not recovering via an {@link
     *         org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource}
     *         carrying children (e.g. the empty-children {@code INSTANCE} used outside a real merge).
     */
    private boolean recoverInPlaceMergeParent(IndexShard indexShard, Store store) throws IOException {
        RecoverySource recoverySource = indexShard.recoveryState().getRecoverySource();
        if (recoverySource instanceof RecoverySource.InPlaceMergeShardRecoverySource == false) {
            return false;
        }
        List<ShardRange> children = ((RecoverySource.InPlaceMergeShardRecoverySource) recoverySource).children();
        if (children.isEmpty()) {
            return false;
        }

        String indexUuid = indexShard.shardId().getIndex().getUUID();
        int parentShardId = indexShard.shardId().getId();
        // Each child's own clone lineage points directly at the parent (one hop -- see
        // #recoverInPlaceSplitChild, which writes exactly that), so the child's own container is
        // always tried first. But the PARENT itself may be a clone/split child too (a split-of-a-split
        // later merged back), in which case its manifest can still reference bundles that only
        // physically exist further back in the lineage than the parent's own container -- walk the full
        // chain once, shared by every child below, mirroring #recoverInPlaceSplitChild's fix.
        List<BundleFileReader> parentReaders = lineageReaders(
            indexUuid,
            parentShardId,
            sameIndexLineageResolver("in-place merge", indexUuid)
        );

        List<InPlaceSiblingMerger.MergeChild> mergeChildren = new ArrayList<>(children.size());
        for (ShardRange childRange : children) {
            int childShardId = childRange.shardId();
            BlobContainer childContainer = containerResolver.resolve(indexUuid, childShardId);
            ShardStateStore childShardStateStore = new BlobContainerShardStateStore(childContainer);
            VersionedShardHead childHead = childShardStateStore.get(indexUuid, childShardId)
                .orElseThrow(
                    () -> new IOException(
                        "in-place merge: child shard " + indexUuid + "/" + childShardId + " has no published head to merge"
                    )
                );
            BlobContainerManifestStore childManifestStore = new BlobContainerManifestStore(childContainer);
            CommitManifest childManifest = childManifestStore.readManifest(
                childHead.head().primaryTerm(),
                childHead.head().latestManifestGeneration()
            );

            List<BundleFileReader> childDelegates = new ArrayList<>(parentReaders.size() + 1);
            childDelegates.add(new BlobContainerBundleStore(childContainer));
            childDelegates.addAll(parentReaders);
            BundleFileReader childReadPath = new InPlaceSiblingMerger.FallbackBundleFileReader(childDelegates);
            mergeChildren.add(new InPlaceSiblingMerger.MergeChild(childManifest, childReadPath, childRange));
        }

        long mergedMaxSeqNo = InPlaceSiblingMerger.merge(mergeChildren, store.directory());

        String translogUUID = Translog.createEmptyTranslog(
            indexShard.shardPath().resolveTranslog(),
            mergedMaxSeqNo,
            indexShard.shardId(),
            indexShard.getPendingPrimaryTerm()
        );
        store.associateIndexWithNewTranslog(translogUUID);
        return true;
    }

    /**
     * The latest durably-published manifest for {@code (indexUuid, shardId)}, or empty when this shard
     * has never had a head at all or has one whose {@code latestManifestGeneration()} is still the
     * {@code 0} "nothing published yet" sentinel. Rebuilt per call from the shard's own container rather
     * than held as state: this strategy is one node-wide instance, and reading a head is two blob reads,
     * on a path that runs at most once per shard recovery.
     */
    private Optional<CommitManifest> readLatestManifest(String indexUuid, int shardId) throws IOException {
        BlobContainer container = containerResolver.resolve(indexUuid, shardId);
        return new ObjectStoreCommitHeadPublisher(
            new ObjectStoreCommitPublisher(new BlobContainerBundleStore(container), new BlobContainerManifestStore(container)),
            new BlobContainerShardStateStore(container)
        ).readLatestManifest(indexUuid, shardId);
    }

    /**
     * A materializer whose read path is {@code (indexUuid, shardId)}'s own container followed by every
     * ancestor container in its clone lineage, in order. A shard freshly split off a parent (or several
     * clone hops deep -- see {@link ShardCloner#resolveLineageChain}'s own javadoc) has a manifest
     * referencing bundle files that physically exist only in an ancestor's container until it
     * republishes its own commit; without the fallback chain every read of such a manifest throws
     * {@code NoSuchFileException}.
     */
    private ObjectStoreCommitMaterializer materializerFor(String indexUuid, int shardId) throws IOException {
        return new ObjectStoreCommitMaterializer(FallbackBundleFileReader.chain(lineageReaders(indexUuid, shardId, containerResolver)));
    }

    /** {@link #materializerFor}'s reader chain, also reused directly where it is one leg of a wider chain. */
    private List<BundleFileReader> lineageReaders(String indexUuid, int shardId, ShardCloner.ContainerResolver lineageResolver)
        throws IOException {
        BlobContainer ownContainer = containerResolver.resolve(indexUuid, shardId);
        List<BlobContainer> lineageChain = ShardCloner.resolveLineageChain(ownContainer, indexUuid, shardId, lineageResolver);
        List<BundleFileReader> readers = new ArrayList<>(lineageChain.size());
        for (BlobContainer container : lineageChain) {
            readers.add(new BlobContainerBundleStore(container));
        }
        return readers;
    }

    /**
     * A lineage resolver that refuses to leave {@code indexUuid}. In-place split/merge lineage is
     * always same-index by construction, so a hop into a different index means the lineage records
     * are describing something this recovery path cannot reason about -- caught here with an
     * explanation rather than silently reading another index's bundles.
     */
    private ShardCloner.ContainerResolver sameIndexLineageResolver(String operation, String indexUuid) {
        return (lineageIndexUuid, lineageShardId) -> {
            if (lineageIndexUuid.equals(indexUuid) == false) {
                throw new IOException(
                    operation
                        + " recovery: lineage of index "
                        + indexUuid
                        + " unexpectedly crosses into a different index ["
                        + lineageIndexUuid
                        + "] -- in-place split/merge lineage must stay within one index"
                );
            }
            return containerResolver.resolve(lineageIndexUuid, lineageShardId);
        };
    }
}
