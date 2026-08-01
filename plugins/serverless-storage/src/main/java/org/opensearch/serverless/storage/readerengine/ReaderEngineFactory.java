/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.gc.GcSchedulerConfig;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.resharding.PartitionRewriteSchedulerConfig;
import org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.util.Optional;

/**
 * {@link EngineFactory} for reader shards: resolves the shard's currently-published head via
 * {@link ShardStateStore}, reads the corresponding {@link CommitManifest}, and opens an
 * {@link ObjectStoreReaderEngine} against it. A reader shard that has never had anything published
 * to it (no head yet) has nothing to open -- that is a configuration/timing error a reader shard
 * should not exist for, so it fails engine creation rather than opening an empty index.
 */
public final class ReaderEngineFactory implements EngineFactory {

    private final ShardStateStore shardStateStore;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final ReaderShardAdmissionController admissionController;
    private final CompactionSchedulerConfig compactionConfig;
    private final GcSchedulerConfig gcConfig;
    private final ReaderShardActivityRegistry activityRegistry;
    private final ShardPartitionDescriptor partitionDescriptor;
    private final PartitionRewriteSchedulerConfig partitionRewriteConfig;
    private final PitrRetentionConfig pitrRetentionConfig;

    /**
     * Creates a factory with no admission controller, compaction, or GC scheduling.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId
    ) {
        this(shardStateStore, manifestStore, materializer, shardDirectory, localNodeId, null, null, null);
    }

    /**
     * Creates a factory with no compaction or GC scheduling.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController
    ) {
        this(shardStateStore, manifestStore, materializer, shardDirectory, localNodeId, admissionController, null, null);
    }

    /**
     * Creates a factory with no GC scheduling.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig
    ) {
        this(shardStateStore, manifestStore, materializer, shardDirectory, localNodeId, admissionController, compactionConfig, null, null);
    }

    /**
     * Creates a factory with no shard-activity registration.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc.
     * @param gcConfig {@code null} disables this reader's own background GC sweep -- see its own javadoc.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig
    ) {
        this(
            shardStateStore,
            manifestStore,
            materializer,
            shardDirectory,
            localNodeId,
            admissionController,
            compactionConfig,
            gcConfig,
            null
        );
    }

    /**
     * Creates a factory with all optional features configurable.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc.
     * @param gcConfig {@code null} disables this reader's own background GC sweep -- see its own javadoc.
     * @param activityRegistry {@code null} to skip registering this factory's engines for manifest-generation-lag lookups.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig,
        ReaderShardActivityRegistry activityRegistry
    ) {
        this(
            shardStateStore,
            manifestStore,
            materializer,
            shardDirectory,
            localNodeId,
            admissionController,
            compactionConfig,
            gcConfig,
            activityRegistry,
            null
        );
    }

    /**
     * Creates a factory with every optional feature configurable, including this shard's own
     * &sect;16 Phase 5 partition descriptor.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc.
     * @param gcConfig {@code null} disables this reader's own background GC sweep -- see its own javadoc.
     * @param activityRegistry {@code null} to skip registering this factory's engines for manifest-generation-lag lookups.
     * @param partitionDescriptor {@code null} unless this shard is a split target -- see {@link
     *        org.opensearch.serverless.storage.resharding.PartitionFilteringDirectoryReader}'s own javadoc.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig,
        ReaderShardActivityRegistry activityRegistry,
        ShardPartitionDescriptor partitionDescriptor
    ) {
        this(
            shardStateStore,
            manifestStore,
            materializer,
            shardDirectory,
            localNodeId,
            admissionController,
            compactionConfig,
            gcConfig,
            activityRegistry,
            partitionDescriptor,
            null
        );
    }

    /**
     * Creates a factory with every optional feature configurable, including this shard's own
     * &sect;16 Phase 5 background partition-rewrite scheduler.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc.
     * @param gcConfig {@code null} disables this reader's own background GC sweep -- see its own javadoc.
     * @param activityRegistry {@code null} to skip registering this factory's engines for manifest-generation-lag lookups.
     * @param partitionDescriptor {@code null} unless this shard is a split target -- see {@link
     *        org.opensearch.serverless.storage.resharding.PartitionFilteringDirectoryReader}'s own javadoc.
     * @param partitionRewriteConfig {@code null} disables this reader's own background
     *        partition-rewrite scheduler -- see {@link PartitionRewriteSchedulerConfig}'s own javadoc.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig,
        ReaderShardActivityRegistry activityRegistry,
        ShardPartitionDescriptor partitionDescriptor,
        PartitionRewriteSchedulerConfig partitionRewriteConfig
    ) {
        this(
            shardStateStore,
            manifestStore,
            materializer,
            shardDirectory,
            localNodeId,
            admissionController,
            compactionConfig,
            gcConfig,
            activityRegistry,
            partitionDescriptor,
            partitionRewriteConfig,
            null
        );
    }

    /**
     * Creates a factory with every optional feature configurable, including this shard's own
     * background PITR retention reconciliation -- see this class's own javadoc for why a reader
     * shard, not just the writer, must run this: a reader is what stays alive across a writer
     * scaling to zero, and PITR pins left unreconciled past that point never age out, so GC can
     * never reclaim them.
     *
     * @param shardStateStore resolves the shard's currently-published head
     * @param manifestStore reads the commit manifest for a resolved head
     * @param materializer applies a manifest's files to the engine's store directory
     * @param shardDirectory the shard-directory-tier client the opened engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc.
     * @param gcConfig {@code null} disables this reader's own background GC sweep -- see its own javadoc.
     * @param activityRegistry {@code null} to skip registering this factory's engines for manifest-generation-lag lookups.
     * @param partitionDescriptor {@code null} unless this shard is a split target -- see {@link
     *        org.opensearch.serverless.storage.resharding.PartitionFilteringDirectoryReader}'s own javadoc.
     * @param partitionRewriteConfig {@code null} disables this reader's own background
     *        partition-rewrite scheduler -- see {@link PartitionRewriteSchedulerConfig}'s own javadoc.
     * @param pitrRetentionConfig {@code null} disables this reader's own background PITR retention
     *        reconciliation -- see {@link PitrRetentionConfig}'s own javadoc.
     */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig,
        ReaderShardActivityRegistry activityRegistry,
        ShardPartitionDescriptor partitionDescriptor,
        PartitionRewriteSchedulerConfig partitionRewriteConfig,
        PitrRetentionConfig pitrRetentionConfig
    ) {
        this.shardStateStore = shardStateStore;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.admissionController = admissionController;
        this.compactionConfig = compactionConfig;
        this.gcConfig = gcConfig;
        this.activityRegistry = activityRegistry;
        this.partitionDescriptor = partitionDescriptor;
        this.partitionRewriteConfig = partitionRewriteConfig;
        this.pitrRetentionConfig = pitrRetentionConfig;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            String indexUuid = config.getShardId().getIndex().getUUID();
            int shardId = config.getShardId().getId();

            // Reuse ServerlessStorageLazyDirectoryFactory#newDirectory's own already-fetched head
            // and manifest when this shard's directory is a LazyBundleDirectory: core builds the
            // directory before the engine for the same shard open, and that factory already paid
            // for exactly the head read + manifest GET this method would otherwise redundantly
            // repeat moments later. No staler than the two independent reads it replaces would have
            // been relative to each other anyway -- see LazyBundleDirectory#currentManifest's own
            // javadoc for why this introduces no new consistency risk.
            org.apache.lucene.store.Directory unwrapped = org.apache.lucene.store.FilterDirectory.unwrap(config.getStore().directory());
            CommitManifest manifest;
            long primaryTerm;
            if (unwrapped instanceof org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory) {
                manifest = ((org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory) unwrapped).currentManifest();
                primaryTerm = manifest.primaryTerm();
            } else {
                Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
                // generation 0 means a head exists (e.g. a writer's lease acquisition put one there
                // ahead of any commit, see ObjectStoreCommitHeadPublisher#acquireOrRenewLease) but
                // nothing has actually been published yet -- same "nothing for a reader to open"
                // case as no head at all.
                if (head.isEmpty() || head.get().head().latestManifestGeneration() == 0) {
                    throw new IllegalStateException(
                        "no published head for shard " + config.getShardId() + "; nothing for a reader to open"
                    );
                }
                ShardHead shardHead = head.get().head();
                manifest = manifestStore.readManifest(shardHead.primaryTerm(), shardHead.latestManifestGeneration());
                primaryTerm = shardHead.primaryTerm();
            }
            // ObjectStoreReaderEngine reports to the directory tier itself, both on activation and
            // on a fixed refresh schedule for as long as it stays open (rfc-serverless-metadata-plane.md
            // &sect;9 activation path step 3, &sect;13 risk #1 metastability mitigation) -- see its
            // javadoc. This is a hint, not a fact, so a report failure is never worth failing engine
            // construction over.
            ObjectStoreReaderEngine engine = ObjectStoreReaderEngine.open(
                config,
                manifest,
                materializer,
                primaryTerm,
                shardStateStore,
                manifestStore,
                shardDirectory,
                localNodeId,
                admissionController,
                compactionConfig,
                gcConfig,
                partitionDescriptor,
                partitionRewriteConfig,
                pitrRetentionConfig
            );
            if (activityRegistry != null) {
                activityRegistry.register(indexUuid, shardId, engine);
            }
            return engine;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store reader engine", e);
        }
    }
}
