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

    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId
    ) {
        this(shardStateStore, manifestStore, materializer, shardDirectory, localNodeId, null, null, null);
    }

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

    /** @param compactionConfig {@code null} disables this reader's own background compaction scheduler -- see its own javadoc. */
    public ReaderEngineFactory(
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig
    ) {
        this(shardStateStore, manifestStore, materializer, shardDirectory, localNodeId, admissionController, compactionConfig, null);
    }

    /** @param gcConfig {@code null} disables this reader's own background GC sweep -- see its own javadoc. */
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
        this.shardStateStore = shardStateStore;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.admissionController = admissionController;
        this.compactionConfig = compactionConfig;
        this.gcConfig = gcConfig;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            String indexUuid = config.getShardId().getIndex().getUUID();
            int shardId = config.getShardId().getId();
            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            // generation 0 means a head exists (e.g. a writer's lease acquisition put one there
            // ahead of any commit, see ObjectStoreCommitHeadPublisher#acquireOrRenewLease) but
            // nothing has actually been published yet -- same "nothing for a reader to open" case
            // as no head at all.
            if (head.isEmpty() || head.get().head().latestManifestGeneration() == 0) {
                throw new IllegalStateException("no published head for shard " + config.getShardId() + "; nothing for a reader to open");
            }
            ShardHead shardHead = head.get().head();
            CommitManifest manifest = manifestStore.readManifest(shardHead.primaryTerm(), shardHead.latestManifestGeneration());
            // ObjectStoreReaderEngine reports to the directory tier itself, both on activation and
            // on a fixed refresh schedule for as long as it stays open (rfc-serverless-metadata-plane.md
            // &sect;9 activation path step 3, &sect;13 risk #1 metastability mitigation) -- see its
            // javadoc. This is a hint, not a fact, so a report failure is never worth failing engine
            // construction over.
            return ObjectStoreReaderEngine.open(
                config,
                manifest,
                materializer,
                shardHead.primaryTerm(),
                shardStateStore,
                manifestStore,
                shardDirectory,
                localNodeId,
                admissionController,
                compactionConfig,
                gcConfig
            );
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store reader engine", e);
        }
    }
}
