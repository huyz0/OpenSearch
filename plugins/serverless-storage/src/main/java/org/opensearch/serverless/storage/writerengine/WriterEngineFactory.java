/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.store.Directory;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.wal.DedicatedWalGcConfig;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalGcSchedulerTask;
import org.opensearch.serverless.storage.wal.WalShardRegistry;

import java.io.IOException;
import java.util.Optional;

/** {@link EngineFactory} for writer shards: produces {@link ObjectStoreWriterEngine}s. */
public final class WriterEngineFactory implements EngineFactory {

    private final ObjectStoreCommitHeadPublisher headPublisher;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final PitrRetentionConfig pitrRetentionConfig;
    private final WalChunkService walChunkService;
    private final ObjectStoreCommitMaterializer materializer;
    private final org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider;
    private final ShardActivityRegistry activityRegistry;
    private final DedicatedWalGcConfig dedicatedWalGcConfig;

    /**
     * Creates a factory with neither PITR retention nor WAL mirroring configured, delegating to the
     * fuller constructor with both left {@code null}.
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     */
    public WriterEngineFactory(ObjectStoreCommitHeadPublisher headPublisher, ShardDirectory shardDirectory, String localNodeId) {
        this(headPublisher, shardDirectory, localNodeId, null, null);
    }

    /**
     * Creates a factory with WAL mirroring left disabled, delegating to the fuller constructor with
     * {@code walChunkService} set to {@code null}.
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig
    ) {
        this(headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, null);
    }

    /**
     * Creates a factory with missing-local-store recovery left disabled, delegating to the fuller
     * constructor with {@code materializer} set to {@code null}.
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService
    ) {
        this(headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService, null, null);
    }

    /**
     * Creates a factory with WAL-mirrored records left unencrypted, delegating to the fuller
     * constructor with {@code encryptionKeyProvider} set to {@code null}.
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param materializer {@code null} disables missing-local-store recovery entirely (same shape
     *                     as every other optional feature in this plugin) -- see {@link
     *                     #recoverMissingLocalStore} for what it's for.
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        ObjectStoreCommitMaterializer materializer
    ) {
        this(headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService, materializer, null);
    }

    /**
     * Creates a fully-configured factory, storing every dependency for engines it will later
     * produce via {@link #newReadWriteEngine}, leaving produced engines unregistered with any
     * {@link ShardActivityRegistry} (delegates to the fullest constructor with {@code null}).
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param materializer {@code null} disables missing-local-store recovery entirely (same shape
     *                     as every other optional feature in this plugin) -- see {@link
     *                     #recoverMissingLocalStore} for what it's for.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted, same shape
     *                              as every other optional feature in this plugin -- see {@link
     *                              ObjectStoreWriterEngine}'s own matching constructor javadoc.
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        ObjectStoreCommitMaterializer materializer,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider
    ) {
        this(
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            materializer,
            encryptionKeyProvider,
            null,
            null
        );
    }

    /**
     * Creates a fully-configured factory, storing every dependency for engines it will later
     * produce via {@link #newReadWriteEngine}, including a {@link ShardActivityRegistry} produced
     * engines register themselves into, leaving dedicated-WAL-stream retention unconfigured
     * (delegates to the fullest constructor with {@code dedicatedWalGcConfig} set to {@code null}).
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param materializer {@code null} disables missing-local-store recovery entirely (same shape
     *                     as every other optional feature in this plugin) -- see {@link
     *                     #recoverMissingLocalStore} for what it's for.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted, same shape
     *                              as every other optional feature in this plugin -- see {@link
     *                              ObjectStoreWriterEngine}'s own matching constructor javadoc.
     * @param activityRegistry {@code null} leaves produced engines unreachable for idle-time
     *                         queries (same shape as every other optional feature in this plugin);
     *                         non-null registers each produced engine into it as it's constructed.
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        ObjectStoreCommitMaterializer materializer,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        ShardActivityRegistry activityRegistry
    ) {
        this(
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            materializer,
            encryptionKeyProvider,
            activityRegistry,
            null
        );
    }

    /**
     * Creates a fully-configured factory, additionally scheduling a dedicated WAL stream's own
     * retention sweep on each engine it produces, when {@code walChunkService} is itself scoped to
     * this one shard's own dedicated container (rfc-serverless-opensearch.md &sect;12's "dedicated
     * WAL streams" bullet) rather than the node-shared one.
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     * @param walChunkService {@code null} disables WAL mirroring entirely; the shared node-level
     *                        instance for an ordinary shard, or a shard-scoped dedicated instance
     *                        matching {@code dedicatedWalGcConfig} for an opted-in one.
     * @param materializer {@code null} disables missing-local-store recovery entirely (same shape
     *                     as every other optional feature in this plugin) -- see {@link
     *                     #recoverMissingLocalStore} for what it's for.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted, same shape
     *                              as every other optional feature in this plugin -- see {@link
     *                              ObjectStoreWriterEngine}'s own matching constructor javadoc.
     * @param activityRegistry {@code null} leaves produced engines unreachable for idle-time
     *                         queries (same shape as every other optional feature in this plugin);
     *                         non-null registers each produced engine into it as it's constructed.
     * @param dedicatedWalGcConfig {@code null} for a shard sharing the node-level WAL container (its
     *                             retention is a separate node-level concern, unaffected by this
     *                             factory); non-null schedules a dedicated sweep on each produced
     *                             engine, owned by that engine's own lifecycle -- see {@link
     *                             ObjectStoreWriterEngine}'s matching constructor javadoc for why.
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        ObjectStoreCommitMaterializer materializer,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        ShardActivityRegistry activityRegistry,
        DedicatedWalGcConfig dedicatedWalGcConfig
    ) {
        this.headPublisher = headPublisher;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.pitrRetentionConfig = pitrRetentionConfig;
        this.walChunkService = walChunkService;
        this.materializer = materializer;
        this.encryptionKeyProvider = encryptionKeyProvider;
        this.activityRegistry = activityRegistry;
        this.dedicatedWalGcConfig = dedicatedWalGcConfig;
    }

    /** Exposed for tests (including from other packages, e.g. {@code ServerlessStoragePluginTests}) -- not part of this class's public contract. */
    public org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProviderForTesting() {
        return encryptionKeyProvider;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        // A WalGcSchedulerTask (if dedicatedWalGcConfig is configured) is constructed fresh here,
        // not stored as a factory field: this method runs exactly once per real engine activation,
        // and the task's Scheduler.Cancellable needs engineConfig's own ThreadPool, which only
        // becomes available at this point -- see DedicatedWalGcConfig's own javadoc for why it
        // deliberately doesn't carry a pre-built task. Declared outside the try below (not inline
        // in the ObjectStoreWriterEngine constructor call) so a failure constructing the engine
        // itself can still close this already-started task before rethrowing -- otherwise a lease
        // acquisition failure (or any other engine-construction failure) would leak a live scheduled
        // sweep with nothing left holding a reference to cancel it.
        WalGcSchedulerTask dedicatedWalGcSchedulerTask = dedicatedWalGcConfig == null
            ? null
            : new WalGcSchedulerTask(
                config.getThreadPool(),
                dedicatedWalGcConfig.gcInterval(),
                dedicatedWalGcConfig.walContainer(),
                new WalShardRegistry(dedicatedWalGcConfig.walContainer()),
                (indexUuid, shardId) -> dedicatedWalGcConfig.shardBlobContainer()
            );
        try {
            // ObjectStoreWriterEngine reports to the directory tier itself, both on activation and
            // on a fixed refresh schedule for as long as it stays open (rfc-serverless-metadata-plane.md
            // &sect;9 activation path step 3, &sect;13 risk #1 metastability mitigation) -- see its
            // javadoc. This is a hint, not a fact, so a report failure is never worth failing engine
            // construction over. It also owns PITR retention reconciliation when pitrRetentionConfig
            // is configured (null disables it, same shape as encryption), and WAL mirroring when
            // walChunkService is configured (null disables it the same way).
            ObjectStoreWriterEngine engine = new ObjectStoreWriterEngine(
                config,
                headPublisher,
                shardDirectory,
                localNodeId,
                pitrRetentionConfig,
                walChunkService,
                encryptionKeyProvider,
                dedicatedWalGcSchedulerTask
            );
            if (activityRegistry != null) {
                activityRegistry.register(config.getShardId().getIndex().getUUID(), config.getShardId().getId(), engine);
            }
            return engine;
        } catch (RuntimeException e) {
            if (dedicatedWalGcSchedulerTask != null) {
                dedicatedWalGcSchedulerTask.close();
            }
            throw e;
        } catch (Exception e) {
            if (dedicatedWalGcSchedulerTask != null) {
                dedicatedWalGcSchedulerTask.close();
            }
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store writer engine", e);
        }
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
     * already uses, just applied here instead of at the {@code EngineFactory} level where an
     * earlier attempt at this failed -- see this class's own git history / &sect;6.4's note on that
     * attempt for why materializing too late, after {@code StoreRecovery} already created a local
     * translog, leaves a stale translog-UUID reference behind). Because {@code EXISTING_STORE}
     * recovery assumes a local translog already exists and never creates one on its own (unlike
     * {@code EMPTY_STORE}'s {@code recoverEmptyStore}), this method also bootstraps and associates
     * a fresh one here, at the one point in the whole recovery sequence where doing so is still
     * correct: after the manifest's real segment files are in place (so the new translog is
     * associated with the actual recovered commit, not a stale or trivial one) but before {@code
     * StoreRecovery} does anything else that would assume translog files were already there.
     *
     * @param indexShard the shard whose local store is missing and being recovered from the shard's last durable manifest
     * @param store the (empty) local store to materialize the shard's last durable manifest into
     * @return {@code true} if the store was materialized and a new local translog bootstrapped; {@code false} if
     *         no materializer is configured or no durable manifest exists yet to recover from
     * Deliberately does <em>not</em> call {@code store.bootstrapNewHistory()}: the manifest carries
     * this shard's real prior history (its own {@code maxSeqNo}/{@code localCheckpoint}), which
     * must be preserved, not reset -- {@code bootstrapNewHistory} is for a genuinely fresh index
     * with no real history at all, which this is not.
     *
     * <p>Anything durably written to the WAL <em>after</em> this manifest -- the actual gap this
     * method doesn't attempt to close -- is what {@link ObjectStoreWriterEngine#engineRecoveryOperations()}
     * closes next, once the engine itself opens on top of what this method just materialized.
     */
    @Override
    public boolean recoverMissingLocalStore(IndexShard indexShard, Store store) throws IOException {
        if (materializer == null) {
            return false;
        }
        String indexUuid = indexShard.shardId().getIndex().getUUID();
        int shardId = indexShard.shardId().getId();
        Optional<CommitManifest> latestManifest = headPublisher.readLatestManifest(indexUuid, shardId);
        if (latestManifest.isEmpty()) {
            return false;
        }
        CommitManifest manifest = latestManifest.get();
        Directory directory = store.directory();
        materializer.materialize(manifest, directory);

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
     * {@code true}: {@link ObjectStoreWriterEngine} publishes every commit as an object-store
     * manifest (rfc-serverless-opensearch.md &sect;8) referencing this shard's own segment files
     * directly -- that manifest publication already <em>is</em> this shard's durable remote copy,
     * so core's separate {@code RemoteStoreRefreshListener} upload path (engaged whenever {@code
     * index.remote_store.enabled: true}, which this plugin's own reader-shard support requires
     * regardless, per rfc-serverless-opensearch.md &sect;18 risk #10) would otherwise upload the
     * same segment bytes a second time into a remote-store repository nothing in this plugin's own
     * reader/GC/retention paths ever reads back from -- confirmed wasted work, not a hypothesis, by
     * {@code ServerlessStorageSearchOnlyReplicaIT}.
     */
    @Override
    public boolean ownsRemoteSegmentDurability() {
        return true;
    }
}
