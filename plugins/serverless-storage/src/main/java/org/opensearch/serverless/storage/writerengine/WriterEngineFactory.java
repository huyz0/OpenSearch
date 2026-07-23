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
import java.util.ArrayList;
import java.util.List;
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
    private final long publicationRateLimitMillis;
    private final WriterPublicationNotifier publicationNotifier;
    private final java.util.function.IntFunction<org.opensearch.common.blobstore.BlobContainer> siblingShardBlobContainerResolver;
    private final org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry;
    private final EngineNativeSnapshotSupport engineNativeSnapshotSupport;

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
        this(
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            materializer,
            encryptionKeyProvider,
            activityRegistry,
            dedicatedWalGcConfig,
            0L
        );
    }

    /**
     * Creates a fully-configured factory, additionally rate-limiting how often an
     * externally-triggered refresh may trigger a publish on produced engines (rfc-serverless-opensearch.md
     * &sect;8) -- see {@link ObjectStoreWriterEngine}'s own matching constructor javadoc for the
     * full mechanism.
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
     * @param publicationRateLimitMillis non-positive (the default) disables rate limiting on
     *                                   produced engines; positive is the minimum real milliseconds
     *                                   between two refresh-triggered publish attempts on any one
     *                                   produced engine.
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
        DedicatedWalGcConfig dedicatedWalGcConfig,
        long publicationRateLimitMillis
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
            dedicatedWalGcConfig,
            publicationRateLimitMillis,
            null
        );
    }

    /**
     * Creates a fully-configured factory, additionally notifying every produced engine's reader
     * copies after each publish (rfc-serverless-opensearch.md &sect;8) -- see {@link
     * ObjectStoreWriterEngine}'s own matching constructor javadoc for the full mechanism.
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
     * @param publicationRateLimitMillis non-positive (the default) disables rate limiting on
     *                                   produced engines; positive is the minimum real milliseconds
     *                                   between two refresh-triggered publish attempts on any one
     *                                   produced engine.
     * @param publicationNotifier {@code null} disables writer-side publication notification
     *                            entirely (same shape as every other optional feature in this
     *                            plugin); non-null notifies every produced engine's reader copies
     *                            after each successful publish.
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
        DedicatedWalGcConfig dedicatedWalGcConfig,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier
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
            dedicatedWalGcConfig,
            publicationRateLimitMillis,
            publicationNotifier,
            null
        );
    }

    /**
     * Creates a fully-configured factory, additionally able to produce engines that can recover an
     * in-place split child shard (dynamic-partitioning-plan.md Phase 0) -- see {@link
     * ObjectStoreWriterEngine}'s own matching constructor javadoc for the full mechanism.
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
     * @param publicationRateLimitMillis non-positive (the default) disables rate limiting on
     *                                   produced engines; positive is the minimum real milliseconds
     *                                   between two refresh-triggered publish attempts on any one
     *                                   produced engine.
     * @param publicationNotifier {@code null} disables writer-side publication notification
     *                            entirely (same shape as every other optional feature in this
     *                            plugin); non-null notifies every produced engine's reader copies
     *                            after each successful publish.
     * @param siblingShardBlobContainerResolver {@code null} disables in-place split recovery on
     *                                          produced engines entirely (same shape as every other
     *                                          optional feature in this plugin); non-null resolves
     *                                          an arbitrary sibling shard ID's own blob container
     *                                          within the same index.
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
        DedicatedWalGcConfig dedicatedWalGcConfig,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier,
        java.util.function.IntFunction<org.opensearch.common.blobstore.BlobContainer> siblingShardBlobContainerResolver
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
            dedicatedWalGcConfig,
            publicationRateLimitMillis,
            publicationNotifier,
            siblingShardBlobContainerResolver,
            null,
            null
        );
    }

    /**
     * Creates a fully-configured factory, additionally able to produce engines that support
     * engine-native snapshots -- see {@link ObjectStoreWriterEngine#attemptEngineNativeSnapshot} and
     * {@link EngineNativeSnapshotSupport}.
     *
     * <p>Every other parameter is as documented on the narrower overload above.
     *
     * @param pinRegistry {@code null} disables engine-native snapshot <em>creation</em> on produced
     *                    engines (same shape as every other optional feature in this plugin); a
     *                    snapshot attempt then fails loudly rather than silently falling back to the
     *                    classic copy-based path -- see {@code ObjectStoreWriterEngine#pinRegistry}'s
     *                    own javadoc.
     * @param engineNativeSnapshotSupport {@code null} disables engine-native snapshot
     *                                    <em>restore</em> on produced engines; non-null is the
     *                                    single, node-wide instance also registered under {@link
     *                                    EngineNativeSnapshotSupport#ENGINE_ID} for the release path
     *                                    (see {@code ServerlessStoragePlugin}'s own wiring).
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
        DedicatedWalGcConfig dedicatedWalGcConfig,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier,
        java.util.function.IntFunction<org.opensearch.common.blobstore.BlobContainer> siblingShardBlobContainerResolver,
        org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry,
        EngineNativeSnapshotSupport engineNativeSnapshotSupport
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
        this.publicationRateLimitMillis = publicationRateLimitMillis;
        this.publicationNotifier = publicationNotifier;
        this.siblingShardBlobContainerResolver = siblingShardBlobContainerResolver;
        this.pinRegistry = pinRegistry;
        this.engineNativeSnapshotSupport = engineNativeSnapshotSupport;
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
                dedicatedWalGcSchedulerTask,
                publicationRateLimitMillis,
                publicationNotifier,
                pinRegistry
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
     * The store-population half of an in-place split's child shard activation
     * (dynamic-partitioning-plan.md Phase 0), analogous to {@link #recoverMissingLocalStore} but for
     * a shard with no manifest of its own <em>yet</em> rather than one whose manifest merely isn't
     * locally materialized. Called by {@code StoreRecovery#internalRecoverFromStore} before this
     * shard's local translog is created or its engine is opened -- the same careful ordering {@link
     * #recoverMissingLocalStore} itself requires (see this class's earlier "Task 19" note in
     * dynamic-partitioning-progress.md for the bug this ordering fixes: a first attempt at this
     * attached the child to the parent's data only in the <em>object store</em>, after the engine had
     * already opened against an empty local Lucene index, leaving the two permanently out of sync).
     *
     * <p>In order: resolves this child's parent and hash range from {@code SplitShardsMetadata}
     * (core cluster metadata, reachable via {@code indexShard.indexSettings()} without any
     * additional plumbing), reuses {@link org.opensearch.serverless.storage.clone.ShardCloner#clone}'s
     * existing zero-copy recipe verbatim -- retargeted from "brand-new index" to "same index, sibling
     * shard ID already reserved by core's {@code SplitShardsMetadata}," needing no new manifest-write
     * logic since {@code CloneLineage}'s {@code (sourceIndexUuid, sourceShardId)} shape already
     * tolerates a same-index source -- to attach this child's manifest to the parent's data, then
     * materializes that manifest into {@code store}'s local Lucene commit and creates a matching
     * local translog, exactly as {@link #recoverMissingLocalStore} does for its own case.
     *
     * @param indexShard the child shard whose local store is empty and being attached to its parent's data
     * @param store the (empty) local store to materialize the newly-cloned manifest into
     * @return {@code true} if the store was materialized and a new local translog bootstrapped;
     *         {@code false} if no resolver is configured, or {@code indexShard} isn't a
     *         recognized in-progress split child (e.g. a retried recovery after the split already
     *         committed via {@code MetadataInPlaceSplitShardCommitService})
     */
    @Override
    public boolean recoverInPlaceSplitLocalStore(IndexShard indexShard, Store store) throws IOException {
        if (siblingShardBlobContainerResolver == null) {
            return false;
        }

        String indexUuid = indexShard.shardId().getIndex().getUUID();
        int childShardId = indexShard.shardId().getId();
        org.opensearch.cluster.metadata.IndexMetadata indexMetadata = indexShard.indexSettings().getIndexMetadata();
        org.opensearch.common.collect.Tuple<Integer, org.opensearch.cluster.metadata.ShardRange> parentAndRange = indexMetadata
            .getSplitShardsMetadata()
            .getParentAndRangeOfChild(childShardId);
        if (parentAndRange == null) {
            return false;
        }
        int parentShardId = parentAndRange.v1();
        org.opensearch.cluster.metadata.ShardRange childRange = parentAndRange.v2();

        org.opensearch.common.blobstore.BlobContainer parentContainer = resolveSiblingShardBlobContainer(parentShardId);
        org.opensearch.serverless.storage.manifest.BlobContainerManifestStore parentManifestStore =
            new org.opensearch.serverless.storage.manifest.BlobContainerManifestStore(parentContainer);
        org.opensearch.serverless.storage.shardstate.ShardStateStore parentShardStateStore =
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(parentContainer);
        org.opensearch.serverless.storage.retention.DurablePinRegistry parentPinRegistry =
            new org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry(parentContainer);

        org.opensearch.common.blobstore.BlobContainer childContainer = resolveSiblingShardBlobContainer(childShardId);
        org.opensearch.serverless.storage.manifest.BlobContainerManifestStore childManifestStore =
            new org.opensearch.serverless.storage.manifest.BlobContainerManifestStore(childContainer);
        org.opensearch.serverless.storage.shardstate.ShardStateStore childShardStateStore =
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(childContainer);
        org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore childLineageStore =
            new org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore(childContainer);
        org.opensearch.serverless.storage.resharding.BlobContainerInPlaceSplitRangeStore childRangeStore =
            new org.opensearch.serverless.storage.resharding.BlobContainerInPlaceSplitRangeStore(childContainer);
        org.opensearch.serverless.storage.resharding.InPlaceSplitRangeDescriptor descriptor =
            new org.opensearch.serverless.storage.resharding.InPlaceSplitRangeDescriptor(
                parentShardId,
                childRange.start(),
                childRange.end()
            );

        org.opensearch.serverless.storage.clone.ShardCloner.clone(
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
        // factory's own `materializer` field (which is fixed to THIS shard's own, still-empty
        // container, appropriate for #recoverMissingLocalStore's very different "re-read my own past
        // publish" case, not this one). The parent itself may be a clone/split child too (a
        // split-of-a-split), in which case ITS manifest can still reference bundles that only
        // physically exist further back in the lineage -- walking the full chain (mirroring {@code
        // ServerlessStoragePlugin#chainedBundleReadPath}) rather than reading only `parentContainer`
        // avoids a NoSuchFileException on a bundle that legitimately exists, just further upstream.
        List<org.opensearch.common.blobstore.BlobContainer> parentLineageChain = org.opensearch.serverless.storage.clone.ShardCloner
            .resolveLineageChain(parentContainer, indexUuid, parentShardId, (lineageIndexUuid, lineageShardId) -> {
                if (lineageIndexUuid.equals(indexUuid) == false) {
                    throw new IOException(
                        "in-place split recovery: lineage of parent shard "
                            + indexUuid
                            + "/"
                            + parentShardId
                            + " unexpectedly crosses into a different index ["
                            + lineageIndexUuid
                            + "] -- in-place split/merge lineage must stay within one index"
                    );
                }
                return resolveSiblingShardBlobContainer(lineageShardId);
            });
        List<org.opensearch.serverless.storage.format.BundleFileReader> parentReaders = new ArrayList<>(parentLineageChain.size());
        for (org.opensearch.common.blobstore.BlobContainer container : parentLineageChain) {
            parentReaders.add(new org.opensearch.serverless.storage.format.BlobContainerBundleStore(container));
        }
        ObjectStoreCommitMaterializer parentBundleMaterializer = new ObjectStoreCommitMaterializer(
            org.opensearch.serverless.storage.clone.FallbackBundleFileReader.chain(parentReaders)
        );
        parentBundleMaterializer.materialize(manifest, directory);

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
     * #recoverInPlaceSplitLocalStore}, and, like it, called by {@code
     * StoreRecovery#internalRecoverFromStore} before this shard's local translog is created or its
     * engine is opened (the same stale-translog-UUID ordering constraint, see {@link
     * #recoverMissingLocalStore}'s own javadoc).
     *
     * <p><b>Where the children come from.</b> Unlike a split child (which recovers <em>during</em> the
     * in-progress window and can still resolve its own parent and range from {@code
     * SplitShardsMetadata}), a merge's parent recovers <em>after</em> {@code
     * MetadataInPlaceMergeShardService} has already de-committed the split in the same cluster-state
     * update that revived this parent -- so the metadata no longer records which children it came from.
     * The retired children's {@link org.opensearch.cluster.metadata.ShardRange}s (each carrying its own
     * child shard id) are therefore carried on the recovery source itself ({@link
     * org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource}), which this hook
     * reads back from {@code indexShard.recoveryState()}.
     *
     * <p><b>What it does.</b> For each child: resolves its blob container, reads its current published
     * manifest (via its own shard-head), and materializes it through a fallback read path (the child's
     * own container first, the parent's as fallback for a pristine child that never published a
     * post-split commit and so still references the parent's cloned-by-reference base bundle). It then
     * folds every child's authoritative, range-filtered document slice into {@code store}'s directory
     * via {@link org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger#merge} (a real
     * {@link org.apache.lucene.index.IndexWriter#addIndexes} of the filtered readers -- see that class's
     * javadoc for why the union needs no bespoke delete/version reconciliation), and creates a matching
     * local translog at the merged checkpoint. The revived parent's engine takes over from there,
     * republishing this merged commit to the parent's own container on its first flush.
     *
     * @param indexShard the parent shard being revived from its (now-retired) children's data.
     * @param store the (empty) local store to fold the merged commit into.
     * @return {@code true} if the store was materialized and a new local translog bootstrapped;
     *         {@code false} if no resolver is configured, or this shard is not recovering via an
     *         {@link org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource}
     *         carrying children (e.g. the empty-children {@code INSTANCE} used outside a real merge).
     */
    @Override
    public boolean recoverInPlaceMergeLocalStore(IndexShard indexShard, Store store) throws IOException {
        if (siblingShardBlobContainerResolver == null) {
            return false;
        }
        org.opensearch.cluster.routing.RecoverySource recoverySource = indexShard.recoveryState().getRecoverySource();
        if (recoverySource instanceof org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource == false) {
            return false;
        }
        List<org.opensearch.cluster.metadata.ShardRange> children =
            ((org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource) recoverySource).children();
        if (children.isEmpty()) {
            return false;
        }

        String indexUuid = indexShard.shardId().getIndex().getUUID();
        int parentShardId = indexShard.shardId().getId();
        org.opensearch.common.blobstore.BlobContainer parentContainer = resolveSiblingShardBlobContainer(parentShardId);
        // Each child's own clone lineage points directly at the parent (one hop -- see
        // #recoverInPlaceSplitLocalStore, which writes exactly that), so the child's own container is
        // always tried first. But the PARENT itself may be a clone/split child too (a split-of-a-split
        // later merged back), in which case its manifest can still reference bundles that only
        // physically exist further back in the lineage than `parentContainer` alone -- walk the full
        // chain once, shared by every child below, mirroring #recoverInPlaceSplitLocalStore's fix.
        List<org.opensearch.common.blobstore.BlobContainer> parentLineageChain = org.opensearch.serverless.storage.clone.ShardCloner
            .resolveLineageChain(parentContainer, indexUuid, parentShardId, (lineageIndexUuid, lineageShardId) -> {
                if (lineageIndexUuid.equals(indexUuid) == false) {
                    throw new IOException(
                        "in-place merge recovery: lineage of parent shard "
                            + indexUuid
                            + "/"
                            + parentShardId
                            + " unexpectedly crosses into a different index ["
                            + lineageIndexUuid
                            + "] -- in-place split/merge lineage must stay within one index"
                    );
                }
                return resolveSiblingShardBlobContainer(lineageShardId);
            });
        List<org.opensearch.serverless.storage.format.BundleFileReader> parentReaders = new ArrayList<>(parentLineageChain.size());
        for (org.opensearch.common.blobstore.BlobContainer container : parentLineageChain) {
            parentReaders.add(new org.opensearch.serverless.storage.format.BlobContainerBundleStore(container));
        }

        List<org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger.MergeChild> mergeChildren = new ArrayList<>(children.size());
        for (org.opensearch.cluster.metadata.ShardRange childRange : children) {
            int childShardId = childRange.shardId();
            org.opensearch.common.blobstore.BlobContainer childContainer = resolveSiblingShardBlobContainer(childShardId);
            org.opensearch.serverless.storage.shardstate.ShardStateStore childShardStateStore =
                new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(childContainer);
            org.opensearch.serverless.storage.shardstate.VersionedShardHead childHead = childShardStateStore.get(indexUuid, childShardId)
                .orElseThrow(
                    () -> new IOException("in-place merge: child shard " + indexUuid + "/" + childShardId + " has no published head to merge")
                );
            org.opensearch.serverless.storage.manifest.BlobContainerManifestStore childManifestStore =
                new org.opensearch.serverless.storage.manifest.BlobContainerManifestStore(childContainer);
            CommitManifest childManifest = childManifestStore.readManifest(
                childHead.head().primaryTerm(),
                childHead.head().latestManifestGeneration()
            );

            List<org.opensearch.serverless.storage.format.BundleFileReader> childDelegates = new ArrayList<>(parentReaders.size() + 1);
            childDelegates.add(new org.opensearch.serverless.storage.format.BlobContainerBundleStore(childContainer));
            childDelegates.addAll(parentReaders);
            org.opensearch.serverless.storage.format.BundleFileReader childReadPath =
                new org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger.FallbackBundleFileReader(childDelegates);
            mergeChildren.add(
                new org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger.MergeChild(childManifest, childReadPath, childRange)
            );
        }

        long mergedMaxSeqNo = org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger.merge(mergeChildren, store.directory());

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
     * Unwraps {@link #siblingShardBlobContainerResolver}'s {@link java.io.UncheckedIOException}
     * back into a checked one -- it has no checked-exception escape hatch (it's an {@code
     * IntFunction}), but {@code ServerlessStoragePlugin}'s own resolver wraps {@link IOException}
     * this way, and this method's own contract (via {@link EngineFactory#recoverInPlaceSplitLocalStore})
     * is honestly checked.
     */
    private org.opensearch.common.blobstore.BlobContainer resolveSiblingShardBlobContainer(int shardId) throws IOException {
        try {
            return siblingShardBlobContainerResolver.apply(shardId);
        } catch (java.io.UncheckedIOException e) {
            throw e.getCause();
        }
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

    /**
     * Delegates to {@link #engineNativeSnapshotSupport} -- {@code null} disables this (returns
     * {@code false}, same shape as every other optional feature in this plugin), which core treats
     * as "engine declined to recover the snapshot it originally produced," an
     * {@code IndexShardRecoveryException} surfaced as this shard's own recovery failure.
     */
    @Override
    public boolean recoverFromEngineNativeSnapshot(IndexShard indexShard, Store store, byte[] snapshotPointer) throws IOException {
        if (engineNativeSnapshotSupport == null) {
            return false;
        }
        return engineNativeSnapshotSupport.recoverFromEngineNativeSnapshot(indexShard, store, snapshotPointer);
    }

    /**
     * {@code engineNativeSnapshotSupport != null} -- must stay in lockstep with {@link
     * #recoverFromEngineNativeSnapshot}'s own null check: core's {@code StoreRecovery} uses this
     * method to decide whether it's even worth probing the repository for an engine-native snapshot
     * blob at all before calling {@link #recoverFromEngineNativeSnapshot}, so this must return
     * {@code true} in exactly the cases that method can return something other than {@code false}.
     */
    @Override
    public boolean supportsEngineNativeSnapshots() {
        return engineNativeSnapshotSupport != null;
    }
}
