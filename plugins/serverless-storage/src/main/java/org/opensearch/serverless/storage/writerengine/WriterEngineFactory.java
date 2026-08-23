/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineCreationFailureException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.wal.DedicatedWalGcConfig;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalGcSchedulerTask;
import org.opensearch.serverless.storage.wal.WalShardRegistry;

/** {@link EngineFactory} for writer shards: produces {@link ObjectStoreWriterEngine}s. */
public final class WriterEngineFactory implements EngineFactory {

    private final ObjectStoreCommitHeadPublisher headPublisher;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final PitrRetentionConfig pitrRetentionConfig;
    private final WalChunkService walChunkService;
    private final org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider;
    private final ShardActivityRegistry activityRegistry;
    private final DedicatedWalGcConfig dedicatedWalGcConfig;
    private final long publicationRateLimitMillis;
    private final WriterPublicationNotifier publicationNotifier;
    private final org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry;

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
     * Creates a factory with WAL-mirrored records left unencrypted, delegating to the fuller
     * constructor with {@code encryptionKeyProvider} set to {@code null}.
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
        this(headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService, null);
    }

    /**
     * Creates a fully-configured factory, storing every dependency for engines it will later
     * produce via {@link #newReadWriteEngine}, leaving produced engines unregistered with any
     * {@link ShardActivityRegistry} (delegates to the fuller constructor with {@code null}).
     *
     * @param headPublisher publishes commits and manages lease acquisition/renewal for produced engines
     * @param shardDirectory the directory-registry entry produced engines report themselves into
     * @param localNodeId the id of the node produced engines activate on
     * @param pitrRetentionConfig {@code null} disables PITR retention reconciliation on produced engines; non-null enables it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
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
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider
    ) {
        this(headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService, encryptionKeyProvider, null, null);
    }

    /**
     * Creates a fully-configured factory, storing every dependency for engines it will later
     * produce via {@link #newReadWriteEngine}, including a {@link ShardActivityRegistry} produced
     * engines register themselves into, leaving dedicated-WAL-stream retention unconfigured
     * (delegates to the fuller constructor with {@code dedicatedWalGcConfig} set to {@code null}).
     *
     * <p>Every parameter is as documented on the narrower overload above.
     *
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
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        ShardActivityRegistry activityRegistry
    ) {
        this(
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
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
     * <p>Every other parameter is as documented on the narrower overload above.
     *
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
     * <p>Every other parameter is as documented on the narrower overload above.
     *
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
     * <p>Every other parameter is as documented on the narrower overload above.
     *
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
            encryptionKeyProvider,
            activityRegistry,
            dedicatedWalGcConfig,
            publicationRateLimitMillis,
            publicationNotifier,
            null
        );
    }

    /**
     * Creates a fully-configured factory, additionally able to produce engines that create
     * engine-native snapshots -- see {@link ObjectStoreWriterEngine#attemptEngineNativeSnapshot}.
     *
     * <p>Consuming such a snapshot again (restore, and release on delete) is deliberately not this
     * factory's business and never was on this type: both can run with no live engine, and one of
     * them with no live shard at all. They live on {@link ObjectStoreShardRecoveryStrategy} /
     * {@link EngineNativeSnapshotSupport} instead. The same move took store-population recovery
     * (cross-node failover, in-place split/merge) off this factory: all of it runs before any
     * engine this factory could build exists.
     *
     * <p>Every other parameter is as documented on the narrower overload above.
     *
     * @param pinRegistry {@code null} disables engine-native snapshot <em>creation</em> on produced
     *                    engines (same shape as every other optional feature in this plugin); a
     *                    snapshot attempt then fails loudly rather than silently falling back to the
     *                    classic copy-based path -- see {@code ObjectStoreWriterEngine#pinRegistry}'s
     *                    own javadoc.
     */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        ShardActivityRegistry activityRegistry,
        DedicatedWalGcConfig dedicatedWalGcConfig,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier,
        org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry
    ) {
        this.headPublisher = headPublisher;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.pitrRetentionConfig = pitrRetentionConfig;
        this.walChunkService = walChunkService;
        this.encryptionKeyProvider = encryptionKeyProvider;
        this.activityRegistry = activityRegistry;
        this.dedicatedWalGcConfig = dedicatedWalGcConfig;
        this.publicationRateLimitMillis = publicationRateLimitMillis;
        this.publicationNotifier = publicationNotifier;
        this.pinRegistry = pinRegistry;
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

}
