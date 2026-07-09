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
import org.opensearch.serverless.storage.wal.WalChunkService;

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
     * produce via {@link #newReadWriteEngine}.
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
        this.headPublisher = headPublisher;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.pitrRetentionConfig = pitrRetentionConfig;
        this.walChunkService = walChunkService;
        this.materializer = materializer;
        this.encryptionKeyProvider = encryptionKeyProvider;
    }

    /** Exposed for tests (including from other packages, e.g. {@code ServerlessStoragePluginTests}) -- not part of this class's public contract. */
    public org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProviderForTesting() {
        return encryptionKeyProvider;
    }

    @Override
    public Engine newReadWriteEngine(EngineConfig config) {
        try {
            // ObjectStoreWriterEngine reports to the directory tier itself, both on activation and
            // on a fixed refresh schedule for as long as it stays open (rfc-serverless-metadata-plane.md
            // &sect;9 activation path step 3, &sect;13 risk #1 metastability mitigation) -- see its
            // javadoc. This is a hint, not a fact, so a report failure is never worth failing engine
            // construction over. It also owns PITR retention reconciliation when pitrRetentionConfig
            // is configured (null disables it, same shape as encryption), and WAL mirroring when
            // walChunkService is configured (null disables it the same way).
            return new ObjectStoreWriterEngine(
                config,
                headPublisher,
                shardDirectory,
                localNodeId,
                pitrRetentionConfig,
                walChunkService,
                encryptionKeyProvider
            );
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
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
}
