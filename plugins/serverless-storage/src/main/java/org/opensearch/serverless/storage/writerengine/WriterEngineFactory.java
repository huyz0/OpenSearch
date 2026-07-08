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
import org.opensearch.serverless.storage.wal.WalChunkService;

/** {@link EngineFactory} for writer shards: produces {@link ObjectStoreWriterEngine}s. */
public final class WriterEngineFactory implements EngineFactory {

    private final ObjectStoreCommitHeadPublisher headPublisher;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final PitrRetentionConfig pitrRetentionConfig;
    private final WalChunkService walChunkService;

    public WriterEngineFactory(ObjectStoreCommitHeadPublisher headPublisher, ShardDirectory shardDirectory, String localNodeId) {
        this(headPublisher, shardDirectory, localNodeId, null, null);
    }

    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig
    ) {
        this(headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, null);
    }

    /** @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin. */
    public WriterEngineFactory(
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService
    ) {
        this.headPublisher = headPublisher;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.pitrRetentionConfig = pitrRetentionConfig;
        this.walChunkService = walChunkService;
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
            return new ObjectStoreWriterEngine(config, headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new EngineCreationFailureException(config.getShardId(), "failed to create object-store writer engine", e);
        }
    }
}
