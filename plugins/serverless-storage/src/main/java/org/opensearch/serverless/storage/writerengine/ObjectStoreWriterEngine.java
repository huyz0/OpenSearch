/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.DocumentIndexWriter;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * An {@link InternalEngine} whose durability is object-store-native (rfc-serverless-opensearch.md
 * &sect;5/&sect;6): local Lucene commits happen exactly as they do today (unchanged recovery,
 * merge, and translog-file semantics), but every commit is additionally packaged into a segment
 * bundle and published as the shard's head via {@link ObjectStoreCommitHeadPublisher} immediately
 * after the local Lucene commit completes.
 *
 * <p>If this writer has been fenced out (a different primary term now holds the shard's head --
 * e.g. its lease expired and another node took over) the local Lucene commit has already happened
 * by the time that's discovered, since object-store publication can only happen after the commit
 * whose files it packages exists. The commit stays valid locally (it does not corrupt anything),
 * but the engine is failed so this node stops serving as the shard's writer, matching how any
 * other fatal engine condition (e.g. a translog failure) is handled today.
 *
 * <p>Besides reporting to the {@link ShardDirectory} once on activation, this engine also refreshes
 * that entry on a fixed schedule for as long as it stays open (rfc-serverless-metadata-plane.md
 * &sect;13 risk #1, "metastability of the directory tier"): relying only on on-demand re-report
 * after a miss means a burst of expiries can turn into a burst of {@code ShardStateStore} reads all
 * at once, which is exactly the correlated-load failure mode that risk describes. Refreshing well
 * before the entry's TTL elapses keeps the entry's staleness bounded by the refresh interval
 * instead of by traffic patterns, and spreads the read load out over time instead of clumping it at
 * expiry.
 *
 * <p>If a {@link PitrRetentionConfig} is supplied, this engine also owns a {@link
 * PitrRetentionSchedulerTask} for as long as it stays open, keeping the shard's {@code "pitr"}
 * durable pins in line with the configured retention window (rfc-serverless-opensearch.md
 * &sect;16 Phase 4.6) -- {@code null} disables PITR retention for this shard entirely, matching
 * how {@code encryptionKeyProvider} being {@code null} means "encryption is off" elsewhere in
 * this plugin.
 */
public class ObjectStoreWriterEngine extends InternalEngine {

    /** How long a directory entry for a writer shard is trusted before it's treated as stale. */
    private static final long DIRECTORY_ENTRY_TTL_MILLIS = 60_000L;

    /**
     * Refresh well inside the TTL, not at its edge: a refresh that only just beats expiry still
     * leaves a window where a slow/delayed scheduler tick lets the entry lapse anyway.
     */
    private static final TimeValue DIRECTORY_REFRESH_INTERVAL = TimeValue.timeValueMillis(DIRECTORY_ENTRY_TTL_MILLIS / 3);

    /**
     * PITR reconciliation lists and reads every manifest the shard has ever written -- far heavier
     * than the directory refresh -- and the retention window moves far more slowly than a
     * directory entry's TTL, so this runs on its own, much longer interval.
     */
    private static final TimeValue PITR_RECONCILE_INTERVAL = TimeValue.timeValueMinutes(5);

    private final ObjectStoreCommitHeadPublisher headPublisher;
    private final String indexUuid;
    private final int shardId;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final Scheduler.Cancellable directoryRefreshTask;
    private final PitrRetentionSchedulerTask pitrRetentionTask;

    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, null);
    }

    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig
    ) {
        super(engineConfig);
        this.headPublisher = headPublisher;
        this.indexUuid = engineConfig.getShardId().getIndex().getUUID();
        this.shardId = engineConfig.getShardId().getId();
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        // Report once synchronously so the shard is discoverable immediately on activation, rather
        // than waiting out the first refresh interval; scheduleWithFixedDelay's first execution
        // only happens after DIRECTORY_REFRESH_INTERVAL elapses, not on registration.
        refreshDirectoryEntry();
        this.directoryRefreshTask = engineConfig.getThreadPool()
            .scheduleWithFixedDelay(this::refreshDirectoryEntry, DIRECTORY_REFRESH_INTERVAL, ThreadPool.Names.GENERIC);
        this.pitrRetentionTask = pitrRetentionConfig == null
            ? null
            : new PitrRetentionSchedulerTask(
                engineConfig.getThreadPool(),
                PITR_RECONCILE_INTERVAL,
                indexUuid,
                shardId,
                pitrRetentionConfig.manifestStore(),
                pitrRetentionConfig.pinRegistry(),
                pitrRetentionConfig.windowMillis()
            );
    }

    private void refreshDirectoryEntry() {
        shardDirectory.report(
            indexUuid,
            shardId,
            new ShardDirectoryEntry(
                localNodeId,
                ShardRole.WRITER,
                engineConfig.getPrimaryTermSupplier().getAsLong(),
                0,
                System.currentTimeMillis() + DIRECTORY_ENTRY_TTL_MILLIS
            )
        );
    }

    @Override
    public void close() throws IOException {
        directoryRefreshTask.cancel();
        if (pitrRetentionTask != null) {
            pitrRetentionTask.close();
        }
        super.close();
    }

    @Override
    protected void commitIndexWriter(final DocumentIndexWriter writer, final String translogUUID) throws IOException {
        super.commitIndexWriter(writer, translogUUID);

        try {
            SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
            long primaryTerm = engineConfig.getPrimaryTermSupplier().getAsLong();
            long maxSeqNo = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.MAX_SEQ_NO));
            long localCheckpoint = Long.parseLong(segmentInfos.userData.get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));

            boolean published = headPublisher.publishCommitAsHead(
                store.directory(),
                segmentInfos,
                indexUuid,
                shardId,
                primaryTerm,
                maxSeqNo,
                localCheckpoint,
                new WalPosition(String.valueOf(primaryTerm), 0),
                0,
                PruningStats.empty()
            );
            if (published == false) {
                throw new EngineException(
                    engineConfig.getShardId(),
                    "fenced out publishing local commit (segments generation "
                        + segmentInfos.getGeneration()
                        + ") under term "
                        + primaryTerm
                );
            }
        } catch (final EngineException ex) {
            failEngine("object-store commit publication fenced out", ex);
            throw ex;
        } catch (final Exception ex) {
            failEngine("object-store commit publication failed", ex);
            throw new IOException(ex);
        }
    }
}
