/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Function;

/**
 * Opens a reader-shard engine directly from a published {@link CommitManifest}
 * (rfc-serverless-opensearch.md &sect;5/&sect;6): never a full local copy recovered by peer
 * transfer, never promotable to a writer -- exactly the reader-shard role's contract.
 *
 * <p>This does not need to be a wildly different {@code Engine} subclass: once {@link
 * ObjectStoreCommitMaterializer} has populated the engine's {@link EngineConfig#getStore()}
 * directory with the manifest's files, that directory holds an ordinary valid Lucene commit, and
 * {@link ReadOnlyEngine} already implements everything a read-only shard needs (search, get,
 * completion stats, segment listing, refusing writes) against exactly that. Reusing it here means
 * the object-store-specific work is confined to the two genuinely novel pieces -- materializing a
 * manifest into a directory, and refreshing this shard's {@link ShardDirectory} entry -- rather
 * than re-deriving several thousand lines of tested read-path behavior.
 *
 * <p>Refreshing to a newer manifest generation (reopening once a writer publishes a new commit,
 * per the notification path in rfc-serverless-metadata-plane.md &sect;5) is not implemented here:
 * each instance materializes and opens one fixed generation for the lifetime of the engine,
 * matching how {@link ReadOnlyEngine} itself is a one-shot immutable view. Making a reader shard
 * advance to new generations without a full engine reopen is separate, larger work.
 *
 * <p>What this class does own is the directory-tier refresh (rfc-serverless-metadata-plane.md
 * &sect;13 risk #1, "metastability of the directory tier"), the same mitigation {@code
 * ObjectStoreWriterEngine} applies on the writer side: reporting once on activation and then again
 * on a fixed schedule for as long as the engine stays open, so this shard's directory entry doesn't
 * depend on traffic to stay fresh, and the refresh task is canceled cleanly on {@link #close()}.
 *
 * <p>If a {@link ReaderShardAdmissionController} is supplied, {@link #open} acquires a permit from
 * it before materializing anything, and this engine releases that permit in {@link #close()} --
 * see that class's javadoc for what this coarse cap does and does not protect against
 * (rfc-serverless-opensearch.md &sect;18 risk #3). {@code null} disables it entirely, matching how
 * every other optional feature in this plugin is threaded through as an absent value rather than a
 * separate on/off flag.
 */
public final class ObjectStoreReaderEngine extends ReadOnlyEngine {

    /** How long a directory entry for a reader shard is trusted before it's treated as stale. */
    private static final long DIRECTORY_ENTRY_TTL_MILLIS = 60_000L;

    /**
     * Refresh well inside the TTL, not at its edge: a refresh that only just beats expiry still
     * leaves a window where a slow/delayed scheduler tick lets the entry lapse anyway.
     */
    private static final TimeValue DIRECTORY_REFRESH_INTERVAL = TimeValue.timeValueMillis(DIRECTORY_ENTRY_TTL_MILLIS / 3);

    private final String indexUuid;
    private final int shardId;
    private final long primaryTerm;
    private final long manifestGeneration;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final Scheduler.Cancellable directoryRefreshTask;
    private final ReaderShardAdmissionController admissionController;

    private ObjectStoreReaderEngine(
        EngineConfig config,
        SeqNoStats seqNoStats,
        long primaryTerm,
        long manifestGeneration,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController
    ) {
        super(config, seqNoStats, new TranslogStats(), true, Function.identity(), false);
        this.indexUuid = config.getShardId().getIndex().getUUID();
        this.shardId = config.getShardId().getId();
        this.primaryTerm = primaryTerm;
        this.manifestGeneration = manifestGeneration;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.admissionController = admissionController;
        // Report once synchronously so the shard is discoverable immediately on activation, rather
        // than waiting out the first refresh interval; scheduleWithFixedDelay's first execution
        // only happens after DIRECTORY_REFRESH_INTERVAL elapses, not on registration.
        refreshDirectoryEntry();
        this.directoryRefreshTask = config.getThreadPool()
            .scheduleWithFixedDelay(this::refreshDirectoryEntry, DIRECTORY_REFRESH_INTERVAL, ThreadPool.Names.GENERIC);
    }

    private void refreshDirectoryEntry() {
        shardDirectory.report(
            indexUuid,
            shardId,
            new ShardDirectoryEntry(
                localNodeId,
                ShardRole.READER,
                primaryTerm,
                manifestGeneration,
                System.currentTimeMillis() + DIRECTORY_ENTRY_TTL_MILLIS
            )
        );
    }

    @Override
    public void close() throws IOException {
        directoryRefreshTask.cancel();
        if (admissionController != null) {
            admissionController.release();
        }
        super.close();
    }

    /**
     * Materializes {@code manifest} into {@code config.getStore().directory()}, opens a {@link
     * ReadOnlyEngine} against the result, and reports/refreshes the shard's {@link
     * ShardDirectory} entry for as long as the returned engine stays open. Sequence-number and
     * translog stats are taken directly from the manifest rather than read back out of the
     * materialized commit or (as {@link ReadOnlyEngine} would otherwise try) an actual local
     * translog -- a reader shard has no local translog at all, so this avoids requiring one to
     * exist just to report stats.
     */
    public static ObjectStoreReaderEngine open(
        EngineConfig config,
        CommitManifest manifest,
        ObjectStoreCommitMaterializer materializer,
        long primaryTerm,
        ShardDirectory shardDirectory,
        String localNodeId
    ) throws IOException {
        return open(config, manifest, materializer, primaryTerm, shardDirectory, localNodeId, null);
    }

    /** @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc. */
    public static ObjectStoreReaderEngine open(
        EngineConfig config,
        CommitManifest manifest,
        ObjectStoreCommitMaterializer materializer,
        long primaryTerm,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController
    ) throws IOException {
        if (admissionController != null) {
            // Acquire before any I/O: rejecting an over-capacity open should never pay for a
            // materialization that's just going to be thrown away.
            admissionController.acquire(config.getShardId());
        }
        try {
            materializer.materialize(manifest, config.getStore().directory());
            SeqNoStats seqNoStats = new SeqNoStats(
                manifest.maxSeqNo(),
                manifest.localCheckpoint(),
                config.getGlobalCheckpointSupplier().getAsLong()
            );
            return new ObjectStoreReaderEngine(
                config,
                seqNoStats,
                primaryTerm,
                manifest.generation(),
                shardDirectory,
                localNodeId,
                admissionController
            );
        } catch (Exception e) {
            // The engine that would have owned releasing this permit in close() never got built --
            // release it here instead, or a rejected/failed open would permanently leak a permit.
            if (admissionController != null) {
                admissionController.release();
            }
            throw e;
        }
    }
}
