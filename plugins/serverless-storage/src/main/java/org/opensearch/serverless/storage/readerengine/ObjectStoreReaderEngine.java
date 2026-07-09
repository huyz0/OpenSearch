/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.ReadOnlyEngine;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.translog.TranslogStats;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerConfig;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerTask;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;
import org.opensearch.serverless.storage.gc.GcSchedulerConfig;
import org.opensearch.serverless.storage.gc.GcSchedulerTask;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
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
 * the object-store-specific work is confined to the genuinely novel pieces -- materializing a
 * manifest into a directory, refreshing this shard's {@link ShardDirectory} entry, and (see below)
 * advancing to a newer manifest generation -- rather than re-deriving several thousand lines of
 * tested read-path behavior.
 *
 * <p><b>Refreshing to a newer manifest generation is implemented</b>, and needed less than it first
 * looked: {@link ReadOnlyEngine}'s own internal reader manager already reopens via {@code
 * DirectoryReader#openIfChanged} (see {@code OpenSearchReaderManager#refreshIfNeeded}) -- Lucene's
 * standard incremental-reopen mechanism, which already tolerates new segment files landing
 * alongside old ones in the same directory (distinct generations mean distinct file names, and
 * {@link ObjectStoreCommitMaterializer#materialize} is purely additive per-file writes with no
 * empty-directory assumption -- confirmed by reading it, not assumed). {@link ReadOnlyEngine} only
 * *disables* triggering that reopen ({@link #refresh}/{@link #maybeRefresh} are no-ops there, by
 * that class's own design, "we could allow refreshes if we want down the road") -- so this class
 * overrides those three methods to call through to the reader manager it already builds, and adds
 * {@link #pollForNewerManifest()} (scheduled alongside the existing directory-tier refresh) to
 * materialize a newer manifest's files into the same directory and trigger that reopen. No new
 * {@code Engine} subclass, no {@code IndexShard}-level engine swap -- this mirrors the same
 * in-place-reopen shape core's own {@code NRTReplicationEngine}/{@code NRTReplicationReaderManager}
 * already use for segment replication's own read path, just triggered by a manifest-generation poll
 * instead of a segment-replication event. Polling, not true pub/sub notification (rfc-serverless-metadata-plane.md
 * &sect;5's eventual notification path) -- a possible later refinement, not a prerequisite for
 * closing this section's own "still open" note.
 *
 * <p>What this class also owns is the directory-tier refresh (rfc-serverless-metadata-plane.md
 * &sect;13 risk #1, "metastability of the directory tier"), the same mitigation {@code
 * ObjectStoreWriterEngine} applies on the writer side: reporting once on activation and then again
 * on a fixed schedule for as long as the engine stays open, so this shard's directory entry doesn't
 * depend on traffic to stay fresh, and both scheduled tasks are canceled cleanly on {@link #close()}.
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

    /**
     * Aimed well under this phase's own p99 freshness-lag milestone (15 s) -- a poll finding
     * nothing newer is cheap (one {@link ShardStateStore#get} read), so there is no real cost to
     * polling faster than the milestone strictly requires.
     */
    private static final TimeValue MANIFEST_POLL_INTERVAL = TimeValue.timeValueSeconds(5);

    private final String indexUuid;
    private final int shardId;
    private final AtomicLong currentPrimaryTerm;
    private final AtomicLong currentManifestGeneration;
    private final ShardStateStore shardStateStore;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ShardDirectory shardDirectory;
    private final String localNodeId;
    private final Scheduler.Cancellable directoryRefreshTask;
    private final Scheduler.Cancellable manifestPollTask;
    private final ReaderShardAdmissionController admissionController;
    private final CompactionSchedulerTask compactionSchedulerTask;
    private final GcSchedulerTask gcSchedulerTask;

    private ObjectStoreReaderEngine(
        EngineConfig config,
        SeqNoStats seqNoStats,
        long primaryTerm,
        long manifestGeneration,
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig
    ) {
        super(config, seqNoStats, new TranslogStats(), true, Function.identity(), false);
        this.indexUuid = config.getShardId().getIndex().getUUID();
        this.shardId = config.getShardId().getId();
        this.currentPrimaryTerm = new AtomicLong(primaryTerm);
        this.currentManifestGeneration = new AtomicLong(manifestGeneration);
        this.shardStateStore = shardStateStore;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.admissionController = admissionController;
        // Report once synchronously so the shard is discoverable immediately on activation, rather
        // than waiting out the first refresh interval; scheduleWithFixedDelay's first execution
        // only happens after the interval elapses, not on registration.
        refreshDirectoryEntry();
        this.directoryRefreshTask = config.getThreadPool()
            .scheduleWithFixedDelay(this::refreshDirectoryEntry, DIRECTORY_REFRESH_INTERVAL, ThreadPool.Names.GENERIC);
        this.manifestPollTask = config.getThreadPool()
            .scheduleWithFixedDelay(this::pollForNewerManifest, MANIFEST_POLL_INTERVAL, ThreadPool.Names.GENERIC);
        // A reader shard is a natural home for this: it exists for as long as the shard is
        // searchable at all, including the "writer scaled to zero" case a writer-only scheduler
        // instance would miss entirely (rfc-serverless-opensearch.md &sect;16 Phase 4.5). Running
        // redundantly alongside a writer-attached instance (see ObjectStoreWriterEngine) or another
        // reader copy's is safe, at worst wasted work -- CompactionRebaseExecutor's own rebase-on-
        // conflict protocol already tolerates concurrent compactors.
        this.compactionSchedulerTask = compactionConfig == null
            ? null
            : new CompactionSchedulerTask(
                config.getThreadPool(),
                compactionConfig.interval(),
                indexUuid,
                shardId,
                shardStateStore,
                compactionConfig.manifestStore(),
                compactionConfig.materializer(),
                compactionConfig.commitPublisher(),
                compactionConfig.policy(),
                compactionConfig.rebaseExecutor()
            );
        // Same reasoning and same redundancy-is-safe argument as compactionSchedulerTask above --
        // see GcSchedulerTask's own javadoc for its own, separate safety design (retention window +
        // durable pins only, deliberately not a ShardDirectory-derived lease pin).
        this.gcSchedulerTask = gcConfig == null
            ? null
            : new GcSchedulerTask(config.getThreadPool(), gcConfig.interval(), indexUuid, shardId, gcConfig);
    }

    /**
     * Applies {@code manifest}'s files to {@code directory}, however that directory actually needs
     * it done: a {@link org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory}
     * (rfc-serverless-opensearch.md &sect;7.2/&sect;9, built by {@code
     * ServerlessStorageLazyDirectoryFactory} when this shard opted into {@code
     * ServerlessStoragePlugin#LAZY_DIRECTORY_STORE_TYPE}) already knows how to resolve any file
     * lazily and just needs its file map advanced -- no I/O at all, {@code
     * LazyBundleDirectory#advanceToManifest} is a plain in-memory map merge. Every other directory
     * (a normal local {@code FSDirectory}, the common case) still needs the original eager,
     * full-fetch {@link ObjectStoreCommitMaterializer#materialize}. {@link
     * org.apache.lucene.store.FilterDirectory#unwrap} is needed because {@code Store} always wraps
     * whatever directory it's given in its own {@code FilterDirectory} layers (see {@code Store}'s
     * own constructor) -- the same unwrap {@code ReadOnlyEngine}'s own constructor already does to
     * detect a {@code RemoteSnapshotDirectory}.
     */
    private static void applyManifestToDirectory(
        org.apache.lucene.store.Directory directory,
        CommitManifest manifest,
        ObjectStoreCommitMaterializer materializer
    ) throws IOException {
        org.apache.lucene.store.Directory unwrapped = org.apache.lucene.store.FilterDirectory.unwrap(directory);
        if (unwrapped instanceof org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory) {
            ((org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory) unwrapped).advanceToManifest(manifest);
        } else {
            materializer.materialize(manifest, directory);
        }
    }

    private void refreshDirectoryEntry() {
        shardDirectory.report(
            indexUuid,
            shardId,
            new ShardDirectoryEntry(
                localNodeId,
                ShardRole.READER,
                currentPrimaryTerm.get(),
                currentManifestGeneration.get(),
                System.currentTimeMillis() + DIRECTORY_ENTRY_TTL_MILLIS
            )
        );
    }

    /**
     * Checks whether the shard's published head now names a newer manifest generation than the
     * one this engine currently has materialized, and if so, materializes and refreshes to it. A
     * failure here (a transient object-store error, a torn read) is logged and left for the next
     * poll tick to retry -- never worth failing an already-open, still-serving engine over, exactly
     * the same tolerance {@link #refreshDirectoryEntry} already has for its own reporting.
     */
    private void pollForNewerManifest() {
        try {
            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            if (head.isEmpty()) {
                return;
            }
            ShardHead shardHead = head.get().head();
            if (shardHead.latestManifestGeneration() <= currentManifestGeneration.get()) {
                return;
            }
            CommitManifest manifest = manifestStore.readManifest(shardHead.primaryTerm(), shardHead.latestManifestGeneration());
            applyManifestToDirectory(engineConfig.getStore().directory(), manifest, materializer);
            maybeRefresh("manifest-generation-advance");
            currentManifestGeneration.set(manifest.generation());
            currentPrimaryTerm.set(shardHead.primaryTerm());
        } catch (Exception e) {
            logger.warn("failed to poll/refresh to a newer manifest generation, will retry next tick", e);
        }
    }

    /**
     * {@link ReadOnlyEngine} builds this exact reader manager already (see its own constructor) --
     * it is only {@link ReadOnlyEngine#refresh}/{@link ReadOnlyEngine#maybeRefresh} that never call
     * through to it. Overriding those two methods (and {@link #refreshNeeded}) here is the entire
     * mechanism this class needs: the manager's own {@code refreshIfNeeded} already does a real
     * {@code DirectoryReader#openIfChanged} against the directory {@link #pollForNewerManifest} just
     * wrote a newer manifest's files into.
     */
    @Override
    public void refresh(String source) throws EngineException {
        try {
            getReferenceManager(SearcherScope.EXTERNAL).maybeRefreshBlocking();
        } catch (IOException e) {
            throw new EngineException(engineConfig.getShardId(), "failed to refresh reader engine", e);
        }
    }

    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        try {
            return getReferenceManager(SearcherScope.EXTERNAL).maybeRefresh();
        } catch (IOException e) {
            throw new EngineException(engineConfig.getShardId(), "failed to refresh reader engine", e);
        }
    }

    /**
     * Conservatively {@code true} always, rather than actually checking: the only correct way to
     * check without leaking a reader is to open one via {@code DirectoryReader#openIfChanged} and
     * then either use or close it, which is exactly what {@link #maybeRefresh} already does
     * atomically -- a separate check-only call here would just be a second, wasted open on every
     * "yes" answer. Callers that care about the real cost of a wasted {@link #maybeRefresh} call
     * when nothing changed already tolerate it: {@link ReferenceManager#maybeRefresh} itself is a
     * cheap no-op in that case (it opens a reader, finds the same commit generation reflected, and
     * discards it) -- correct is more important than shaving that discard.
     */
    @Override
    public boolean refreshNeeded() {
        return true;
    }

    /** Test-only visibility into what generation this engine currently has materialized/open. */
    long currentManifestGenerationForTesting() {
        return currentManifestGeneration.get();
    }

    /** Invokes {@link #pollForNewerManifest()} synchronously, rather than waiting out {@link #MANIFEST_POLL_INTERVAL}. */
    void pollForNewerManifestForTesting() {
        pollForNewerManifest();
    }

    @Override
    public void close() throws IOException {
        manifestPollTask.cancel();
        directoryRefreshTask.cancel();
        if (compactionSchedulerTask != null) {
            compactionSchedulerTask.close();
        }
        if (gcSchedulerTask != null) {
            gcSchedulerTask.close();
        }
        if (admissionController != null) {
            admissionController.release();
        }
        super.close();
    }

    /**
     * Materializes {@code manifest} into {@code config.getStore().directory()}, opens a {@link
     * ReadOnlyEngine} against the result, and reports/refreshes the shard's {@link
     * ShardDirectory} entry -- and now polls for and advances to newer manifest generations -- for
     * as long as the returned engine stays open. Sequence-number and translog stats are taken
     * directly from the manifest rather than read back out of the materialized commit or (as {@link
     * ReadOnlyEngine} would otherwise try) an actual local translog -- a reader shard has no local
     * translog at all, so this avoids requiring one to exist just to report stats.
     */
    public static ObjectStoreReaderEngine open(
        EngineConfig config,
        CommitManifest manifest,
        ObjectStoreCommitMaterializer materializer,
        long primaryTerm,
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ShardDirectory shardDirectory,
        String localNodeId
    ) throws IOException {
        return open(
            config,
            manifest,
            materializer,
            primaryTerm,
            shardStateStore,
            manifestStore,
            shardDirectory,
            localNodeId,
            null,
            null,
            null
        );
    }

    /**
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} to disable this reader's own background compaction
     *        scheduler entirely -- see {@link CompactionSchedulerConfig}'s own javadoc.
     * @param gcConfig {@code null} to disable this reader's own background GC sweep entirely --
     *        see {@link GcSchedulerConfig}'s own javadoc.
     */
    public static ObjectStoreReaderEngine open(
        EngineConfig config,
        CommitManifest manifest,
        ObjectStoreCommitMaterializer materializer,
        long primaryTerm,
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ShardDirectory shardDirectory,
        String localNodeId,
        ReaderShardAdmissionController admissionController,
        CompactionSchedulerConfig compactionConfig,
        GcSchedulerConfig gcConfig
    ) throws IOException {
        if (admissionController != null) {
            // Acquire before any I/O: rejecting an over-capacity open should never pay for a
            // materialization that's just going to be thrown away.
            admissionController.acquire(config.getShardId());
        }
        try {
            applyManifestToDirectory(config.getStore().directory(), manifest, materializer);
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
                shardStateStore,
                manifestStore,
                materializer,
                shardDirectory,
                localNodeId,
                admissionController,
                compactionConfig,
                gcConfig
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
