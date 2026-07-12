/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.Engine.Searcher;
import org.opensearch.index.engine.Engine.SearcherScope;
import org.opensearch.index.engine.Engine.SearcherSupplier;
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
import org.opensearch.serverless.storage.resharding.PartitionFilteringDirectoryReader;
import org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
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
    private final AtomicLong lastObservedLatestGeneration;
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

    /**
     * The wall-clock time of this engine's own last real query-serving searcher acquisition
     * (rfc-serverless-opensearch.md &sect;7.3's reader-shard scale-to-zero, the mirror of {@code
     * ObjectStoreWriterEngine#lastActivityMillis} on the writer side). Initialized to construction
     * time so a freshly-opened, never-yet-queried engine reports "just activated," not "infinitely
     * idle" -- the same reasoning {@code lastActivityMillis} already uses.
     *
     * <p>Updated only from {@link #acquireSearcherSupplier(Function, SearcherScope)} with {@link
     * SearcherScope#EXTERNAL}: that scope is real, client-facing search/get traffic; {@link
     * SearcherScope#INTERNAL} covers this engine's own bookkeeping (segment/doc-count stats,
     * completion stats, refresh-needed checks -- see every {@code SearcherScope.INTERNAL} call site
     * in {@code Engine} itself), none of which represents a real client query and none of which
     * should reset the idle clock, mirroring exactly how {@code ObjectStoreWriterEngine} excludes
     * translog-replay-origin operations from resetting its own idle clock.
     */
    private final AtomicLong lastQueryMillis = new AtomicLong(System.currentTimeMillis());

    /**
     * A deliberately crude two-window (previous/current) query-rate counter, feeding the scale-up
     * half of autoscaling (rfc-serverless-opensearch.md's scale-up subsection): {@link
     * #windowStartMillis} marks when {@link #windowQueryCount} started accumulating, and once a
     * window has run for more than {@link #QUERY_RATE_WINDOW_MILLIS}, {@link #queriesPerMinute()}
     * rolls it over -- the just-finished window's count becomes {@link #completedWindowQueryCount},
     * and a fresh window starts counting from 1 (the query that triggered the rollover).
     *
     * <p>This is intentionally not a real sliding window: {@link #queriesPerMinute()} always reports
     * the <em>previous completed window's</em> rate, never the in-progress one, so a shard that just
     * had a single query land right after a rollover briefly under-reports (looks like the old,
     * possibly-lower rate) rather than over-reports (extrapolating one query into a spike). For a
     * scale-up signal, under-reacting for up to one window is a far safer failure mode than
     * flapping on noise, and a shard with sustained real traffic converges to an accurate rate
     * within two windows regardless.
     */
    private static final long QUERY_RATE_WINDOW_MILLIS = 60_000L;
    private final AtomicLong windowStartMillis = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong windowQueryCount = new AtomicLong(0);
    private final AtomicLong completedWindowQueryCount = new AtomicLong(0);

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
        GcSchedulerConfig gcConfig,
        ShardPartitionDescriptor partitionDescriptor
    ) {
        super(config, seqNoStats, new TranslogStats(), true, readerWrapperFunction(partitionDescriptor), false);
        this.indexUuid = config.getShardId().getIndex().getUUID();
        this.shardId = config.getShardId().getId();
        this.currentPrimaryTerm = new AtomicLong(primaryTerm);
        this.currentManifestGeneration = new AtomicLong(manifestGeneration);
        this.lastObservedLatestGeneration = new AtomicLong(manifestGeneration);
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
     * {@code null} (the common case -- every non-split shard has no partition descriptor) yields
     * {@link Function#identity()}, exactly as before this parameter existed. Otherwise wraps with
     * {@link PartitionFilteringDirectoryReader} -- see that class's own javadoc for why this single
     * seam is all a reader engine needs to change to support &sect;16 Phase 5's doc-routing
     * partition filter, and why it survives every later refresh with no further change here.
     */
    private static Function<DirectoryReader, DirectoryReader> readerWrapperFunction(ShardPartitionDescriptor partitionDescriptor) {
        if (partitionDescriptor == null) {
            return Function.identity();
        }
        return reader -> {
            try {
                return new PartitionFilteringDirectoryReader(reader, partitionDescriptor);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
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
     *
     * <p>Also the per-refresh half of admission control (rfc-serverless-opensearch.md &sect;18
     * risk #3): if the node's lazy-directory block cache is over its configured budget, this tick
     * is skipped entirely -- the shard keeps serving its current, slightly stale generation rather
     * than pulling more segment bytes into an already-over-budget cache. The next poll tick tries
     * again, so the shard catches up automatically once cache pressure eases (e.g. another shard
     * closes or its own cache entries get evicted).
     *
     * <p><b>{@code synchronized}, not just backed by an {@link java.util.concurrent.atomic.AtomicLong}.</b>
     * Three independent call paths can reach this method for the same shard: the background poll
     * scheduler, {@link #waitForGeneration}'s on-demand polling (one caller thread per RYW waiter),
     * and {@link #pollNow} (dispatched per publication-notification request). The read-check-apply-write
     * sequence here is a compound operation -- {@code currentManifestGeneration}'s atomicity only
     * covers the individual {@code get()}/{@code set()} calls, not the sequence between them -- so
     * without serialization, two overlapping callers could both observe a stale generation, both
     * apply a manifest to the same {@code Directory} concurrently, and whichever one calls {@code
     * set()} last could regress the tracked generation backward even if it applied the older
     * manifest last. Contention is expected to be rare and each call is already bounded by real
     * object-store I/O, so blocking here is cheap relative to the work already being serialized.
     */
    private synchronized void pollForNewerManifest() {
        try {
            // The head read (and the lastObservedLatestGeneration update below) deliberately runs
            // BEFORE the admission-budget check, not after: an over-budget tick still needs to know
            // how far behind it now is for manifestGenerationLag() to mean anything. Getting this
            // ordering backwards -- checking the budget first and only reading the head if under
            // budget, as an earlier version of this method did -- makes the lag observation and the
            // materialization always advance together, so lag can only ever read zero right after
            // any completed poll (skipped or applied) and never actually surfaces a real backlog,
            // silently defeating the whole point of exposing it as an autoscaling signal. Caught by
            // this method's own test asserting a nonzero lag during a real over-budget skip, not
            // just a zero one.
            Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
            if (head.isEmpty()) {
                return;
            }
            ShardHead shardHead = head.get().head();
            lastObservedLatestGeneration.set(shardHead.latestManifestGeneration());
            if (shardHead.latestManifestGeneration() <= currentManifestGeneration.get()) {
                return;
            }
            if (admissionController != null && admissionController.isOverBudgetForRefresh()) {
                logger.debug(
                    "skipping manifest poll: node's reader-shard admission budget is currently exceeded, staying on generation {}",
                    currentManifestGeneration.get()
                );
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
     *
     * @param source a description of why the refresh was triggered, for logging/diagnostics
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

    /**
     * How many manifest generations behind the latest one this engine has observed published for
     * its shard -- the "search tier: manifest-generation lag" autoscaling signal &sect;10 names as
     * a still-open hook, now backed by a real, continuously-updated measurement rather than
     * nothing. {@code 0} means this engine is caught up as of its own last poll tick (see {@link
     * #pollForNewerManifest} -- this is a point-in-time snapshot, not a live guarantee: a newer
     * generation may have published in the object store since that tick and simply not have been
     * observed yet, same staleness bound {@link #MANIFEST_POLL_INTERVAL} already governs for
     * materialization itself.
     *
     * @return the non-negative gap between the latest generation this engine has observed and the
     *         generation it currently has materialized/open
     */
    public long manifestGenerationLag() {
        return Math.max(0, lastObservedLatestGeneration.get() - currentManifestGeneration.get());
    }

    /** How often {@link #waitForGeneration} re-checks after forcing an on-demand poll, well under {@link #MANIFEST_POLL_INTERVAL}. */
    private static final long WAIT_FOR_GENERATION_POLL_INTERVAL_MILLIS = 100L;

    /**
     * Asynchronously waits until this engine has materialized at least {@code minGeneration}, or
     * {@code timeout} elapses -- the read-after-write mechanism rfc-serverless-opensearch.md
     * &sect;8 asks for: "the reader waits for it (with timeout)." Deliberately forces an on-demand
     * {@link #pollForNewerManifest} on every check rather than only relying on {@link
     * #MANIFEST_POLL_INTERVAL}'s own background schedule -- an RYW caller waiting out that fixed
     * 5-second interval by coincidence would defeat the point of a bounded, responsive wait.
     *
     * <p><b>Never blocks the calling thread</b> -- a real fix for a real problem code review
     * caught: an earlier version of this method blocked via {@link Thread#sleep} in a loop, which
     * meant whatever thread called it (dispatched off the transport thread onto {@code
     * ThreadPool.Names#GENERIC}, since this can take up to the full {@code timeout}) stayed pinned
     * to a shared pool thread for the entire wait. Since this plugin's own background tasks
     * (directory refresh, lease renewal, the poll schedule itself) also run on {@code GENERIC}, a
     * burst of concurrent RYW waiters could genuinely starve those unrelated periodic tasks
     * cluster-wide on the node, not just delay other RYW callers. Each retry here is instead
     * dispatched via {@link EngineConfig#getThreadPool()}'s own {@code schedule}, so a {@code
     * GENERIC} worker is only ever briefly occupied to run one check and either resolve {@code
     * listener} or reschedule -- never held for the wait's full duration.
     *
     * @param minGeneration the manifest generation this engine must reach before resolving {@code true}
     * @param timeout how long to wait before giving up and resolving {@code false}
     * @param listener resolved with {@code true} if {@code minGeneration} was reached before the
     *                  timeout, {@code false} otherwise; never fails, since {@link
     *                  #pollForNewerManifest} already tolerates its own errors internally.
     */
    public void waitForGeneration(long minGeneration, TimeValue timeout, org.opensearch.core.action.ActionListener<Boolean> listener) {
        scheduleWaitForGenerationCheck(minGeneration, System.currentTimeMillis() + timeout.millis(), listener);
    }

    private void scheduleWaitForGenerationCheck(
        long minGeneration,
        long deadlineMillis,
        org.opensearch.core.action.ActionListener<Boolean> listener
    ) {
        if (currentManifestGeneration.get() >= minGeneration) {
            listener.onResponse(true);
            return;
        }
        long remainingMillis = deadlineMillis - System.currentTimeMillis();
        if (remainingMillis <= 0) {
            listener.onResponse(false);
            return;
        }
        pollForNewerManifest();
        if (currentManifestGeneration.get() >= minGeneration) {
            listener.onResponse(true);
            return;
        }
        engineConfig.getThreadPool()
            .schedule(
                () -> scheduleWaitForGenerationCheck(minGeneration, deadlineMillis, listener),
                TimeValue.timeValueMillis(Math.min(WAIT_FOR_GENERATION_POLL_INTERVAL_MILLIS, remainingMillis)),
                ThreadPool.Names.GENERIC
            );
    }

    /**
     * Synchronous convenience wrapper around {@link #waitForGeneration(long, TimeValue,
     * org.opensearch.core.action.ActionListener)} for callers that are already off any thread this
     * engine shouldn't block (this class's own unit tests) -- production code reaching this engine
     * from a transport action must use the listener-based overload directly, never this one.
     *
     * @param minGeneration the manifest generation this engine must reach before returning {@code true}
     * @param timeout how long to wait before giving up and returning {@code false}
     * @return {@code true} if {@code minGeneration} was reached before the timeout; {@code false} otherwise
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public boolean waitForGeneration(long minGeneration, TimeValue timeout) throws InterruptedException {
        java.util.concurrent.CompletableFuture<Boolean> future = new java.util.concurrent.CompletableFuture<>();
        waitForGeneration(
            minGeneration,
            timeout,
            org.opensearch.core.action.ActionListener.wrap(future::complete, future::completeExceptionally)
        );
        try {
            // A generous grace buffer over the caller's own timeout: the real deadline is already
            // enforced inside scheduleWaitForGenerationCheck itself (which resolves false on its
            // own), this just bounds how long this blocking wrapper waits for that resolution to
            // arrive back on this thread.
            return future.get(timeout.millis() + 5_000L, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } catch (java.util.concurrent.TimeoutException e) {
            return false;
        }
    }

    /**
     * Records real query-serving searcher acquisitions (see {@link #lastQueryMillis}'s own javadoc
     * for the {@link SearcherScope#EXTERNAL}-only distinction) before delegating to {@link
     * ReadOnlyEngine#acquireSearcherSupplier(Function, SearcherScope)}.
     *
     * @param wrapper passed through unchanged to {@link ReadOnlyEngine#acquireSearcherSupplier}.
     * @param scope only {@link SearcherScope#EXTERNAL} counts as real query activity.
     */
    @Override
    public SearcherSupplier acquireSearcherSupplier(Function<Searcher, Searcher> wrapper, SearcherScope scope) throws EngineException {
        if (scope == SearcherScope.EXTERNAL) {
            long now = System.currentTimeMillis();
            lastQueryMillis.set(now);
            recordQueryForRateCounter(now);
        }
        return super.acquireSearcherSupplier(wrapper, scope);
    }

    /**
     * Rolls {@link #windowStartMillis}/{@link #windowQueryCount} over into {@link
     * #completedWindowQueryCount} once the current window has run longer than {@link
     * #QUERY_RATE_WINDOW_MILLIS}, then counts {@code now}'s query into whichever window is current
     * after that possible rollover. Synchronized: rollover is a compound check-then-reset that must
     * not race with itself across concurrent searcher acquisitions, and query traffic on a single
     * shard is not so hot that a short critical section here matters.
     *
     * @param now the current wall-clock time, as already computed by the caller.
     */
    private synchronized void recordQueryForRateCounter(long now) {
        long start = windowStartMillis.get();
        if (now - start >= QUERY_RATE_WINDOW_MILLIS) {
            completedWindowQueryCount.set(windowQueryCount.get());
            windowStartMillis.set(now);
            windowQueryCount.set(1);
        } else {
            windowQueryCount.incrementAndGet();
        }
    }

    /**
     * A conservative, always-one-window-stale estimate of this engine's own real client-facing
     * query rate (rfc-serverless-opensearch.md's scale-up subsection) -- see {@link
     * #completedWindowQueryCount}'s own javadoc for why this deliberately reports the previous
     * completed window's count rather than extrapolating the in-progress one. Reports {@code 0}
     * until this engine has completed at least one full {@link #QUERY_RATE_WINDOW_MILLIS} window
     * since construction (or since its last query, if traffic has since gone fully idle for a
     * window -- {@link #recordQueryForRateCounter} only rolls the window over on the next query, so
     * a shard that goes idle keeps reporting its last completed window's rate until one more query
     * arrives after the gap, which is an acceptable imprecision for a scale-<em>up</em> signal: an
     * idle shard has nothing to scale up for regardless).
     *
     * @return queries observed during the previous completed {@link #QUERY_RATE_WINDOW_MILLIS} window.
     */
    public long queriesPerMinute() {
        return completedWindowQueryCount.get();
    }

    /**
     * How long since this engine's own last real client-facing search/get, mirroring {@code
     * ObjectStoreWriterEngine#millisSinceLastActivity()} on the reader side (rfc-serverless-opensearch.md
     * &sect;7.3's "reader shards scale to zero the same way [as writers]" -- the query-activity
     * signal that still-open goal needs).
     */
    public long millisSinceLastQuery() {
        return System.currentTimeMillis() - lastQueryMillis.get();
    }

    /** Invokes {@link #pollForNewerManifest()} synchronously, rather than waiting out {@link #MANIFEST_POLL_INTERVAL}. */
    void pollForNewerManifestForTesting() {
        pollForNewerManifest();
    }

    /**
     * The real, public entry point for forcing this engine to check for a newer manifest generation
     * right now, rather than waiting out {@link #MANIFEST_POLL_INTERVAL} -- the receiving side of
     * rfc-serverless-opensearch.md &sect;8's publication notification mechanism ("a small
     * publication notification... generalize the existing segment-replication checkpoint
     * publisher"). {@link org.opensearch.serverless.storage.readerengine.action.PollNowAction}
     * calls this (via {@link org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry})
     * when a caller wants a specific reader engine to catch up immediately instead of on its own
     * schedule. Notifications remain strictly an optimization: {@link #waitForGeneration} and the
     * background poll schedule both already converge correctly with nobody ever calling this.
     */
    public void pollNow() {
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
     *
     * @param config the engine configuration, whose {@link EngineConfig#getStore()} directory is materialized into
     * @param manifest the commit manifest to open the engine against
     * @param materializer applies the manifest's files to the engine's store directory
     * @param primaryTerm the primary term the manifest was published under
     * @param shardStateStore used to poll for a newer published head
     * @param manifestStore used to read newer manifest generations found via polling
     * @param shardDirectory the shard-directory-tier client this engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @return an open reader engine, with directory-tier reporting and manifest polling running
     * @throws IOException if materializing the manifest into the store directory fails
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
            null,
            null
        );
    }

    /**
     * Same as {@link #open(EngineConfig, CommitManifest, ObjectStoreCommitMaterializer, long,
     * ShardStateStore, BlobContainerManifestStore, ShardDirectory, String, ReaderShardAdmissionController,
     * CompactionSchedulerConfig, GcSchedulerConfig, ShardPartitionDescriptor)}, with no partition descriptor
     * (i.e. this shard is not a &sect;16 Phase 5 split target -- the common case).
     *
     * @param config the engine configuration, whose {@link EngineConfig#getStore()} directory is materialized into
     * @param manifest the commit manifest to open the engine against
     * @param materializer applies the manifest's files to the engine's store directory
     * @param primaryTerm the primary term the manifest was published under
     * @param shardStateStore used to poll for a newer published head
     * @param manifestStore used to read newer manifest generations found via polling
     * @param shardDirectory the shard-directory-tier client this engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} to disable this reader's own background compaction
     *        scheduler entirely -- see {@link CompactionSchedulerConfig}'s own javadoc.
     * @param gcConfig {@code null} to disable this reader's own background GC sweep entirely --
     *        see {@link GcSchedulerConfig}'s own javadoc.
     * @return an open reader engine, with directory-tier reporting and manifest polling running
     * @throws IOException if materializing the manifest into the store directory fails
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
        return open(
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
            null
        );
    }

    /**
     * Same as {@link #open(EngineConfig, CommitManifest, ObjectStoreCommitMaterializer, long,
     * ShardStateStore, BlobContainerManifestStore, ShardDirectory, String)}, with optional
     * admission control, background schedulers, and this shard's own &sect;16 Phase 5 partition descriptor.
     *
     * @param config the engine configuration, whose {@link EngineConfig#getStore()} directory is materialized into
     * @param manifest the commit manifest to open the engine against
     * @param materializer applies the manifest's files to the engine's store directory
     * @param primaryTerm the primary term the manifest was published under
     * @param shardStateStore used to poll for a newer published head
     * @param manifestStore used to read newer manifest generations found via polling
     * @param shardDirectory the shard-directory-tier client this engine reports its entry to
     * @param localNodeId this node's id, reported as part of the shard directory entry
     * @param admissionController {@code null} to disable the admission cap entirely -- see its own javadoc.
     * @param compactionConfig {@code null} to disable this reader's own background compaction
     *        scheduler entirely -- see {@link CompactionSchedulerConfig}'s own javadoc.
     * @param gcConfig {@code null} to disable this reader's own background GC sweep entirely --
     *        see {@link GcSchedulerConfig}'s own javadoc.
     * @param partitionDescriptor {@code null} unless this shard is a &sect;16 Phase 5 split
     *        target -- see {@link PartitionFilteringDirectoryReader}'s own javadoc for what
     *        supplying one does.
     * @return an open reader engine, with directory-tier reporting and manifest polling running
     * @throws IOException if materializing the manifest into the store directory fails
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
        GcSchedulerConfig gcConfig,
        ShardPartitionDescriptor partitionDescriptor
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
                gcConfig,
                partitionDescriptor
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
