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
import org.opensearch.serverless.storage.resharding.PartitionRewritePublisher;
import org.opensearch.serverless.storage.resharding.PartitionRewriteSchedulerConfig;
import org.opensearch.serverless.storage.resharding.PartitionRewriteSchedulerTask;
import org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
     * The floor of the manifest poll's adaptive interval, and the interval a shard with any recent
     * query traffic actually polls at. Aimed well under this phase's own p99 freshness-lag
     * milestone (15 s).
     */
    private static final TimeValue MANIFEST_POLL_INTERVAL = TimeValue.timeValueSeconds(5);

    /**
     * The ceiling of the adaptive interval, used by a shard that is both idle and finding nothing
     * new.
     *
     * <p>A fixed 5&nbsp;s poll is priced per shard ("a poll finding nothing newer is cheap -- one
     * {@link ShardStateStore#get} read"), and per shard it is. Per <em>node</em> it is not: at the
     * density this design targets -- thousands of hot reader shards per node -- a fixed 5&nbsp;s
     * poll is hundreds of object-store requests per second per node of pure polling, before any
     * query traffic, and it dominates every other request the read path makes. It is also no longer
     * the primary freshness mechanism: {@link #pollNow()} exists precisely so a publication
     * notification can pull a reader forward immediately, which makes the schedule a safety net.
     * A safety net does not need to run at 5&nbsp;s on a shard nobody is querying and whose head
     * has not moved in minutes.
     */
    private static final TimeValue MAX_MANIFEST_POLL_INTERVAL = TimeValue.timeValueSeconds(60);

    /**
     * How long a shard must have gone without a real client query before its poll is allowed to
     * back off at all. A shard being queried keeps polling at the floor no matter how quiet its
     * writer is, because that is exactly the shard whose freshness a user can observe.
     */
    private static final long POLL_BACKOFF_IDLE_THRESHOLD_MILLIS = 60_000L;

    /**
     * After this many consecutive over-budget skips, the next tick advances anyway.
     *
     * <p>The budget check is a defence against pulling more bytes onto an already-pressured node,
     * and deferring one refresh is the right answer to that. Deferring <em>every</em> refresh
     * forever is not: an unbounded deferral means a reader's generation lag grows without limit,
     * which violates the consistency model outright ("lag is bounded by publication frequency plus
     * notification delivery"), inflates the very autoscaling signal that would add more reader nodes
     * to fix it, and -- worst -- lets the frozen generation age past the GC retention window and be
     * deleted out from under the reader. Bounding the number of consecutive skips keeps staleness
     * finite no matter what the pressure signal says.
     */
    private static final int MAX_CONSECUTIVE_OVER_BUDGET_SKIPS = 12;

    private final String indexUuid;
    private final int shardId;
    private final AtomicLong currentPrimaryTerm;
    private final AtomicLong currentManifestGeneration;
    private final AtomicLong lastObservedLatestGeneration;
    private final ShardStateStore shardStateStore;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ShardDirectory shardDirectory;
    /** The entry this engine instance itself most recently reported -- see {@link #close()} for why this is tracked. */
    private final java.util.concurrent.atomic.AtomicReference<ShardDirectoryEntry> lastReportedEntry =
        new java.util.concurrent.atomic.AtomicReference<>();
    private final String localNodeId;
    private final Scheduler.Cancellable directoryRefreshTask;
    private volatile Scheduler.Cancellable manifestPollTask;
    private final ReaderShardAdmissionController admissionController;
    /**
     * Guards every piece of teardown this engine owns, so it runs exactly once no matter which of
     * the two close paths gets there first -- see {@link #cleanupOnce()}.
     */
    private final AtomicBoolean cleanedUp = new AtomicBoolean(false);
    /**
     * The registry to remove this engine from when it stops being live, and the exact key it was
     * registered under -- {@code null} until something registers it.
     *
     * <p>The key is remembered rather than re-derived from this engine's own {@code indexUuid}/
     * {@code shardId}: a caller registers under whatever key it chooses, and a test (or any caller
     * holding an index UUID from somewhere other than the engine config) legitimately uses a
     * different one. Unregistering under a key nobody registered would silently leak the entry,
     * which is the whole failure this was added to prevent.
     */
    private volatile RegistryRegistration activityRegistration;

    /** Where this engine was registered, so it can remove exactly that entry. */
    private record RegistryRegistration(ReaderShardActivityRegistry registry, String indexUuid, int shardId) {
    }

    /**
     * The durable pin this engine holds on the generation it currently serves, or {@code null} when
     * no pin registry is available -- see {@link #takeOrMoveReaderPin}.
     */
    private final org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry;
    private final AtomicReference<org.opensearch.serverless.storage.retention.PinRecord> currentReaderPin = new AtomicReference<>();
    /** Consecutive poll ticks skipped because the node was over its refresh budget -- see {@link #MAX_CONSECUTIVE_OVER_BUDGET_SKIPS}. */
    private final AtomicInteger consecutiveOverBudgetSkips = new AtomicInteger();
    /** The current adaptive poll interval, in millis -- see {@link #MAX_MANIFEST_POLL_INTERVAL}. */
    private final AtomicLong currentPollIntervalMillis = new AtomicLong(MANIFEST_POLL_INTERVAL.millis());
    /** Wall-clock time the next real poll is allowed to run; ticks before it cost nothing at all. */
    private final AtomicLong nextPollDueAtMillis = new AtomicLong(0L);
    /**
     * The manifest this engine currently serves, and the one before it. Both are retained so
     * {@link ObjectStoreCommitMaterializer#pruneUnreferencedFiles} can delete everything else
     * without ever touching a file a searcher acquired just before the last advance still needs.
     */
    private final AtomicReference<CommitManifest> currentManifest = new AtomicReference<>();
    private final AtomicReference<CommitManifest> previousManifest = new AtomicReference<>();
    /**
     * Sequence-number statistics for the generation currently open, rebuilt on every advance.
     *
     * <p>{@link ReadOnlyEngine} takes its {@code SeqNoStats} once in its constructor and caches it
     * forever, which is right for a genuinely read-only engine over a fixed commit and wrong for
     * this one: this engine materializes newer generations for its whole life. Without this, a
     * reader shard's reported {@code maxSeqNo} and {@code localCheckpoint} are permanently those of
     * whatever generation it happened to open on, however many it later advanced through -- and
     * they look perfectly valid to {@code _stats}, {@code _cat/shards}, and any seq-no-based
     * consistency check reading them.
     */
    private volatile SeqNoStats currentSeqNoStats;
    /** Pending read-after-write waiters, resolved by one shared poll rather than one poll each -- see {@link #waitForGeneration}. */
    private final java.util.Queue<GenerationWaiter> generationWaiters = new java.util.concurrent.ConcurrentLinkedQueue<>();
    private final CompactionSchedulerTask compactionSchedulerTask;
    private final GcSchedulerTask gcSchedulerTask;
    private final PartitionRewriteSchedulerTask partitionRewriteSchedulerTask;
    private final PitrRetentionSchedulerTask pitrRetentionTask;

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
        CommitManifest openedManifest,
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
        ShardPartitionDescriptor partitionDescriptor,
        PartitionRewriteSchedulerConfig partitionRewriteConfig,
        PitrRetentionConfig pitrRetentionConfig
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
        this.currentSeqNoStats = seqNoStats;
        this.currentManifest.set(openedManifest);
        this.previousManifest.set(openedManifest);
        // Either scheduler's registry will do -- they are the same CAS-backed, cluster-wide
        // registry, and a deployment that has neither has no pin mechanism to use at all. Read out
        // of the configs this engine is already given rather than threaded through as yet another
        // constructor parameter, because there is nothing to decide: if a pin registry exists for
        // this shard, a reader on this shard should be pinning with it.
        this.pinRegistry = gcConfig != null
            ? gcConfig.pinRegistry()
            : (pitrRetentionConfig != null ? pitrRetentionConfig.pinRegistry() : null);
        // Taken BEFORE any scheduler starts, and before this constructor can return an engine that
        // is serving queries: the generation this engine opened on must be protected from GC from
        // the first instant it is in use, not from the first poll tick. See takeOrMoveReaderPin.
        takeOrMoveReaderPin(primaryTerm, manifestGeneration);
        // Report once synchronously so the shard is discoverable immediately on activation, rather
        // than waiting out the first refresh interval; scheduleWithFixedDelay's first execution
        // only happens after the interval elapses, not on registration.
        refreshDirectoryEntry();
        this.directoryRefreshTask = config.getThreadPool()
            .scheduleWithFixedDelay(this::refreshDirectoryEntry, DIRECTORY_REFRESH_INTERVAL, ThreadPool.Names.GENERIC);
        this.manifestPollTask = config.getThreadPool()
            .scheduleWithFixedDelay(this::pollTick, MANIFEST_POLL_INTERVAL, ThreadPool.Names.GENERIC);
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
                compactionConfig.rebaseExecutor(),
                compactionConfig.admissionController(),
                compactionConfig.mergeWorkRoot()
            );
        // Same reasoning and same redundancy-is-safe argument as compactionSchedulerTask above --
        // see GcSchedulerTask's own javadoc for its own, separate safety design (retention window +
        // durable pins only, deliberately not a ShardDirectory-derived lease pin).
        this.gcSchedulerTask = gcConfig == null
            ? null
            : new GcSchedulerTask(config.getThreadPool(), gcConfig.interval(), indexUuid, shardId, gcConfig);
        // §16 Phase 5's "no automatic background scheduler for physical partition rewrite" gap,
        // closed the same way compaction's own equivalent gap was: only meaningful for a split
        // target (partitionDescriptor != null is what PartitionRewritePublisher#rewrite itself
        // checks on every tick anyway, but there's no reason to schedule a task that would only
        // ever no-op for every other shard). Same redundancy-is-safe argument as the other two
        // schedulers above -- a rewrite is idempotent and safe to call speculatively.
        this.partitionRewriteSchedulerTask = (partitionRewriteConfig == null || partitionDescriptor == null)
            ? null
            : new PartitionRewriteSchedulerTask(
                config.getThreadPool(),
                partitionRewriteConfig.interval(),
                new PartitionRewritePublisher(
                    indexUuid,
                    shardId,
                    partitionRewriteConfig.shardStateStore(),
                    partitionRewriteConfig.manifestStore(),
                    partitionRewriteConfig.bundleStore(),
                    partitionRewriteConfig.materializer(),
                    partitionRewriteConfig.commitPublisher(),
                    partitionRewriteConfig.partitionStore()
                ),
                partitionRewriteConfig.admissionController()
            );
        // Same reasoning as gcSchedulerTask above, applied to PITR: a reader shard outlives its
        // writer scaling to zero, so it -- not just ObjectStoreWriterEngine -- must keep reconciling
        // PITR pins, or a pin set frozen at scale-to-zero time never ages out of the window and GC
        // can never reclaim it. Redundant with a writer-attached instance is safe, same
        // "worst case wasted work" argument as every other scheduler here.
        this.pitrRetentionTask = pitrRetentionConfig == null
            ? null
            : new PitrRetentionSchedulerTask(
                config.getThreadPool(),
                PitrRetentionSchedulerTask.DEFAULT_RECONCILE_INTERVAL,
                indexUuid,
                shardId,
                pitrRetentionConfig.manifestStore(),
                pitrRetentionConfig.pinRegistry(),
                pitrRetentionConfig.windowMillis()
            );
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
        ShardDirectoryEntry entry = new ShardDirectoryEntry(
            localNodeId,
            ShardRole.READER,
            currentPrimaryTerm.get(),
            currentManifestGeneration.get(),
            System.currentTimeMillis() + DIRECTORY_ENTRY_TTL_MILLIS
        );
        shardDirectory.report(indexUuid, shardId, entry);
        lastReportedEntry.set(entry);
        // Piggy-backed on a tick that already runs every 20 s: a reader that sits on one generation
        // for hours (an idle shard, a brownout, a long-lived searcher) must not have that generation
        // collected out from under it just because it had nothing new to advance to.
        renewReaderPin();
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
            // Every exit from this method updates the adaptive interval, so a tick that finds
            // nothing on an idle shard genuinely stops costing a request every 5 seconds.
            lastPollAdvanced = false;
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
            if (isRefreshDeferredByBudget()) {
                return;
            }
            CommitManifest manifest = manifestStore.readManifest(shardHead.primaryTerm(), shardHead.latestManifestGeneration());
            // The pin moves BEFORE the advance, not after: the generation about to be opened is the
            // one that must not be collected, and taking the pin first means there is no instant
            // where this engine is serving a generation nothing is holding. If the pin cannot be
            // taken, the advance does not happen -- staying one generation behind is a freshness
            // cost; advancing onto a generation GC is free to delete is a correctness one.
            if (takeOrMoveReaderPin(shardHead.primaryTerm(), manifest.generation()) == false) {
                logger.warn(
                    "not advancing shard [{}][{}] to generation {}: could not take a reader pin on it",
                    indexUuid,
                    shardId,
                    manifest.generation()
                );
                return;
            }
            applyManifestToDirectory(engineConfig.getStore().directory(), manifest, materializer);
            maybeRefresh("manifest-generation-advance");
            if (servesCommitNamedBy(manifest) == false) {
                // The materialization succeeded and the reopen ran, but the reader is not actually
                // on this manifest's commit. Advancing the tracked generation here is exactly the
                // failure that made compaction invisible: manifestGenerationLag(), the shard's
                // directory entry, and every read-after-write waiter would all report this shard as
                // caught up while it served older data. Reporting honestly and retrying next tick is
                // the only safe answer.
                logger.error(
                    "shard [{}][{}] materialized manifest generation {} but is not serving its commit [{}]; staying on generation {}",
                    indexUuid,
                    shardId,
                    manifest.generation(),
                    manifest.segmentsFileName(),
                    currentManifestGeneration.get()
                );
                return;
            }
            previousManifest.set(currentManifest.get());
            currentManifest.set(manifest);
            currentManifestGeneration.set(manifest.generation());
            currentPrimaryTerm.set(shardHead.primaryTerm());
            // Rebuilt on every advance, not taken once at open -- see currentSeqNoStats' own comment
            // for what a frozen value silently misreports.
            currentSeqNoStats = new SeqNoStats(
                manifest.maxSeqNo(),
                manifest.localCheckpoint(),
                engineConfig.getGlobalCheckpointSupplier().getAsLong()
            );
            lastPollAdvanced = true;
            pruneSupersededFiles();
            resolveGenerationWaiters();
        } catch (Exception e) {
            logger.warn("failed to poll/refresh to a newer manifest generation, will retry next tick", e);
        } finally {
            updatePollInterval(lastPollAdvanced);
        }
    }

    /**
     * Whether the poll currently running advanced a generation. A field rather than a local purely
     * so the {@code finally} above can read it; only ever touched under this class's own monitor,
     * which {@link #pollForNewerManifest} holds for its whole body.
     */
    private boolean lastPollAdvanced;

    /**
     * Whether this tick should defer because the node is over its refresh budget -- and, crucially,
     * whether it has deferred too many times in a row to keep doing so.
     *
     * <p>Deferring at all is right: pulling another generation's bytes onto a node whose block
     * cache is already at its admission budget is what the deferral exists to prevent. Deferring
     * without limit is not, for the three reasons {@link #MAX_CONSECUTIVE_OVER_BUDGET_SKIPS}
     * describes. A prune-and-recheck runs first, because the budget signal is measured against a
     * cache that evicts on demand: what looks like "no headroom" is very often "no headroom until
     * something asks for some".
     */
    private boolean isRefreshDeferredByBudget() {
        if (admissionController == null || admissionController.isOverBudgetForRefresh() == false) {
            consecutiveOverBudgetSkips.set(0);
            return false;
        }
        if (admissionController.pruneAndRecheckOverBudgetForRefresh() == false) {
            consecutiveOverBudgetSkips.set(0);
            return false;
        }
        int skips = consecutiveOverBudgetSkips.incrementAndGet();
        if (skips > MAX_CONSECUTIVE_OVER_BUDGET_SKIPS) {
            logger.warn(
                "shard [{}][{}] has skipped {} consecutive refreshes for cache budget; advancing anyway to keep staleness bounded",
                indexUuid,
                shardId,
                skips - 1
            );
            consecutiveOverBudgetSkips.set(0);
            return false;
        }
        logger.debug(
            "skipping manifest poll: node's reader-shard admission budget is currently exceeded, staying on generation {}",
            currentManifestGeneration.get()
        );
        return true;
    }

    /**
     * Whether the Lucene commit this engine currently has open is the one {@code manifest} names.
     *
     * <p>Nothing else checks this, and everything downstream assumes it. Lucene resolves "the
     * commit" in a directory as the highest-generation {@code segments_N} present, which is not the
     * same question as "which commit did the manifest I just applied name" whenever the two can
     * disagree -- and they can: a compaction's merged commit and a writer's own next commit are two
     * different byte-sequences that can carry the same generation number. {@code
     * ObjectStoreCommitMaterializer} removes superseded commit pointers precisely so they cannot,
     * and this is the assertion that the removal actually worked, checked where it matters rather
     * than trusted.
     */
    private boolean servesCommitNamedBy(CommitManifest manifest) {
        try {
            String open = org.apache.lucene.index.SegmentInfos.getLastCommitSegmentsFileName(engineConfig.getStore().directory());
            return manifest.segmentsFileName().equals(open);
        } catch (IOException e) {
            logger.warn("could not determine which commit shard [" + indexUuid + "][" + shardId + "] currently has open", e);
            return false;
        }
    }

    /**
     * Deletes everything in the store directory that neither the current nor the immediately
     * previous generation references -- see {@link
     * ObjectStoreCommitMaterializer#pruneUnreferencedFiles} for why one generation of slack is kept
     * and why a failure here is logged rather than propagated. Only meaningful on the eager path;
     * the lazy directory prunes its own file map inside {@code advanceToManifest}.
     */
    private void pruneSupersededFiles() {
        org.apache.lucene.store.Directory directory = engineConfig.getStore().directory();
        if (org.apache.lucene.store.FilterDirectory.unwrap(
            directory
        ) instanceof org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory) {
            return;
        }
        CommitManifest current = currentManifest.get();
        CommitManifest previous = previousManifest.get();
        if (current == null) {
            return;
        }
        try {
            List<CommitManifest> retained = new ArrayList<>(2);
            retained.add(current);
            if (previous != null) {
                retained.add(previous);
            }
            materializer.pruneUnreferencedFiles(directory, retained);
        } catch (Exception e) {
            logger.debug("failed to prune superseded reader files, will retry after a later advance", e);
        }
    }

    /**
     * Moves the adaptive poll interval, and schedules when the next real poll may run.
     *
     * <p>Back off only when both halves are true: nothing new was found <em>and</em> nobody has
     * queried this shard recently. A queried shard polls at the floor regardless of how quiet its
     * writer is, because that is the shard whose staleness someone can actually observe. Any
     * advance, any query, and any {@link #pollNow()} resets to the floor immediately.
     */
    private void updatePollInterval(boolean advanced) {
        long now = System.currentTimeMillis();
        long floor = MANIFEST_POLL_INTERVAL.millis();
        long next;
        if (advanced || millisSinceLastQuery() < POLL_BACKOFF_IDLE_THRESHOLD_MILLIS) {
            next = floor;
        } else {
            next = Math.min(MAX_MANIFEST_POLL_INTERVAL.millis(), currentPollIntervalMillis.get() * 2);
        }
        currentPollIntervalMillis.set(next);
        nextPollDueAtMillis.set(now + next);
    }

    /**
     * The scheduled entry point, which is deliberately not {@link #pollForNewerManifest} itself:
     * the schedule fires at the floor interval and this decides whether the current adaptive
     * interval has actually elapsed. A tick that has not is free -- no lock, no object-store
     * request -- which is the whole point of backing off.
     */
    private void pollTick() {
        if (System.currentTimeMillis() < nextPollDueAtMillis.get()) {
            return;
        }
        pollForNewerManifest();
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
    public synchronized void refresh(String source) throws EngineException {
        try {
            getReferenceManager(SearcherScope.EXTERNAL).maybeRefreshBlocking();
        } catch (IOException e) {
            throw new EngineException(engineConfig.getShardId(), "failed to refresh reader engine", e);
        }
    }

    /**
     * {@code synchronized} on the same monitor {@link #pollForNewerManifest} holds, and that is the
     * point of the keyword here rather than a nicety.
     *
     * <p>These two entry points are public and are called by core independently of this engine's own
     * poll: {@code IndexService}'s async refresh task reaches {@code Engine#maybeRefresh} on the
     * default one-second {@code index.refresh_interval}, and {@code _refresh} reaches the same path.
     * The poll was already {@code synchronized}; these were not, so they were not mutually excluded
     * from it. That left a real window: the poll begins materializing a generation -- which can mean
     * a multi-hundred-megabyte object-store read -- and a scheduled refresh fires inside it,
     * {@code openIfChanged} reads a segments file whose segment data has not landed yet, and the
     * refresh fails with {@code NoSuchFileException} (or, worse, opens a partially-written file).
     * Writing the segments file last closes most of that window; sharing the monitor closes the rest,
     * and costs nothing, because a refresh that would have raced is one that had nothing new to see.
     *
     * @param source a description of why the refresh was triggered, for logging/diagnostics
     * @return whether a reopen actually happened
     */
    @Override
    public synchronized boolean maybeRefresh(String source) throws EngineException {
        try {
            return getReferenceManager(SearcherScope.EXTERNAL).maybeRefresh();
        } catch (IOException e) {
            throw new EngineException(engineConfig.getShardId(), "failed to refresh reader engine", e);
        }
    }

    /**
     * The sequence-number statistics of the generation this engine currently serves, not of the one
     * it happened to open on -- see {@link #currentSeqNoStats}.
     *
     * @param globalCheckpoint the global checkpoint to report alongside this engine's own values
     * @return stats for the currently-open generation
     */
    @Override
    public SeqNoStats getSeqNoStats(long globalCheckpoint) {
        SeqNoStats current = currentSeqNoStats;
        if (current == null) {
            return super.getSeqNoStats(globalCheckpoint);
        }
        return new SeqNoStats(current.getMaxSeqNo(), current.getLocalCheckpoint(), globalCheckpoint);
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
        if (currentManifestGeneration.get() >= minGeneration) {
            listener.onResponse(true);
            return;
        }
        long deadlineMillis = System.currentTimeMillis() + Math.min(timeout.millis(), MAX_WAIT_FOR_GENERATION_MILLIS);
        generationWaiters.add(new GenerationWaiter(minGeneration, deadlineMillis, listener));
        // One shared poll serves every waiter, so N concurrent read-after-write callers on one shard
        // cost one object-store round trip per interval, not N. Kick it once here so the first
        // waiter does not have to sit out an interval it could have skipped.
        scheduleWaiterSweep(0L);
    }

    /**
     * Runs one poll on behalf of every pending waiter and resolves whichever of them that satisfied,
     * then reschedules itself for as long as any remain.
     *
     * <p>The shape this replaces did one {@code pollForNewerManifest} <em>per waiter</em> every
     * 100&nbsp;ms, and that method is {@code synchronized} and does a real shard-head read: one
     * waiter was ten object-store GETs a second for the whole timeout, N waiters were 10N GETs a
     * second plus N threads queued on this engine's monitor each holding it across a network round
     * trip -- and the shard's own background poll queued behind all of them, delaying the very
     * mechanism read-after-write depends on. Coalescing removes the multiplier entirely, and the
     * interval widening after the first couple of attempts removes most of what is left: a
     * generation that has not published within 200&nbsp;ms is not going to publish sooner because
     * this shard asked ten more times.
     */
    private void scheduleWaiterSweep(long delayMillis) {
        engineConfig.getThreadPool()
            .schedule(this::sweepGenerationWaiters, TimeValue.timeValueMillis(delayMillis), ThreadPool.Names.GENERIC);
    }

    private void sweepGenerationWaiters() {
        if (generationWaiters.isEmpty()) {
            return;
        }
        int attempt = waiterSweepAttempts.incrementAndGet();
        pollForNewerManifest();
        resolveGenerationWaiters();
        if (generationWaiters.isEmpty()) {
            waiterSweepAttempts.set(0);
            return;
        }
        // The first couple of sweeps run at the responsive 100 ms cadence read-after-write actually
        // needs; after that, fall back to the background poll interval rather than keeping up a
        // 10-per-second head-read rate for the rest of what may be a minute-long wait.
        long delay = attempt <= 2 ? WAIT_FOR_GENERATION_POLL_INTERVAL_MILLIS : MANIFEST_POLL_INTERVAL.millis();
        scheduleWaiterSweep(delay);
    }

    /** Resolves every waiter whose generation has arrived or whose deadline has passed. */
    private void resolveGenerationWaiters() {
        if (generationWaiters.isEmpty()) {
            return;
        }
        long generation = currentManifestGeneration.get();
        long now = System.currentTimeMillis();
        List<GenerationWaiter> stillWaiting = new ArrayList<>();
        for (GenerationWaiter waiter = generationWaiters.poll(); waiter != null; waiter = generationWaiters.poll()) {
            if (generation >= waiter.minGeneration) {
                waiter.listener.onResponse(true);
            } else if (now >= waiter.deadlineMillis) {
                waiter.listener.onResponse(false);
            } else {
                stillWaiting.add(waiter);
            }
        }
        generationWaiters.addAll(stillWaiting);
    }

    /** How many consecutive sweeps the current batch of waiters has already cost -- see {@link #sweepGenerationWaiters}. */
    private final AtomicInteger waiterSweepAttempts = new AtomicInteger();

    /**
     * The longest this engine will wait for a generation, whatever a caller asks for.
     *
     * <p>{@code WaitForGenerationRequest} accepts any non-negative timeout, so an hour-long wait was
     * a legal request. A read-after-write wait is a latency mechanism, not a subscription: past a
     * minute the honest answer is "not yet", and letting a caller hold a waiter (and its share of
     * the poll cadence) open indefinitely is a denial-of-service surface, not a feature.
     */
    static final long MAX_WAIT_FOR_GENERATION_MILLIS = 60_000L;

    /** One pending read-after-write waiter. */
    private static final class GenerationWaiter {
        private final long minGeneration;
        private final long deadlineMillis;
        private final org.opensearch.core.action.ActionListener<Boolean> listener;

        private GenerationWaiter(long minGeneration, long deadlineMillis, org.opensearch.core.action.ActionListener<Boolean> listener) {
            this.minGeneration = minGeneration;
            this.deadlineMillis = deadlineMillis;
            this.listener = listener;
        }
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
        // An explicit notification is exactly the signal the adaptive backoff exists to defer to:
        // reset to the floor first, so a shard that had backed off to 60 s does not merely catch up
        // once and then go straight back to sleep while more publications are landing.
        nextPollDueAtMillis.set(0L);
        currentPollIntervalMillis.set(MANIFEST_POLL_INTERVAL.millis());
        pollForNewerManifest();
        resolveGenerationWaiters();
    }

    /**
     * Takes, or moves forward, this engine's durable pin on the generation it is about to serve.
     *
     * <p>The design promises "open searchers pin their manifest generation ... so GC never yanks a
     * bundle out from under an in-flight query", and no reader pinned anything: the GC sweep is
     * always handed an <em>empty</em> lease-pin set, and the only protection was a retention window
     * justified as "comfortably longer than any legitimate reader's own manifest-generation lag,
     * bounded by the 5&nbsp;s poll interval". A reader's lag is not bounded by 5&nbsp;s in at least
     * four reachable states: a refresh deferred for cache budget (unbounded before this class
     * started counting consecutive skips), an object-store brownout in which every poll throws and
     * is swallowed, a shard suspended by scale-to-zero, and -- the case the promise was written
     * about -- a long-lived {@code Searcher}, which pins the Lucene reader but never the manifest
     * generation its files came from. On the eager path a deleted bundle is survivable because the
     * files are already local; on the lazy path it is fatal mid-query. And because a reader engine
     * hosts the GC scheduler itself, a reader stuck at generation G is the very process deleting G.
     *
     * <p>{@code DurablePinRegistry} is used rather than the {@code ShardDirectory} tier because it
     * is CAS-backed and correct cluster-wide (it is what point-in-time recovery already relies on),
     * while the directory tier is explicitly node-local in-memory hints. The pin carries a bounded
     * expiry so a crashed reader's pin ages out on its own instead of leaking a generation forever;
     * it is re-stamped on every advance, and the reader engine also refreshes it on every directory
     * tick, so a healthy reader's pin never lapses under it.
     *
     * @return whether the generation is now pinned (and therefore safe to advance onto).
     */
    private boolean takeOrMoveReaderPin(long primaryTerm, long generation) {
        if (pinRegistry == null) {
            // No registry configured for this shard: nothing to pin with, and refusing to open the
            // shard over it would be a far larger regression than the exposure it removes. The
            // retention window remains the only protection, exactly as before.
            return true;
        }
        org.opensearch.serverless.storage.retention.PinRecord pin = new org.opensearch.serverless.storage.retention.PinRecord(
            readerPinId(),
            primaryTerm,
            generation,
            localNodeId,
            System.currentTimeMillis() + READER_PIN_TTL_MILLIS
        );
        try {
            // replacePin, not addPin: this reason pins exactly one generation at a time, and
            // replacing atomically is what stops a reader that has advanced a thousand generations
            // from having pinned all thousand of them.
            pinRegistry.replacePin(indexUuid, shardId, pin);
            currentReaderPin.set(pin);
            return true;
        } catch (Exception e) {
            logger.warn("failed to pin generation " + generation + " for reader shard [" + indexUuid + "][" + shardId + "]", e);
            return false;
        }
    }

    /**
     * Re-stamps the current pin's expiry so a long-lived reader that has not advanced in a while
     * does not have its own generation collected out from under it. Called from the directory-entry
     * refresh tick, which already runs well inside {@link #READER_PIN_TTL_MILLIS}.
     */
    private void renewReaderPin() {
        org.opensearch.serverless.storage.retention.PinRecord pin = currentReaderPin.get();
        if (pinRegistry == null || pin == null) {
            return;
        }
        try {
            pinRegistry.confirmPin(indexUuid, shardId, pin.pinId(), System.currentTimeMillis() + READER_PIN_TTL_MILLIS);
        } catch (UnsupportedOperationException notSupported) {
            // Some registries cannot re-stamp; fall back to rewriting the pin outright, which is
            // idempotent for this reason since it only ever holds one generation.
            takeOrMoveReaderPin(pin.primaryTerm(), pin.generation());
        } catch (Exception e) {
            logger.debug("failed to renew the reader pin for [" + indexUuid + "][" + shardId + "]", e);
        }
    }

    private void releaseReaderPin() {
        org.opensearch.serverless.storage.retention.PinRecord pin = currentReaderPin.getAndSet(null);
        if (pinRegistry == null || pin == null) {
            return;
        }
        try {
            pinRegistry.removePin(indexUuid, shardId, pin);
        } catch (Exception e) {
            // Bounded leak, not a correctness problem: the pin carries an expiry precisely so a
            // reader that cannot clean up after itself does not hold a generation forever.
            logger.warn("failed to release the reader pin for [" + indexUuid + "][" + shardId + "]; it will expire on its own", e);
        }
    }

    /**
     * One pin id per (node, shard), so two reader copies of the same shard on different nodes pin
     * independently and neither can release the other's.
     */
    private String readerPinId() {
        return "reader:" + localNodeId + ":" + indexUuid + ":" + shardId;
    }

    /**
     * How long a reader's pin survives without being renewed. Long enough to ride out a brownout in
     * which every poll and every directory tick fails, short enough that a node that died holding a
     * pin stops blocking collection within a sweep or two.
     */
    static final long READER_PIN_TTL_MILLIS = 30 * 60_000L;

    /**
     * Lets {@link ReaderShardActivityRegistry} tell this engine where to remove itself from when it
     * stops being live -- see that class's {@code register}.
     *
     * @param registry the registry this engine was registered in
     * @param registeredIndexUuid the index UUID it was registered under
     * @param registeredShardId the shard id it was registered under
     */
    void attachActivityRegistry(ReaderShardActivityRegistry registry, String registeredIndexUuid, int registeredShardId) {
        RegistryRegistration registration = new RegistryRegistration(registry, registeredIndexUuid, registeredShardId);
        this.activityRegistration = registration;
        if (cleanedUp.get()) {
            // Lost a race with our own teardown -- remove immediately rather than leaving a dead
            // engine registered because it closed a moment before it was registered.
            registry.unregister(registeredIndexUuid, registeredShardId, this);
        }
    }

    /**
     * Every piece of teardown this engine owns, run exactly once.
     *
     * <p>It used to live directly in {@link #close()}, which is only one of the two ways this engine
     * stops being live. {@code Engine#failEngine} calls {@code closeNoLock} <em>directly</em> and
     * never goes through {@code close()} -- so on any engine failure (a corrupt read, an
     * {@code IOException} out of the reader manager: precisely the conditions the reader-side
     * correctness bugs produced) none of this ran. The consequences per failed reader shard were
     * concrete: the admission permit was never released, so the node's effective cap shrank toward
     * zero and eventually no reader shard could be allocated to it at all; the manifest poll kept
     * ticking every five seconds forever against a closed store; and the compaction and GC
     * schedulers kept running, so a <em>dead</em> shard went on merging and deleting in the object
     * store.
     *
     * <p>The guard matters in the other direction too. {@code Engine#close()} is itself idempotent,
     * but this override ran its body before delegating, unguarded -- so a second {@code close()}
     * called {@code Semaphore#release()} a second time, and a {@code Semaphore} has no ownership
     * check, silently <em>raising</em> the node's admission cap. One {@code compareAndSet} fixes
     * both directions.
     */
    private void cleanupOnce() {
        if (cleanedUp.compareAndSet(false, true) == false) {
            return;
        }
        Scheduler.Cancellable poll = manifestPollTask;
        if (poll != null) {
            poll.cancel();
        }
        directoryRefreshTask.cancel();
        // Resolve rather than abandon: a read-after-write caller waiting on a shard that just went
        // away should get a prompt "no", not a timeout.
        for (GenerationWaiter waiter = generationWaiters.poll(); waiter != null; waiter = generationWaiters.poll()) {
            waiter.listener.onResponse(false);
        }
        RegistryRegistration registration = activityRegistration;
        if (registration != null) {
            registration.registry().unregister(registration.indexUuid(), registration.shardId(), this);
        }
        // Cleans up this engine's own directory entry rather than leaving it to linger until its
        // TTL lapses (InMemoryShardDirectory has no active eviction, only lazy removal on a later
        // lookup -- see its own javadoc). Conditional, not a plain drop(): an unconditional removal
        // could just as easily discard a DIFFERENT, newer entry some other engine instance already
        // reported for this same shard (e.g. it already relocated and reopened elsewhere before this
        // instance got around to closing), which dropIfMatches's compare-and-remove semantics avoid.
        ShardDirectoryEntry ownEntry = lastReportedEntry.get();
        if (ownEntry != null) {
            shardDirectory.dropIfMatches(indexUuid, shardId, ownEntry);
        }
        closeQuietly(compactionSchedulerTask);
        closeQuietly(gcSchedulerTask);
        closeQuietly(partitionRewriteSchedulerTask);
        closeQuietly(pitrRetentionTask);
        releaseReaderPin();
        if (admissionController != null) {
            admissionController.release();
        }
    }

    /** A scheduler failing to stop must never prevent the rest of teardown -- especially not the admission permit. */
    private void closeQuietly(java.io.Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            logger.warn("failed to close a background scheduler for reader shard [" + indexUuid + "][" + shardId + "]", e);
        }
    }

    @Override
    public void close() throws IOException {
        cleanupOnce();
        super.close();
    }

    /**
     * The other way this engine stops being live: {@code Engine#failEngine} calls this directly,
     * bypassing {@link #close()} entirely. Overriding it is what makes teardown actually happen on
     * a failure rather than only on an orderly close -- see {@link #cleanupOnce()} for what was
     * being leaked.
     *
     * @param reason why the engine is being closed
     * @param closedLatch counted down by the superclass once the close completes
     */
    @Override
    protected void closeNoLock(String reason, CountDownLatch closedLatch) {
        cleanupOnce();
        super.closeNoLock(reason, closedLatch);
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
            partitionDescriptor,
            null
        );
    }

    /**
     * Same as {@link #open(EngineConfig, CommitManifest, ObjectStoreCommitMaterializer, long,
     * ShardStateStore, BlobContainerManifestStore, ShardDirectory, String, ReaderShardAdmissionController,
     * CompactionSchedulerConfig, GcSchedulerConfig, ShardPartitionDescriptor)}, with this shard's
     * own optional &sect;16 Phase 5 background partition-rewrite scheduler.
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
     * @param partitionRewriteConfig {@code null} to disable this reader's own background
     *        partition-rewrite scheduler -- see {@link PartitionRewriteSchedulerConfig}'s own
     *        javadoc. Only meaningful together with a non-null {@code partitionDescriptor}.
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
        ShardPartitionDescriptor partitionDescriptor,
        PartitionRewriteSchedulerConfig partitionRewriteConfig
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
            partitionDescriptor,
            partitionRewriteConfig,
            null
        );
    }

    /**
     * Same as {@link #open(EngineConfig, CommitManifest, ObjectStoreCommitMaterializer, long,
     * ShardStateStore, BlobContainerManifestStore, ShardDirectory, String, ReaderShardAdmissionController,
     * CompactionSchedulerConfig, GcSchedulerConfig, ShardPartitionDescriptor, PartitionRewriteSchedulerConfig)},
     * with this shard's own optional background PITR retention reconciliation -- see this class's
     * own javadoc for why a reader shard, not just the writer, must run this.
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
     * @param partitionRewriteConfig {@code null} to disable this reader's own background
     *        partition-rewrite scheduler -- see {@link PartitionRewriteSchedulerConfig}'s own
     *        javadoc. Only meaningful together with a non-null {@code partitionDescriptor}.
     * @param pitrRetentionConfig {@code null} to disable this reader's own background PITR
     *        retention reconciliation -- see {@link PitrRetentionConfig}'s own javadoc.
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
        ShardPartitionDescriptor partitionDescriptor,
        PartitionRewriteSchedulerConfig partitionRewriteConfig,
        PitrRetentionConfig pitrRetentionConfig
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
                manifest,
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
                partitionDescriptor,
                partitionRewriteConfig,
                pitrRetentionConfig
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
