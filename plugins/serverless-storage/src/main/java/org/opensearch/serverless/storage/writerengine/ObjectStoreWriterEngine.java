/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.DocumentIndexWriter;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.Engine.Delete;
import org.opensearch.index.engine.Engine.DeleteResult;
import org.opensearch.index.engine.Engine.Index;
import org.opensearch.index.engine.Engine.IndexResult;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineNativeSnapshotPointer;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.CompositeTranslogEventListener;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask;
import org.opensearch.serverless.storage.translog.WalMirroringTranslog;
import org.opensearch.serverless.storage.translog.WalMirroringTranslogFactory;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalGcSchedulerTask;
import org.opensearch.serverless.storage.wal.WalReplayRecovery;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

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

    private static final Logger logger = LogManager.getLogger(ObjectStoreWriterEngine.class);

    /** How long a directory entry for a writer shard is trusted before it's treated as stale. */
    private static final long DIRECTORY_ENTRY_TTL_MILLIS = 60_000L;

    /**
     * Refresh well inside the TTL, not at its edge: a refresh that only just beats expiry still
     * leaves a window where a slow/delayed scheduler tick lets the entry lapse anyway.
     */
    private static final TimeValue DIRECTORY_REFRESH_INTERVAL = TimeValue.timeValueMillis(DIRECTORY_ENTRY_TTL_MILLIS / 3);

    /**
     * How long this writer's lease on {@code ShardHead} is trusted before {@code
     * CompactionSchedulerTask} may treat the shard as available (rfc-serverless-opensearch.md
     * &sect;16 Phase 4.5). Deliberately much shorter than {@link #DIRECTORY_ENTRY_TTL_MILLIS}: the
     * directory entry only affects discoverability, while a stale lease is what lets a compactor
     * start touching a shard a writer still considers its own, so it is bounded tightly.
     */
    private static final long LEASE_TTL_MILLIS = 30_000L;

    /** Same well-inside-the-TTL rationale as {@link #DIRECTORY_REFRESH_INTERVAL}. */
    private static final TimeValue LEASE_RENEWAL_INTERVAL = TimeValue.timeValueMillis(LEASE_TTL_MILLIS / 3);

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
    private final Scheduler.Cancellable leaseRenewalTask;
    private final PitrRetentionSchedulerTask pitrRetentionTask;
    private final WalChunkService walChunkService;
    private final org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider;
    /**
     * Whether {@link #walChunkService}'s {@code WalShardRegistry} registration has ever succeeded
     * for this shard -- {@code true} makes {@link #registerWalShardIfNeeded} a no-op. Registration
     * is retried on every {@link #renewLease} tick until it succeeds once, not just attempted once
     * at construction: {@code WalGcSchedulerTask}'s safety bound is computed only over registered
     * shards, so a shard that never registers is invisible to it and its WAL chunks could be GC'd
     * while still needed for recovery -- a real data-loss gap a single, never-retried attempt left
     * open.
     */
    private final AtomicBoolean walShardRegistered = new AtomicBoolean(false);
    /**
     * Sweeps {@link #walChunkService}'s own container on a schedule -- non-{@code null} only for a
     * shard opted into a dedicated WAL stream (rfc-serverless-opensearch.md &sect;12's "dedicated
     * WAL streams" regulatory co-residency bullet), where {@link #walChunkService} is scoped to
     * exclusively this one shard rather than the node-shared container. Owned and closed by this
     * engine (see {@link #close()}) rather than by a node-level task the way the shared WAL's own
     * {@code WalGcSchedulerTask} is, because a per-shard container's lifecycle IS this engine's own
     * lifecycle -- there is no other event (no close hook exists on the registries/factories this
     * plugin builds elsewhere) that reliably fires when this shard stops using its dedicated
     * container. {@code null} when this shard shares the node-level WAL container, matching every
     * other optional-feature-off shape in this plugin.
     */
    private final WalGcSchedulerTask dedicatedWalGcSchedulerTask;

    /**
     * Non-positive disables rate limiting entirely (every {@code api}/{@code schedule}-sourced
     * refresh may attempt a publish); positive is the minimum real milliseconds between two
     * refresh-triggered publish attempts -- see {@link #maybePublishOnRefresh} and the constructor
     * javadoc that introduces this parameter.
     */
    private final long publicationRateLimitMillis;

    /** {@code null} disables writer-side publication notification entirely -- see that class's own javadoc. */
    private final WriterPublicationNotifier publicationNotifier;

    /**
     * {@code null} disables engine-native snapshot support entirely -- see {@link
     * #attemptEngineNativeSnapshot}, which always throws rather than returning empty when this is
     * {@code null}, so a snapshot attempt fails loudly instead of silently falling back to the
     * classic copy-based path this shard's own model doesn't actually support (this shard's local
     * commit and local {@code Directory} are not guaranteed to reflect its real durable state --
     * see the design writeup at {@code docs-site/src/content/docs/design/snapshot-restore-proposal.md}).
     */
    private final DurablePinRegistry pinRegistry;

    /**
     * Set (via {@link java.util.concurrent.atomic.AtomicLong#compareAndSet}, so two concurrent
     * refreshes can't both win) to {@code engineConfig.getThreadPool().relativeTimeInMillis()}
     * every time {@link #maybePublishOnRefresh} actually attempts a publish. {@code 0} initially,
     * meaning the very first {@code api}/{@code schedule} refresh after activation is never
     * rate-limited regardless of {@link #publicationRateLimitMillis}.
     */
    private final AtomicLong lastRefreshPublicationAttemptMillis = new AtomicLong(0);

    /**
     * Guards against dispatching a second async refresh-triggered publish while one is already
     * running for this engine -- see {@link #maybePublishOnRefresh}'s own javadoc for why.
     */
    private final AtomicBoolean refreshPublicationInFlight = new AtomicBoolean(false);

    /**
     * The fencing snapshot verified sound in {@code plugins/serverless-storage/formal/WalReplayFencing.tla}
     * (its {@code leaseTransferWalPos}): an exclusive upper bound on WAL chunk sequences that
     * existed at the moment this engine activated, captured as early as possible -- before {@code
     * super(engineConfig)} even runs, ahead of any of this engine's own construction work including
     * local translog recovery -- so it is as tight a bound on "chunks that existed before this
     * writer took over" as this architecture can currently produce. Consumed by {@link
     * #replayWalOperations()} as the exclusive upper bound {@link WalReplayRecovery#replayOperations}
     * filters WAL chunk replay against.
     *
     * <p><b>Closed for the live-promotion case, via {@link #onPrimaryTermBumped}</b>: this field is
     * re-snapshotted, atomically with the term bump itself, whenever {@code IndexShard} calls
     * {@link Engine#onPrimaryTermBumped(long)} on an already-constructed engine transitioning to a
     * new primary term -- see that method's own javadoc for exactly what atomicity core now
     * guarantees. The constructor-time snapshot below remains correct and untouched for the other
     * real activation path (a shard's primary being freshly allocated to a node that never held a
     * copy before): there, {@code EngineConfig#getPrimaryTermSupplier()} already reflects the
     * correct term by the time this constructor runs (traced directly against {@code
     * IndexShard}'s own constructor, which sets {@code pendingPrimaryTerm} from cluster metadata
     * before any engine construction begins), so no atomicity gap exists on that path and
     * {@link #onPrimaryTermBumped} is never even called for it.
     *
     * <p>{@code -1} when WAL mirroring is disabled ({@link #walChunkService} is {@code null}) --
     * there is no WAL chunk stream to bound in that case.
     */
    private volatile long activationWalPosition;

    /**
     * Wall-clock time of this engine's last {@link #index} or {@link #delete} call -- the
     * foundational signal rfc-serverless-opensearch.md &sect;16 Phase 4's "suspended writers,
     * scale-to-zero/cold-start" milestone needs (a controller can only decide a shard is idle if
     * something reports how long it's actually been idle), and the "autoscaling signal emitters"
     * bullet &sect;15 lists under plugin/module code. Initialized to construction time, not
     * {@code 0}, so a freshly opened engine reads as "just active," not as "idle since the epoch."
     *
     * <p>Deliberately tracks real write calls, not flush/commit/refresh activity: this plugin's own
     * background scheduling (directory refresh, lease renewal, compaction/GC/PITR ticks) all run on
     * fixed timers independent of whether the index is actually being written to, so using {@link
     * #commitIndexWriter} or {@link #refresh} as the signal would make an index that's genuinely
     * idle from a client's perspective look perpetually active. {@link #index}/{@link #delete} are
     * this engine's only two entry points client writes ever actually reach.
     *
     * <p>Only the timestamp is tracked here -- routing it into an actual suspension/scale-to-zero
     * decision, or exposing it over a stats/REST surface, is separate, still-open Phase 4 work; see
     * {@link #millisSinceLastActivity()}'s own javadoc.
     */
    private final AtomicLong lastActivityMillis = new AtomicLong(System.currentTimeMillis());

    /**
     * A deliberately crude two-window (previous/current) write-rate counter, mirroring {@code
     * ObjectStoreReaderEngine#queriesPerMinute()}'s own field of the same shape exactly (see that
     * field's own javadoc for the full reasoning -- repeated only in brief here): {@link
     * #windowStartMillis} marks when {@link #windowWriteCount} started accumulating, and once a
     * window has run for more than {@link #WRITE_RATE_WINDOW_MILLIS}, {@link #writesPerMinute()}
     * rolls it over -- the just-finished window's count becomes {@link #completedWindowWriteCount},
     * and a fresh window starts counting from 1 (the write that triggered the rollover).
     *
     * <p>This is intentionally not a real sliding window: {@link #writesPerMinute()} always reports
     * the <em>previous completed window's</em> rate, never the in-progress one, so this deliberately
     * under-reacts rather than over-reacts to a burst that just started. {@code
     * org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction} is this
     * signal's first, deliberately read-only, consumer -- see that class's own javadoc for why
     * nothing yet acts on it automatically.
     */
    private static final long WRITE_RATE_WINDOW_MILLIS = 60_000L;
    private final AtomicLong windowStartMillis = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong windowWriteCount = new AtomicLong(0);
    private final AtomicLong completedWindowWriteCount = new AtomicLong(0);

    /**
     * Set just before {@link #flushAndPublishQuiescent} forces a commit, read (and cleared) by
     * {@link #commitIndexWriter} to mark that one resulting manifest as {@link
     * CommitManifest#quiescent()} -- see {@link #flushAndPublishQuiescent}'s own javadoc for why
     * threading this through a field, rather than a parameter, is needed: {@link #commitIndexWriter}
     * is core's own callback, invoked from deep inside {@link InternalEngine#flush}, with a fixed
     * signature this class cannot add a parameter to.
     */
    private final java.util.concurrent.atomic.AtomicBoolean nextCommitIsQuiescent = new java.util.concurrent.atomic.AtomicBoolean(false);

    // Deliberately has NO initializer expression. InternalEngine's own constructor calls
    // getTranslogDeletionPolicy(EngineConfig) (overridden below) from inside super(engineConfig),
    // i.e. before this class's own field initializers would normally run -- an explicit
    // initializer here (even "= null") would execute afterward and clobber the value the override
    // assigns during super(). See rfc-serverless-opensearch.md &sect;7.1.1.
    private ObjectStoreDurabilityTranslogDeletionPolicy translogDeletionPolicy;

    // Same no-initializer pattern as translogDeletionPolicy above: createTranslogManager
    // (overridden below) is also called from inside super(engineConfig). Stays null when
    // walChunkService is null (WAL mirroring disabled) -- see commitIndexWriter's use of it.
    private WalMirroringTranslog walMirroringTranslog;

    /**
     * Bridges {@code walChunkService} across the {@code super(engineConfig)} call: {@link
     * #createTranslogManager} (overridden below) needs it, but is invoked from inside {@code
     * InternalEngine}'s own constructor, before this class's constructor body -- and therefore
     * before any constructor argument of this class, not just its fields -- is reachable from that
     * call. A thread-local set immediately before {@code super(...)} and cleared immediately after
     * is the standard way around this specific Java constructor-ordering limitation; construction
     * is synchronous and non-reentrant on one thread, so there is no window where this could leak
     * across two unrelated engines' construction.
     */
    private static final ThreadLocal<WalChunkService> CONSTRUCTION_WAL_CHUNK_SERVICE = new ThreadLocal<>();

    /** Bridges {@link #activationWalPosition} across {@code super(...)} the same way as {@link #CONSTRUCTION_WAL_CHUNK_SERVICE} above. */
    private static final ThreadLocal<Long> CONSTRUCTION_ACTIVATION_WAL_POSITION = new ThreadLocal<>();

    /** Bridges {@link #encryptionKeyProvider} across {@code super(...)} the same way as {@link #CONSTRUCTION_WAL_CHUNK_SERVICE} above -- {@link #createTranslogManager} needs it too, to decide whether to wrap {@link #walChunkService} in an {@code EncryptingWalChunkService} before mirroring into it. */
    private static final ThreadLocal<
        org.opensearch.serverless.storage.security.EncryptionKeyProvider> CONSTRUCTION_ENCRYPTION_KEY_PROVIDER = new ThreadLocal<>();

    /**
     * Creates a writer engine with neither PITR retention nor WAL mirroring configured, delegating
     * to the fuller constructor with both left {@code null}.
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, null, null);
    }

    /**
     * Creates a writer engine with WAL mirroring left disabled, delegating to the fuller constructor
     * with {@code walChunkService} set to {@code null}.
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     * @param pitrRetentionConfig {@code null} disables the periodic PITR retention reconciliation task; non-null schedules it
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, null);
    }

    /**
     * Creates a writer engine with WAL records left unencrypted, delegating to the fuller
     * constructor with {@code encryptionKeyProvider} set to {@code null}.
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     * @param pitrRetentionConfig {@code null} disables the periodic PITR retention reconciliation task; non-null schedules it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService, null);
    }

    /**
     * Creates a fully-configured writer engine: acquires this shard's writer lease, wires WAL
     * mirroring and, if configured, WAL encryption and PITR retention, then delegates to the
     * private constructor that actually runs {@code super(engineConfig)} after priming the
     * thread-locals {@link #createTranslogManager} needs.
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     * @param pitrRetentionConfig {@code null} disables the periodic PITR retention reconciliation task; non-null schedules it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted (matching
     *                              every prior caller's behavior); non-null wraps {@code
     *                              walChunkService} in an {@code EncryptingWalChunkService} so
     *                              records mirrored into it are encrypted the same way bundles and
     *                              manifests already are when this is configured (rfc-serverless-opensearch.md
     *                              &sect;12 bullet 1 -- see that section's own note on this
     *                              previously being built-but-unwired).
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, walChunkService, encryptionKeyProvider, null);
    }

    /**
     * Creates a fully-configured writer engine, additionally owning a dedicated WAL container's own
     * retention sweep for as long as this engine stays open.
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     * @param pitrRetentionConfig {@code null} disables the periodic PITR retention reconciliation task; non-null schedules it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted; non-null
     *                              wraps {@code walChunkService} in an {@code EncryptingWalChunkService}.
     * @param dedicatedWalGcSchedulerTask {@code null} for a shard sharing the node-level WAL
     *                                    container (its retention is swept by that container's own
     *                                    node-level task instead); non-null for a shard on a
     *                                    dedicated WAL stream, whose container this engine then owns
     *                                    sweeping for as long as it stays open -- see this field's
     *                                    own javadoc for why ownership lives here.
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        WalGcSchedulerTask dedicatedWalGcSchedulerTask
    ) {
        this(
            engineConfig,
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            encryptionKeyProvider,
            dedicatedWalGcSchedulerTask,
            0L
        );
    }

    /**
     * Creates a fully-configured writer engine, additionally rate-limiting how often an
     * externally-triggered refresh (rfc-serverless-opensearch.md &sect;8's "{@code _refresh}
     * changes meaning honestly": {@code api}/{@code schedule}-sourced refreshes now also flush and
     * publish, not just reopen the local reader) is allowed to trigger a publish.
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     * @param pitrRetentionConfig {@code null} disables the periodic PITR retention reconciliation task; non-null schedules it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted; non-null
     *                              wraps {@code walChunkService} in an {@code EncryptingWalChunkService}.
     * @param dedicatedWalGcSchedulerTask {@code null} for a shard sharing the node-level WAL
     *                                    container (its retention is swept by that container's own
     *                                    node-level task instead); non-null for a shard on a
     *                                    dedicated WAL stream, whose container this engine then owns
     *                                    sweeping for as long as it stays open -- see this field's
     *                                    own javadoc for why ownership lives here.
     * @param publicationRateLimitMillis non-positive (the default) disables rate limiting: every
     *                                   {@code api}/{@code schedule}-sourced refresh may trigger a
     *                                   publish (still a no-op if nothing has changed since the last
     *                                   commit, since it flows through the same non-forcing {@link
     *                                   #flush}). A positive value is the minimum real time between
     *                                   two refresh-triggered publish attempts on this engine,
     *                                   protecting the object store from a caller hammering
     *                                   {@code _refresh}.
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        WalGcSchedulerTask dedicatedWalGcSchedulerTask,
        long publicationRateLimitMillis
    ) {
        this(
            engineConfig,
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            encryptionKeyProvider,
            dedicatedWalGcSchedulerTask,
            publicationRateLimitMillis,
            null
        );
    }

    /**
     * Creates a fully-configured writer engine, additionally notifying reader copies after each
     * publish (rfc-serverless-opensearch.md &sect;8).
     *
     * @param engineConfig the core engine configuration for this shard
     * @param headPublisher publishes commits and manages lease acquisition/renewal for this shard's head
     * @param shardDirectory the shard's directory-registry entry, refreshed periodically while this engine is active
     * @param localNodeId the id of the node this engine is activating on
     * @param pitrRetentionConfig {@code null} disables the periodic PITR retention reconciliation task; non-null schedules it
     * @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin.
     * @param encryptionKeyProvider {@code null} leaves WAL-mirrored records unencrypted; non-null
     *                              wraps {@code walChunkService} in an {@code EncryptingWalChunkService}.
     * @param dedicatedWalGcSchedulerTask {@code null} for a shard sharing the node-level WAL
     *                                    container (its retention is swept by that container's own
     *                                    node-level task instead); non-null for a shard on a
     *                                    dedicated WAL stream, whose container this engine then owns
     *                                    sweeping for as long as it stays open -- see this field's
     *                                    own javadoc for why ownership lives here.
     * @param publicationRateLimitMillis non-positive (the default) disables rate limiting: every
     *                                   {@code api}/{@code schedule}-sourced refresh may trigger a
     *                                   publish (still a no-op if nothing has changed since the last
     *                                   commit, since it flows through the same non-forcing {@link
     *                                   #flush}). A positive value is the minimum real time between
     *                                   two refresh-triggered publish attempts on this engine,
     *                                   protecting the object store from a caller hammering
     *                                   {@code _refresh}.
     * @param publicationNotifier {@code null} disables writer-side publication notification
     *                            (same shape as every other optional feature in this plugin);
     *                            non-null notifies every reader copy of this shard after each
     *                            successful publish -- see that class's own javadoc for the
     *                            mechanism.
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        WalGcSchedulerTask dedicatedWalGcSchedulerTask,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier
    ) {
        this(
            engineConfig,
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            encryptionKeyProvider,
            dedicatedWalGcSchedulerTask,
            publicationRateLimitMillis,
            publicationNotifier,
            null
        );
    }

    /**
     * Creates a fully-configured writer engine, additionally able to pin its own manifest
     * generations against GC when this shard is snapshotted -- see {@link #attemptEngineNativeSnapshot}.
     * Every other parameter is as documented on the narrower overload above.
     *
     * @param pinRegistry {@code null} disables engine-native snapshot support entirely (same shape
     *                    as every other optional feature in this plugin) -- {@link
     *                    #attemptEngineNativeSnapshot} always throws in that case, rather than
     *                    silently falling back to the classic copy-based snapshot path.
     */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        WalGcSchedulerTask dedicatedWalGcSchedulerTask,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier,
        DurablePinRegistry pinRegistry
    ) {
        this(
            engineConfig,
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            encryptionKeyProvider,
            dedicatedWalGcSchedulerTask,
            publicationRateLimitMillis,
            publicationNotifier,
            pinRegistry,
            beginConstruction(walChunkService, encryptionKeyProvider)
        );
    }

    /** @param ignored only exists so the thread-local set above can run as an argument expression, strictly before {@code super(...)}. */
    private ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider,
        WalGcSchedulerTask dedicatedWalGcSchedulerTask,
        long publicationRateLimitMillis,
        WriterPublicationNotifier publicationNotifier,
        DurablePinRegistry pinRegistry,
        Void ignored
    ) {
        super(engineConfig);
        this.publicationRateLimitMillis = publicationRateLimitMillis;
        this.publicationNotifier = publicationNotifier;
        this.pinRegistry = pinRegistry;
        CONSTRUCTION_WAL_CHUNK_SERVICE.remove();
        CONSTRUCTION_ENCRYPTION_KEY_PROVIDER.remove();
        this.activationWalPosition = CONSTRUCTION_ACTIVATION_WAL_POSITION.get();
        CONSTRUCTION_ACTIVATION_WAL_POSITION.remove();
        this.headPublisher = headPublisher;
        this.indexUuid = engineConfig.getShardId().getIndex().getUUID();
        this.shardId = engineConfig.getShardId().getId();
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.walChunkService = walChunkService;
        this.encryptionKeyProvider = encryptionKeyProvider;
        this.dedicatedWalGcSchedulerTask = dedicatedWalGcSchedulerTask;
        registerWalShardIfNeeded();
        // Acquired synchronously, before this engine is usable, so the lease is visible to any
        // observability/diagnostics code that inspects it (see acquireOrRenewLease's javadoc --
        // fencing correctness itself lives entirely in publishCommitAsHead, not here; compaction no
        // longer gates on lease presence). A failure here means real evidence (a publication) that
        // this node's term assignment is already stale -- it must not come up as this shard's
        // writer at all. super(engineConfig) above has already opened the local translog/store, so
        // those must be closed before rethrowing -- this constructor cannot rely on close() being
        // called for it, since a constructor that throws never produces an object for a caller to
        // close.
        try {
            boolean acquired = headPublisher.acquireOrRenewLease(
                indexUuid,
                shardId,
                engineConfig.getPrimaryTermSupplier().getAsLong(),
                localNodeId,
                System.currentTimeMillis() + LEASE_TTL_MILLIS
            );
            if (acquired == false) {
                throw new EngineException(
                    engineConfig.getShardId(),
                    "failed to acquire writer lease: a newer primary term already holds this shard's head"
                );
            }
        } catch (EngineException e) {
            IOUtils.closeWhileHandlingException(super::close);
            throw e;
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(super::close);
            throw new EngineException(engineConfig.getShardId(), "failed to acquire writer lease", e);
        }
        // Report once synchronously so the shard is discoverable immediately on activation, rather
        // than waiting out the first refresh interval; scheduleWithFixedDelay's first execution
        // only happens after DIRECTORY_REFRESH_INTERVAL elapses, not on registration.
        refreshDirectoryEntry();
        this.directoryRefreshTask = engineConfig.getThreadPool()
            .scheduleWithFixedDelay(this::refreshDirectoryEntry, DIRECTORY_REFRESH_INTERVAL, ThreadPool.Names.GENERIC);
        this.leaseRenewalTask = engineConfig.getThreadPool()
            .scheduleWithFixedDelay(this::renewLease, LEASE_RENEWAL_INTERVAL, ThreadPool.Names.GENERIC);
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

    /**
     * Overrides core's default age/size/total-files translog retention with one driven by
     * object-store durability instead (rfc-serverless-opensearch.md &sect;7.1.1) -- see {@link
     * ObjectStoreDurabilityTranslogDeletionPolicy}'s own javadoc for the full safety argument.
     *
     * @param engineConfig the core engine configuration for this shard
     * @return the durability-driven translog deletion policy this engine retains for later use
     */
    @Override
    protected TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
        translogDeletionPolicy = new ObjectStoreDurabilityTranslogDeletionPolicy();
        return translogDeletionPolicy;
    }

    /**
     * Records this call as write activity (see {@link #lastActivityMillis}'s own javadoc) before
     * delegating to {@link InternalEngine#index}, unless {@code index.origin().isRecovery()} --
     * {@code LOCAL_TRANSLOG_RECOVERY} (this engine's own translog replay on open, e.g. after a
     * crash/restart) and {@code PEER_RECOVERY} both re-apply operations that already happened, not
     * new client activity, and counting them would make a genuinely idle-but-just-recovered shard
     * misreport as freshly active. Timestamped unconditionally on entry for every other origin
     * (i.e. every real client write), not only on success -- a client genuinely attempting to write
     * is not an idle shard even if the write itself later fails (e.g. a version conflict), and
     * gating on the result here would need inspecting {@link IndexResult} after the fact for no
     * real benefit to what this signal means.
     *
     * @param index the operation to index, delegated to {@link InternalEngine#index} unchanged
     * @return whatever {@link InternalEngine#index} returns
     */
    @Override
    public IndexResult index(Index index) throws IOException {
        if (index.origin().isRecovery() == false) {
            long now = System.currentTimeMillis();
            lastActivityMillis.set(now);
            recordWriteForRateCounter(now);
        }
        return super.index(index);
    }

    /**
     * Same reasoning as {@link #index}, for the delete entry point.
     *
     * @param delete the operation to delete, delegated to {@link InternalEngine#delete} unchanged
     * @return whatever {@link InternalEngine#delete} returns
     */
    @Override
    public DeleteResult delete(Delete delete) throws IOException {
        if (delete.origin().isRecovery() == false) {
            lastActivityMillis.set(System.currentTimeMillis());
        }
        return super.delete(delete);
    }

    /**
     * How long it's been since this engine last saw a real client {@link #index}/{@link #delete}
     * call -- the raw signal, not a decision. Nothing in this plugin currently reads this value
     * (see {@link #lastActivityMillis}'s own javadoc for what routing it into an actual
     * suspension/scale-to-zero decision or a stats/REST surface would still need); this method
     * exists so that future work has the data already being tracked, the same incremental shape
     * {@link #activationWalPosition} was built in.
     *
     * @return milliseconds elapsed since the last {@link #index} or {@link #delete} call, or since
     *         this engine was constructed if neither has ever been called
     */
    public long millisSinceLastActivity() {
        return System.currentTimeMillis() - lastActivityMillis.get();
    }

    /**
     * Rolls {@link #windowStartMillis}/{@link #windowWriteCount} over into {@link
     * #completedWindowWriteCount} once the current window has run longer than {@link
     * #WRITE_RATE_WINDOW_MILLIS}, then counts {@code now}'s write into whichever window is current
     * after that possible rollover. Synchronized for the same reason {@code
     * ObjectStoreReaderEngine#recordQueryForRateCounter} is: rollover is a compound
     * check-then-reset that must not race with itself across concurrent writes.
     *
     * @param now the current wall-clock time, as already computed by the caller.
     */
    private synchronized void recordWriteForRateCounter(long now) {
        long start = windowStartMillis.get();
        if (now - start >= WRITE_RATE_WINDOW_MILLIS) {
            completedWindowWriteCount.set(windowWriteCount.get());
            windowStartMillis.set(now);
            windowWriteCount.set(1);
        } else {
            windowWriteCount.incrementAndGet();
        }
    }

    /**
     * A conservative, always-one-window-stale estimate of this engine's own real client-facing
     * write rate, mirroring {@code ObjectStoreReaderEngine#queriesPerMinute()} exactly -- see that
     * method's own javadoc for the full reasoning behind reporting the previous completed window
     * rather than the in-progress one. Reports {@code 0} until this engine has completed at least
     * one full {@link #WRITE_RATE_WINDOW_MILLIS} window since construction (or since its last
     * write, if writes have since gone fully idle for a window).
     *
     * <p>{@code org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction}
     * reads this value, but only to surface a candidate signal -- unlike the reader side, there is
     * no "writer replica count" knob to scale up (exactly one primary per shard), and {@code
     * ShardSplitter.split} only re-points an already-provisioned target shard identity, it does not
     * create or allocate one, so there is still no automatic action wired to this signal. See
     * {@code ShardSplitCandidatesAction}'s own javadoc for the full reasoning.
     *
     * @return writes observed during the previous completed {@link #WRITE_RATE_WINDOW_MILLIS} window.
     */
    public long writesPerMinute() {
        return completedWindowWriteCount.get();
    }

    /**
     * This shard's current on-object-store size, in bytes, from the total file size of its own
     * latest published manifest ({@code ManifestSegmentMetrics#totalBytes}) -- the split-for-size
     * counterpart to {@link #writesPerMinute()}'s split-for-heat signal
     * (dynamic-partitioning-plan.md Phase 1 item 1.1). No local I/O and no bundle needs opening:
     * {@code ManifestSegmentMetrics#from} derives it entirely from the manifest's own file map,
     * already fetched here via {@link #headPublisher}, the same manifest-metadata-only approach
     * {@code CompactionSchedulerTask} uses for its own size-based triggers.
     *
     * @return the latest published manifest's total file size in bytes, or {@code 0} if this shard
     *         has never published a manifest yet or the read failed -- a best-effort advisory
     *         signal, same tolerance {@link #writesPerMinute()}'s own callers already have for a
     *         momentarily-unavailable estimate.
     */
    public long shardSizeInBytes() {
        try {
            return headPublisher.readLatestManifest(indexUuid, shardId)
                .map(manifest -> org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics.from(manifest).totalBytes)
                .orElse(0L);
        } catch (IOException e) {
            return 0L;
        }
    }

    /**
     * A real-time existence/version/seqNo check against this engine's own live version map
     * (rfc-serverless-opensearch.md &sect;8: "{@code _get} by document id can optionally route to
     * the writer shard for true realtime gets"). Delegates straight to the inherited {@link
     * InternalEngine#get} -- this class overrides neither {@code get} nor anything it depends on,
     * so this is exactly the same real-time-get machinery any ordinary {@link InternalEngine}
     * shard already provides, just reached directly rather than through core's own routing.
     *
     * <p>A reader engine ({@code ObjectStoreReaderEngine}) cannot answer this authoritatively at
     * all -- it has no live version map, only whatever manifest generation it last materialized,
     * which can lag the writer's true state by up to {@code MANIFEST_POLL_INTERVAL}. This method
     * is what {@code TransportRealtimeGetAction} calls when a caller explicitly opts into routing
     * to the writer instead.
     *
     * @param id the document id to look up
     * @return the real-time existence/version/seqNo result
     */
    public RealtimeGetResult realtimeGet(String id) {
        org.apache.lucene.index.Term uidTerm = new org.apache.lucene.index.Term(
            org.opensearch.index.mapper.IdFieldMapper.NAME,
            org.opensearch.index.mapper.Uid.encodeId(id)
        );
        try (Engine.GetResult result = get(new Engine.Get(true, true, id, uidTerm), this::acquireSearcher)) {
            if (result.exists() == false) {
                return new RealtimeGetResult(false, org.opensearch.common.lucene.uid.Versions.NOT_FOUND, -1, -1);
            }
            return new RealtimeGetResult(true, result.version(), result.docIdAndVersion().seqNo, result.docIdAndVersion().primaryTerm);
        }
    }

    /**
     * Sets {@link #CONSTRUCTION_WAL_CHUNK_SERVICE} and, before anything else about this engine's
     * activation happens, snapshots {@link #activationWalPosition} into {@link
     * #CONSTRUCTION_ACTIVATION_WAL_POSITION} -- capturing it here, ahead of {@code super(...)}, is
     * what makes it as tight a bound as this architecture can currently produce (see {@link
     * #activationWalPosition}'s own javadoc). Returns {@code null} so it can be used as the last
     * argument evaluated before {@code super(...)}.
     */
    private static Void beginConstruction(
        WalChunkService walChunkService,
        org.opensearch.serverless.storage.security.EncryptionKeyProvider encryptionKeyProvider
    ) {
        CONSTRUCTION_WAL_CHUNK_SERVICE.set(walChunkService);
        CONSTRUCTION_ENCRYPTION_KEY_PROVIDER.set(encryptionKeyProvider);
        long activationWalPosition;
        try {
            // A live read (see WalChunkService#currentChunkSequenceUpperBound's own javadoc for
            // why it must be live, not cached) -- a failure here means this writer cannot safely
            // establish its own fencing bound, so it must not activate at all rather than silently
            // using a wrong one.
            activationWalPosition = walChunkService == null ? -1L : walChunkService.currentChunkSequenceUpperBound();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to snapshot activationWalPosition for writer engine activation", e);
        }
        CONSTRUCTION_ACTIVATION_WAL_POSITION.set(activationWalPosition);
        return null;
    }

    /**
     * Wires WAL mirroring into this engine's translog (rfc-serverless-opensearch.md &sect;6.4):
     * when {@link #walChunkService} is configured, every operation appended to this shard's
     * translog is additionally mirrored into it, and {@link WalMirroringTranslog#lastFlushedWalChunkSequence()}
     * becomes {@link #commitIndexWriter}'s source for a real {@link WalPosition} instead of the
     * placeholder used before this was wired in. {@code InternalEngine.createTranslogManager}'s own
     * body is duplicated here rather than delegated to, because the one seam that would avoid that
     * -- {@code EngineConfig#getTranslogFactory()} -- is fixed before this engine is even
     * constructed (built by core, immutable, no plugin hook to override it per-shard); overriding
     * this method and substituting a {@link WalMirroringTranslogFactory} directly is the actual
     * available seam, matching {@code getTranslogDeletionPolicy} and
     * {@code globalCheckpointSupplierForCombinedDeletionPolicy} above.
     *
     * @param translogUUID the UUID of the translog being opened or created for this shard
     * @param translogDeletionPolicy the deletion policy to associate with the translog manager
     * @param translogEventListener the listener to notify of translog lifecycle events
     * @return the translog manager for this engine, with WAL mirroring wired in when configured
     */
    @Override
    protected TranslogManager createTranslogManager(
        String translogUUID,
        TranslogDeletionPolicy translogDeletionPolicy,
        CompositeTranslogEventListener translogEventListener
    ) throws IOException {
        WalChunkService configuredWalChunkService = CONSTRUCTION_WAL_CHUNK_SERVICE.get();
        if (configuredWalChunkService == null) {
            return super.createTranslogManager(translogUUID, translogDeletionPolicy, translogEventListener);
        }
        org.opensearch.serverless.storage.security.EncryptionKeyProvider configuredEncryptionKeyProvider =
            CONSTRUCTION_ENCRYPTION_KEY_PROVIDER.get();
        org.opensearch.serverless.storage.wal.WalAppendTarget walAppendTarget = configuredEncryptionKeyProvider == null
            ? configuredWalChunkService
            : new org.opensearch.serverless.storage.wal.EncryptingWalChunkService(
                configuredWalChunkService,
                configuredEncryptionKeyProvider
            );
        InternalTranslogManager manager = new InternalTranslogManager(
            engineConfig.getTranslogConfig(),
            engineConfig.getPrimaryTermSupplier(),
            engineConfig.getGlobalCheckpointSupplier(),
            translogDeletionPolicy,
            engineConfig.getShardId(),
            readLock,
            this::getLocalCheckpointTracker,
            translogUUID,
            translogEventListener,
            this::ensureOpen,
            new WalMirroringTranslogFactory(walAppendTarget),
            engineConfig.getStartedPrimarySupplier(),
            TranslogOperationHelper.create(engineConfig)
        );
        this.walMirroringTranslog = (WalMirroringTranslog) manager.getTranslog();
        return manager;
    }

    /**
     * The other half of &sect;7.1.1's local retention design: widens the threshold {@link
     * org.opensearch.index.engine.CombinedDeletionPolicy} uses to decide which local Lucene
     * commits are safe to delete, from just the translog's last-synced global checkpoint to
     * {@code max(that checkpoint, the same durability watermark the translog policy above already
     * tracks)}. This can only permit deleting <em>more</em> than core's default would (see this
     * hook's own javadoc on {@link org.opensearch.index.engine.Engine}) -- reusing {@link
     * #translogDeletionPolicy}'s watermark rather than tracking a second one keeps "what's
     * durable" a single source of truth shared by both halves of this design. Called from within
     * {@code InternalEngine}'s constructor before this class's own field initializers would
     * normally run, same as {@link #getTranslogDeletionPolicy}, but that's not a hazard here: the
     * returned supplier only dereferences {@code translogDeletionPolicy} lazily, when actually
     * invoked (well after construction completes), and by then it's already been assigned --
     * {@link #getTranslogDeletionPolicy} runs earlier in the same constructor.
     *
     * @param translogManagerRef the translog manager whose last-synced global checkpoint feeds the returned supplier
     * @return a supplier of the more permissive of the translog's synced checkpoint and the durability watermark
     */
    @Override
    protected LongSupplier globalCheckpointSupplierForCombinedDeletionPolicy(TranslogManager translogManagerRef) {
        return () -> Math.max(translogManagerRef.getLastSyncedGlobalCheckpoint(), translogDeletionPolicy.durablyPublishedMaxSeqNo());
    }

    /** The durability-driven translog deletion policy this engine constructed -- test-only visibility, not part of the plugin's contract. */
    ObjectStoreDurabilityTranslogDeletionPolicy translogDeletionPolicyForTesting() {
        return translogDeletionPolicy;
    }

    /** The WAL-mirroring translog this engine constructed, or {@code null} if WAL mirroring is disabled -- test-only visibility. */
    WalMirroringTranslog walMirroringTranslogForTesting() {
        return walMirroringTranslog;
    }

    /**
     * See {@link #activationWalPosition}'s own javadoc -- test-only visibility. Public, not
     * package-private like this class's other test-only accessors, specifically so a real
     * multi-node {@code internalClusterTest} (a different Gradle source set/package than this
     * class's own unit tests) can observe it after a genuine live promotion, not just via a
     * same-package unit test.
     */
    public long activationWalPositionForTesting() {
        return activationWalPosition;
    }

    /**
     * Re-snapshots {@link #activationWalPosition} atomically with a live term bump on this
     * already-constructed engine -- see that field's own javadoc for the full "closes the
     * live-promotion case" rationale, and {@link org.opensearch.index.engine.Engine#onPrimaryTermBumped(long)}'s
     * own javadoc for the exact atomicity core now guarantees this is called under.
     *
     * <p>Deliberately does not itself trigger a replay: {@link #replayWalOperations()} already runs
     * once, during this engine's own construction; this only tightens the bound {@link
     * #activationWalPosition} holds for any future replay this engine performs (e.g. a resync),
     * consistent with the rest of this class's own "capture as early/atomically as possible, ahead
     * of anything that would consume it" discipline.
     *
     * @param newPrimaryTerm the primary term this engine is now operating under.
     */
    @Override
    public void onPrimaryTermBumped(long newPrimaryTerm) {
        if (walChunkService == null) {
            return;
        }
        try {
            this.activationWalPosition = walChunkService.currentChunkSequenceUpperBound();
        } catch (IOException e) {
            // Matches beginConstruction's own "must not activate under a wrong bound" reasoning,
            // but this fires deep inside IndexShard#bumpPrimaryTerm's own onResponse callback,
            // which itself has no checked-exception escape hatch -- failing the shard outright
            // here (rather than silently keeping a now-possibly-stale bound) is the honest
            // response to a live read failing at exactly the moment correctness depends on it.
            throw new org.opensearch.index.engine.EngineException(
                engineConfig.getShardId(),
                "failed to re-snapshot activationWalPosition on primary term bump to " + newPrimaryTerm,
                e
            );
        }
    }

    /**
     * The manifest's real {@code WalPosition} once WAL mirroring is wired in (&sect;6.4), or {@code
     * null} when WAL mirroring is disabled ({@link #walMirroringTranslog} is {@code null}) -- this
     * manifest simply carries no real WAL coverage information. {@code null} here, not a
     * placeholder {@link WalPosition}, is load-bearing: {@code WalGcSchedulerTask#sweep} treats a
     * {@code null} manifest {@code walPosition()} as "this shard's coverage is unknown, skip this
     * tick" (see that class's own class javadoc), and previously could not tell that case apart
     * from a genuine, real position at chunk sequence {@code 0} -- an earlier {@code (String.valueOf(primaryTerm),
     * 0)} placeholder was non-{@code null} and silently defeated that bail-out, pinning WAL GC's
     * deletable bound at {@code 0} for the whole shared container, cluster-wide, for as long as any
     * registered shard's latest manifest carried it. {@link CommitManifest} already fully supports
     * a {@code null} {@code walPosition} (see e.g. {@code ShardShrinker#shrink}, which always
     * publishes one), so no other caller needed to change for this.
     */
    private WalPosition currentWalPosition() {
        if (walMirroringTranslog == null) {
            return null;
        }
        return new WalPosition(walChunkService.writerEpoch(), walMirroringTranslog.lastFlushedWalChunkSequence());
    }

    /**
     * Fetches, filters, and decodes the WAL operations this shard needs to catch up on between the
     * last durably-published manifest and this writer's own {@link #activationWalPosition} --
     * {@code plugins/serverless-storage/formal/WalReplayFencing.tla}'s verified {@code FixedReplay}
     * design, via {@link WalReplayRecovery}. The term floor passed is one term back from what this
     * writer is activating under ({@code WalReplayFencing.tla}'s {@code ReplayFloor}), so a
     * predecessor's legitimately-durable-but-not-yet-manifested records are not wrongly excluded by
     * term alone -- see {@link org.opensearch.serverless.storage.wal.WalChunkReader
     * #filterByShardAndMinimumTerm}'s own javadoc for why that term filter needs the position
     * cutoff alongside it.
     *
     * <p>Returns an empty list when WAL mirroring is disabled ({@link #walChunkService} is
     * {@code null}) -- there is nothing durable in the WAL to replay from, and local recovery
     * (already run inside {@code super(engineConfig)}, before this method could ever be called) is
     * this engine's only recovery mechanism in that configuration, same as any other {@code
     * InternalEngine}.
     *
     * <p>Wired to {@code IndexShard} via {@link #engineRecoveryOperations()} below, which is what
     * {@code Engine}'s own javadoc for that method points to as the real override -- see there for
     * why the actual apply step (mapping-aware document parsing, version/seqno bookkeeping) has to
     * happen in {@code IndexShard}, not here.
     */
    List<Translog.Operation> replayWalOperations() throws IOException {
        if (walChunkService == null) {
            return List.of();
        }
        long currentTerm = engineConfig.getPrimaryTermSupplier().getAsLong();
        long minPrimaryTerm = currentTerm - 1;
        Optional<CommitManifest> latestManifest = headPublisher.readLatestManifest(indexUuid, shardId);
        WalPosition lastDurableWalPosition = latestManifest.map(CommitManifest::walPosition).orElse(null);
        return WalReplayRecovery.replayOperations(
            walChunkService.blobContainer(),
            indexUuid,
            shardId,
            minPrimaryTerm,
            lastDurableWalPosition,
            activationWalPosition,
            encryptionKeyProvider
        );
    }

    /**
     * {@code Engine}'s own additive core seam (server module, not this plugin) for exactly this
     * purpose: {@code IndexShard#openEngineAndRecoverFromTranslog()} calls this once local translog
     * recovery has completed, and replays whatever it returns through the identical
     * {@code applyTranslogOperation} path local translog recovery just used -- see that method's
     * own javadoc on {@code Engine} for the full contract. A failure fetching/decoding the WAL is
     * surfaced as an {@link EngineException} (this engine cannot safely become usable with a
     * recovery gap silently swallowed) rather than degrading to local-only recovery.
     */
    @Override
    public List<Translog.Operation> engineRecoveryOperations() {
        try {
            return replayWalOperations();
        } catch (IOException e) {
            throw new EngineException(engineConfig.getShardId(), "failed to replay WAL operations for activation", e);
        }
    }

    /**
     * This engine's own already-durable manifest publication already <em>is</em> a complete,
     * addressable point-in-time copy of this shard (the same insight {@link
     * WriterEngineFactory#ownsRemoteSegmentDurability} documents) -- so a real {@code _snapshot} request never needs
     * to copy segment bytes for this engine the way {@code Engine#acquireLastIndexCommit}-based
     * snapshotting does. This pins the latest manifest generation under {@code snapshotId} (via
     * {@link ObjectStoreCommitHeadPublisher#readLatestManifestWithPin}, the same pin-before-read
     * ordering {@code ShardCloner#clone} uses) and returns it, serialized, as the pointer -- see
     * {@link EngineNativeSnapshotSupport} for how the matching restore/release hooks consume it.
     *
     * <p>Never returns {@link Optional#empty()}: an activated writer engine always supports
     * engine-native snapshots when {@link #pinRegistry} is configured (the normal case -- see that
     * field's own javadoc for the {@code null} opt-out). If nothing has been published yet, that is
     * a real failure to surface (there is nothing yet to snapshot), not a silent signal to fall
     * back to the classic path this shard's model doesn't actually support.
     *
     * @throws EngineException if {@link #pinRegistry} is {@code null} (feature disabled), if no
     *                          manifest has ever been published yet, or if pinning/serialization
     *                          itself failed.
     */
    @Override
    public Optional<EngineNativeSnapshotPointer> attemptEngineNativeSnapshot(SnapshotId snapshotId) throws EngineException {
        if (pinRegistry == null) {
            throw new EngineException(engineConfig.getShardId(), "engine-native snapshot support is not configured for this shard");
        }
        Optional<CommitManifest> manifest;
        try {
            manifest = headPublisher.readLatestManifestWithPin(indexUuid, shardId, pinRegistry, snapshotId.getUUID());
        } catch (IOException e) {
            throw new EngineException(engineConfig.getShardId(), "failed to produce engine-native snapshot pointer", e);
        }
        if (manifest.isEmpty()) {
            throw new EngineException(engineConfig.getShardId(), "no manifest has been published yet for this shard");
        }
        try {
            byte[] payload = new EngineNativeSnapshotPayload(snapshotId.getUUID(), manifest.get()).toBytes();
            return Optional.of(new EngineNativeSnapshotPointer(EngineNativeSnapshotSupport.ENGINE_ID, payload));
        } catch (IOException e) {
            // readLatestManifestWithPin above already registered a durable pin for this generation.
            // If serialization fails here, no pointer is ever returned for anything downstream (a
            // later snapshot delete) to key a release off of -- release it ourselves before failing,
            // or it is orphaned forever.
            try {
                pinRegistry.removePin(indexUuid, shardId, snapshotId.getUUID());
            } catch (IOException releaseFailure) {
                e.addSuppressed(releaseFailure);
            }
            throw new EngineException(engineConfig.getShardId(), "failed to produce engine-native snapshot pointer", e);
        }
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

    /**
     * Best-effort, like {@link #refreshDirectoryEntry}: a transient failure here is logged and
     * retried next tick rather than failing the engine, since the lease's own {@link
     * #LEASE_TTL_MILLIS} margin -- renewed well before expiry, same rationale as {@link
     * #DIRECTORY_REFRESH_INTERVAL} -- already bounds how stale a missed renewal can leave things.
     * If renewals keep failing until the lease actually lapses, {@code CompactionSchedulerTask} may
     * start treating this shard as available -- an efficiency question (a compactor briefly racing
     * this still-live writer's own local merges), not a correctness one: publication remains
     * protected by {@code ShardHead}'s own CAS, so nothing is lost or corrupted either way.
     */
    private void renewLease() {
        try {
            headPublisher.acquireOrRenewLease(
                indexUuid,
                shardId,
                engineConfig.getPrimaryTermSupplier().getAsLong(),
                localNodeId,
                System.currentTimeMillis() + LEASE_TTL_MILLIS
            );
        } catch (Exception e) {
            logger.warn("failed to renew writer lease, will retry next tick", e);
        }
        registerWalShardIfNeeded();
    }

    /**
     * Registers this shard in {@code WalShardRegistry}, retrying on every {@link #renewLease} tick
     * until it succeeds once -- see {@link #walShardRegistered}'s own javadoc for why a single,
     * never-retried attempt at construction left a real gap. Once per activation is still all this
     * needs on the happy path (the grow-only registry only needs to know "has this shard ever used
     * this container," not continuous confirmation -- see {@code WalShardRegistry}'s own javadoc),
     * so {@link #walShardRegistered} makes every call after the first successful one a no-op.
     */
    private void registerWalShardIfNeeded() {
        if (walChunkService == null || walShardRegistered.get()) {
            return;
        }
        try {
            new org.opensearch.serverless.storage.wal.WalShardRegistry(walChunkService.blobContainer()).register(indexUuid, shardId);
            walShardRegistered.set(true);
        } catch (Exception e) {
            logger.warn("failed to register this shard in the WAL shard registry, will retry next lease-renewal tick", e);
        }
    }

    @Override
    public void close() throws IOException {
        directoryRefreshTask.cancel();
        leaseRenewalTask.cancel();
        if (pitrRetentionTask != null) {
            pitrRetentionTask.close();
        }
        if (dedicatedWalGcSchedulerTask != null) {
            dedicatedWalGcSchedulerTask.close();
        }
        flushAndPublishQuiescentBestEffort();
        super.close();
    }

    /**
     * The "final flush+publication, manifest marked quiescent" nuance rfc-serverless-opensearch.md
     * &sect;7.3 describes for writer scale-to-zero suspension -- called unconditionally from {@link
     * #close()} (not only when the close is actually suspension-triggered) so this engine's very
     * last published manifest is both as fresh as possible (a real forced commit, not whatever
     * happened to be last published on the normal refresh/flush schedule) and marked {@link
     * CommitManifest#quiescent()}, for whatever future consumer eventually reads that flag.
     *
     * <p>Deliberately unconditional rather than suspension-aware: distinguishing "closing because of
     * suspension" from "closing for an ordinary reason" (relocation, node restart, primary-term
     * change) is exactly the coordinator-to-live-engine communication {@code
     * ShardSuspensionCoordinator}'s own javadoc already documents as out of scope for that class
     * (it acts purely through cluster state/allocation, never talking to the node hosting the live
     * engine) -- building that channel is a materially larger, still-open increment. Marking every
     * closing engine's very last manifest quiescent is harmless in the meantime: nothing in this
     * plugin consumes {@link CommitManifest#quiescent()} yet, and for the one case where it matters
     * semantically (ordinary relocation), the new engine that immediately opens on the new node
     * publishes its own non-quiescent manifests right away, which simply supersede the old one --
     * there is no lasting incorrectness, only an unused bit on a manifest that's about to be
     * superseded anyway.
     *
     * <p>Deliberately best-effort: {@link #close()} must still tear this engine down even if the
     * object store is unreachable or this shard has already been fenced out by a newer writer (both
     * real, already-possible outcomes of an ordinary {@link #commitIndexWriter} publish) -- failing
     * to quiesce cleanly is a missed optimization (a future cold-start reads a slightly older
     * manifest than it could have), never a correctness gap, exactly the same distinction {@code
     * ShardSuspensionCoordinator}'s own javadoc already draws for the rest of this feature.
     */
    private void flushAndPublishQuiescentBestEffort() {
        try {
            flushAndPublishQuiescent();
        } catch (Exception e) {
            logger.warn("failed to publish a final quiescent commit before closing, a later reactivation will republish", e);
        }
    }

    /**
     * Forces one real final commit (unconditionally, even if nothing has changed since the last
     * one, so this engine's very last published manifest is guaranteed as fresh as it can possibly
     * be) and marks the resulting manifest {@link CommitManifest#quiescent()}.
     *
     * <p>Threaded through {@link #nextCommitIsQuiescent} rather than a parameter: {@link #flush}
     * only ever leads to {@link #commitIndexWriter} being invoked by core's own {@code
     * InternalEngine} machinery deep inside {@link #flush}, with a fixed signature this class
     * cannot add a "this one is quiescent" parameter to -- the field is set immediately before the
     * forced flush and consumed (cleared) by the very next {@link #commitIndexWriter} call, which
     * {@code force=true} guarantees happens synchronously within this method's own call.
     */
    public void flushAndPublishQuiescent() throws IOException {
        nextCommitIsQuiescent.set(true);
        try {
            flush(true, true);
        } finally {
            nextCommitIsQuiescent.set(false);
        }
    }

    /**
     * Sources a real user/scheduler-driven refresh actually arrives with -- see {@code
     * TransportShardRefreshAction#shardOperationOnPrimary}'s {@code primary.refresh("api")} for the
     * explicit {@code _refresh} API path, and {@code IndexShard#scheduledRefresh}'s {@code
     * getIndexer().maybeRefresh("schedule")} for the {@code index.refresh_interval}-driven periodic
     * one. Every other source string ({@code "recovery_finalization"}, {@code "post_recovery"},
     * {@code "reset_engine"}, {@code "too_many_listeners"}, snapshot/relocation refreshes, etc.) is
     * an internal engine-lifecycle refresh this engine must not react to by publishing -- publishing
     * mid-recovery, before this engine is fully activated, or on every listener-driven internal
     * reopen would be either wrong or simply noisy, not the honest "{@code _refresh} now costs a
     * publish" contract rfc-serverless-opensearch.md &sect;8 actually asks for.
     */
    private static final java.util.Set<String> PUBLICATION_TRIGGERING_REFRESH_SOURCES = java.util.Set.of("api", "schedule");

    /**
     * {@code Engine#refresh}'s contract is a local reader reopen -- always run first via {@code
     * super.refresh(source)}, so this engine's own local searches stay correct regardless of
     * whether the publish attempt below is rate-limited, still running, or fails.
     * rfc-serverless-opensearch.md &sect;8: "{@code _refresh} changes meaning honestly: it becomes
     * 'flush, publish, notify'... the API contract ('changes visible to search after refresh
     * returns') is preserved" -- that contract is about local search visibility, which {@code
     * super.refresh} already guarantees synchronously; the publish itself runs asynchronously (see
     * {@link #maybePublishOnRefresh}) so this call never blocks on it.
     *
     * @param source what triggered this refresh; only {@code "api"} (explicit {@code _refresh}) and
     *               {@code "schedule"} ({@code index.refresh_interval}) are publication-triggering
     */
    @Override
    public void refresh(String source) throws EngineException {
        super.refresh(source);
        maybePublishOnRefresh(source);
    }

    /**
     * Same publication-triggering treatment as {@link #refresh}, for the {@code maybeRefresh}
     * variant {@code IndexShard#scheduledRefresh} actually calls (only reopens if a refresh is
     * genuinely pending, unlike {@link #refresh}'s unconditional reopen).
     *
     * @param source what triggered this refresh; only {@code "api"} and {@code "schedule"} are publication-triggering
     * @return whatever {@code super.maybeRefresh(source)} returned -- this override's added publish
     *         attempt never changes whether a reader-visible refresh actually happened
     */
    @Override
    public boolean maybeRefresh(String source) throws EngineException {
        boolean refreshed = super.maybeRefresh(source);
        maybePublishOnRefresh(source);
        return refreshed;
    }

    /**
     * Dispatches a flush (non-forcing: a no-op if nothing has changed since the last commit, so an
     * idle index's periodic {@code schedule} refreshes never publish an identical manifest), and
     * thereby a publish, onto {@link ThreadPool.Names#GENERIC} -- honoring {@link
     * #publicationRateLimitMillis}, see that field's own javadoc for the rate-limit half.
     *
     * <p><b>Dispatched asynchronously, not run inline on the calling thread.</b> {@code refresh}/
     * {@code maybeRefresh} are invoked by core on {@link ThreadPool.Names#REFRESH} -- a small,
     * node-wide pool shared by every shard on the node, not scoped to serverless-storage ones.
     * Running this method's real object-store network I/O inline there would tie up that shared
     * pool for the duration of every {@code api}/{@code schedule} refresh on every serverless-storage
     * writer shard, starving unrelated shards' refreshes under load -- caught by code review, not
     * by any test, since nothing here throws or returns wrong data, it just contends for a resource
     * this class has no business monopolizing. A failed async publish is logged and left for the
     * next triggering refresh to retry, the same tolerance {@code pollForNewerManifest} already
     * applies on the reader side -- a delayed or dropped publish attempt only delays reader-visible
     * generation advancement, it never loses data (already durable locally via the translog/WAL
     * before this ever runs).
     *
     * <p>{@link #refreshPublicationInFlight} skips dispatching a second publish while one is
     * already running for this engine: a redundant trigger loses nothing since the in-flight
     * attempt's own {@link #flush} call picks up every change made up to when it actually runs,
     * including ones made after this skipped call was triggered -- and it keeps an unbounded burst
     * of rapid refreshes (rate limiting disabled, the default) from flooding {@code GENERIC} with
     * many overlapping flushes for the same shard.
     */
    private void maybePublishOnRefresh(String source) {
        if (PUBLICATION_TRIGGERING_REFRESH_SOURCES.contains(source) == false) {
            return;
        }
        if (publicationRateLimitMillis > 0) {
            long now = engineConfig.getThreadPool().relativeTimeInMillis();
            long last = lastRefreshPublicationAttemptMillis.get();
            if (now - last < publicationRateLimitMillis) {
                return;
            }
            if (lastRefreshPublicationAttemptMillis.compareAndSet(last, now) == false) {
                return;
            }
        }
        if (refreshPublicationInFlight.compareAndSet(false, true) == false) {
            return;
        }
        engineConfig.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                flush(false, false);
            } catch (Exception e) {
                logger.warn("failed to publish on refresh (source [" + source + "]), will retry on the next triggering refresh", e);
            } finally {
                refreshPublicationInFlight.set(false);
            }
        });
    }

    @Override
    protected void commitIndexWriter(final DocumentIndexWriter writer, final String translogUUID) throws IOException {
        // Snapshotted before the commit, not after: currentWalPosition() reads a chunk-sequence
        // watermark this WAL service shares across every shard on the node (see its own javadoc).
        // Concurrent indexing on this shard is not excluded during commitIndexWriter (only a shared
        // read lock is held here, same as ordinary index() calls), so an op whose Lucene write lands
        // just after this commit's segment snapshot but whose WAL mirror write completes before a
        // *post*-commit read of this watermark would otherwise be recorded as "already covered" by
        // this manifest's WalPosition even though it is not reflected in these segments -- silent
        // data loss on replay, and permanent once WAL GC trusts the same inflated position to delete
        // the chunk. A pre-commit snapshot cannot have this problem: this WAL service's own contract
        // (translog.add() only enqueues a record after the same operation's Lucene write has already
        // returned) guarantees every op reflected in this watermark already has its Lucene write
        // applied before the watermark was read, and Lucene's own commit() durability contract
        // guarantees any such already-applied write is included in the very next commit -- so this
        // snapshot is always a safe (if occasionally conservative) lower bound for what this specific
        // commit contains, never an inflated one.
        WalPosition walPositionBeforeCommit = currentWalPosition();
        super.commitIndexWriter(writer, translogUUID);

        boolean quiescent = nextCommitIsQuiescent.getAndSet(false);
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
                walPositionBeforeCommit,
                0,
                PruningStats.empty(),
                quiescent
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
            // Only now, after publishCommitAsHead has actually returned true (not merely
            // uploaded -- see ObjectStoreCommitPublisher's own "never the reverse" invariant), is
            // it safe to let local translog retention advance past these ops.
            translogDeletionPolicy.recordDurablePublication(maxSeqNo);
            if (publicationNotifier != null) {
                // Dispatched, never called inline: commitIndexWriter can run synchronously inside
                // a cluster-state-applier callback (IndicesClusterStateService#updateShard ->
                // IndexShard#flush, on initial shard start/promotion), and
                // WriterPublicationNotifier#notifyReaders calls clusterService.state(), which
                // ClusterApplierService asserts against reentrantly from that exact call stack --
                // caught by ServerlessStoragePublicationNotificationIT before this dispatch existed.
                engineConfig.getThreadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                    try {
                        publicationNotifier.notifyReaders(indexUuid, shardId);
                    } catch (Exception e) {
                        logger.warn("failed to notify readers of new publication for shard [" + indexUuid + "][" + shardId + "]", e);
                    }
                });
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
