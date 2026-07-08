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
import org.opensearch.index.translog.InternalTranslogManager;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.index.translog.listener.CompositeTranslogEventListener;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectoryEntry;
import org.opensearch.serverless.storage.directory.ShardRole;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.retention.PitrRetentionSchedulerTask;
import org.opensearch.serverless.storage.translog.WalMirroringTranslog;
import org.opensearch.serverless.storage.translog.WalMirroringTranslogFactory;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
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
    private final WalChunkService walChunkService;

    /**
     * The fencing snapshot verified sound in {@code plugins/serverless-storage/formal/WalReplayFencing.tla}
     * (its {@code leaseTransferWalPos}): an exclusive upper bound on WAL chunk sequences that
     * existed at the moment this engine activated, captured as early as possible -- before {@code
     * super(engineConfig)} even runs, ahead of any of this engine's own construction work including
     * local translog recovery -- so it is as tight a bound on "chunks that existed before this
     * writer took over" as this architecture can currently produce. Not yet consumed by anything:
     * no WAL-replay recovery mechanism exists yet to use it as a filter bound (see
     * rfc-serverless-opensearch.md &sect;16 Phase 2's "still open" note) -- this field exists so
     * that mechanism, whenever it's built, has the value it needs already captured at the right
     * moment, rather than needing engine-construction-timing changes of its own.
     *
     * <p><b>Honest limitation, not yet closed</b>: this narrows the race the TLA+ model's
     * {@code AcquireLease} action captures atomically with the term change itself, but isn't
     * perfectly equivalent to it -- by the time this engine's constructor runs, core's cluster
     * coordination has already decided this node holds the new term (see &sect;7.1's "term
     * authority bridge": term authority is still borrowed from core cluster coordination today, not
     * yet a metadata-plane CAS event with its own natural place to capture this atomically). A
     * fully atomic capture needs lease acquisition to migrate to the metadata plane, which is
     * separately still-open future work, not something this snapshot alone closes.
     *
     * <p>{@code -1} when WAL mirroring is disabled ({@link #walChunkService} is {@code null}) --
     * there is no WAL chunk stream to bound in that case.
     */
    private final long activationWalPosition;

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

    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, null, null);
    }

    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig
    ) {
        this(engineConfig, headPublisher, shardDirectory, localNodeId, pitrRetentionConfig, null);
    }

    /** @param walChunkService {@code null} disables WAL mirroring entirely, same shape as every other optional feature in this plugin. */
    public ObjectStoreWriterEngine(
        EngineConfig engineConfig,
        ObjectStoreCommitHeadPublisher headPublisher,
        ShardDirectory shardDirectory,
        String localNodeId,
        PitrRetentionConfig pitrRetentionConfig,
        WalChunkService walChunkService
    ) {
        this(
            engineConfig,
            headPublisher,
            shardDirectory,
            localNodeId,
            pitrRetentionConfig,
            walChunkService,
            beginConstruction(walChunkService)
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
        Void ignored
    ) {
        super(engineConfig);
        CONSTRUCTION_WAL_CHUNK_SERVICE.remove();
        this.activationWalPosition = CONSTRUCTION_ACTIVATION_WAL_POSITION.get();
        CONSTRUCTION_ACTIVATION_WAL_POSITION.remove();
        this.headPublisher = headPublisher;
        this.indexUuid = engineConfig.getShardId().getIndex().getUUID();
        this.shardId = engineConfig.getShardId().getId();
        this.shardDirectory = shardDirectory;
        this.localNodeId = localNodeId;
        this.walChunkService = walChunkService;
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

    /**
     * Overrides core's default age/size/total-files translog retention with one driven by
     * object-store durability instead (rfc-serverless-opensearch.md &sect;7.1.1) -- see {@link
     * ObjectStoreDurabilityTranslogDeletionPolicy}'s own javadoc for the full safety argument.
     */
    @Override
    protected TranslogDeletionPolicy getTranslogDeletionPolicy(EngineConfig engineConfig) {
        translogDeletionPolicy = new ObjectStoreDurabilityTranslogDeletionPolicy();
        return translogDeletionPolicy;
    }

    /**
     * Sets {@link #CONSTRUCTION_WAL_CHUNK_SERVICE} and, before anything else about this engine's
     * activation happens, snapshots {@link #activationWalPosition} into {@link
     * #CONSTRUCTION_ACTIVATION_WAL_POSITION} -- capturing it here, ahead of {@code super(...)}, is
     * what makes it as tight a bound as this architecture can currently produce (see {@link
     * #activationWalPosition}'s own javadoc). Returns {@code null} so it can be used as the last
     * argument evaluated before {@code super(...)}.
     */
    private static Void beginConstruction(WalChunkService walChunkService) {
        CONSTRUCTION_WAL_CHUNK_SERVICE.set(walChunkService);
        CONSTRUCTION_ACTIVATION_WAL_POSITION.set(walChunkService == null ? -1L : walChunkService.currentChunkSequenceUpperBound());
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
            new WalMirroringTranslogFactory(configuredWalChunkService),
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

    /** See {@link #activationWalPosition}'s own javadoc -- test-only visibility. */
    long activationWalPositionForTesting() {
        return activationWalPosition;
    }

    /**
     * The manifest's real {@code WalPosition} once WAL mirroring is wired in (&sect;6.4), instead
     * of the {@code (String.valueOf(primaryTerm), 0)} placeholder used before this was tracked at
     * all. When WAL mirroring is disabled ({@link #walMirroringTranslog} is {@code null}), the
     * placeholder shape is kept so every existing caller/test that never configured a {@link
     * WalChunkService} keeps working unchanged -- this manifest simply carries no real WAL
     * coverage information, matching today's behavior exactly.
     */
    private WalPosition currentWalPosition() {
        if (walMirroringTranslog == null) {
            return new WalPosition(String.valueOf(engineConfig.getPrimaryTermSupplier().getAsLong()), 0);
        }
        return new WalPosition(walChunkService.writerEpoch(), walMirroringTranslog.lastFlushedWalChunkSequence());
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
                currentWalPosition(),
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
            // Only now, after publishCommitAsHead has actually returned true (not merely
            // uploaded -- see ObjectStoreCommitPublisher's own "never the reverse" invariant), is
            // it safe to let local translog retention advance past these ops.
            translogDeletionPolicy.recordDurablePublication(maxSeqNo);
        } catch (final EngineException ex) {
            failEngine("object-store commit publication fenced out", ex);
            throw ex;
        } catch (final Exception ex) {
            failEngine("object-store commit publication failed", ex);
            throw new IOException(ex);
        }
    }
}
