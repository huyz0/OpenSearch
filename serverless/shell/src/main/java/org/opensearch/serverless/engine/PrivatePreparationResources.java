/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.engine.DeleteVersionMapSnapshot;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.CompositeDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.store.BlockCacheDirectory;
import org.opensearch.storage.utils.DirectoryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * Owns one admitted, disposable private engine and its fresh local generation. No public shard,
 * index service or search endpoint is registered. This resource primitive is not a mutation
 * sequencer, replay/rebuild controller, disk governor or proof of committed ownership.
 */
@InternalApi
public final class PrivatePreparationResources implements Closeable {
    @FunctionalInterface
    interface NativeOpener {
        PrivatePreparationEngine open(EngineConfig configuration) throws IOException;
    }

    private final PreparationEngineAdmission.Ticket ticket;
    private final Path generation;
    private final Store store;
    private final PrivatePreparationEngine engine;
    private final long maximumBufferBytes;
    private Throwable preparationFailure;
    private boolean closed;
    private Throwable closeFailure;

    private PrivatePreparationResources(
        PreparationEngineAdmission.Ticket ticket,
        Path generation,
        Store store,
        PrivatePreparationEngine engine,
        long maximumBufferBytes
    ) {
        this.ticket = ticket;
        this.generation = generation;
        this.store = store;
        this.engine = engine;
        this.maximumBufferBytes = maximumBufferBytes;
    }

    /**
     * Opens a fresh native context after acquiring resident count/byte admission.
     * The trusted host must independently establish empty committed history and current ownership.
     * @param admission host-owned resident budget, retained while quarantined resources may live
     * @param group assigned physical group
     * @param serving open empty serving engine supplying schema/analysis context
     * @param generation existing empty directory owned by this operation; deleted on successful close
     * @param residentBytes declared resident reservation, including qualified engine/mapping overhead
     * @param indexingBufferBytes native buffer ceiling including writer, version map and in-flight writing estimates
     * @return resource owner; private native access is restricted to the engine adapter package
     * @throws IOException if paths, empty-history checks or native initialization fail
     */
    public static PrivatePreparationResources openEmpty(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long residentBytes,
        long indexingBufferBytes
    ) throws IOException {
        return openEmpty(admission, group, serving, generation, residentBytes, indexingBufferBytes, PrivatePreparationEngine::new);
    }

    /**
     * Limits charged by the host before a process-local committed-history rebuild.
     * @param maximumFiles retained source file count
     * @param maximumMetadataBytes retained source filename/overlay metadata reservation
     * @param maximumPinnedBytes source disk reservation held by the immutable commit pin
     * @param maximumDeleteEntries recent delete identity count
     * @param maximumDeleteBytes reservation for each temporary/native copy of recent delete state
     */
    public record RebuildLimits(int maximumFiles, long maximumMetadataBytes, long maximumPinnedBytes, int maximumDeleteEntries,
        long maximumDeleteBytes) {
        /**
         * Validates positive snapshot bounds.
         * @param maximumFiles retained source file count
         * @param maximumMetadataBytes retained filename/overlay reservation
         * @param maximumPinnedBytes pinned source disk reservation
         * @param maximumDeleteEntries recent delete identity count
         * @param maximumDeleteBytes reservation for each copy of recent delete state
         */
        public RebuildLimits {
            new PreparationOverlayDirectory.Limits(maximumFiles, maximumMetadataBytes, maximumPinnedBytes);
            if (maximumDeleteEntries <= 0 || maximumDeleteBytes < 128) {
                throw new IllegalArgumentException("invalid recent-delete rebuild bounds");
            }
        }
    }

    /**
     * Clones a quiescent committed serving history without reindexing old sources under a newer
     * mapping. The trusted host must hold the ownership/schema/lifecycle and mutation exclusion
     * permits throughout this call, and prove the expected sequence from its applied logical prefix.
     * The clone shares only pinned immutable commit files, retains original recent-delete expiry,
     * and owns a fresh private translog. No source pin survives process loss; this is not archival.
     * Snapshot metadata and both temporary/native delete-state copies are charged to residentBytes.
     * The host separately reserves maximumPinnedBytes in its SSD governor before calling.
     * Currently supports local source storage; remote GC needs an independent immutable-file lease.
     * @param admission host-owned resident admission
     * @param group assigned physical journal group
     * @param serving quiescent serving engine containing only the verified accepted prefix
     * @param generation existing empty separately owned private directory
     * @param residentBytes total resident reservation including snapshot copies and native overhead
     * @param indexingBufferBytes native writer/version-map buffer ceiling
     * @param expectedSequenceNumber exact contiguous accepted native sequence, or -1 for empty
     * @param primaryTerm fixed accepted preparation term, no smaller than the native serving term
     * @param limits host-admitted snapshot metadata, pinned-disk and recent-delete limits
     * @return private resource owner retaining its source pin until cleanup completes
     * @throws IOException if history, local paths, native initialization or verification fail
     */
    public static PrivatePreparationResources openCommitted(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long residentBytes,
        long indexingBufferBytes,
        long expectedSequenceNumber,
        long primaryTerm,
        RebuildLimits limits
    ) throws IOException {
        return openCommitted(
            admission,
            group,
            serving,
            generation,
            residentBytes,
            indexingBufferBytes,
            expectedSequenceNumber,
            primaryTerm,
            limits,
            PrivatePreparationEngine::new
        );
    }

    static PrivatePreparationResources openCommitted(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long residentBytes,
        long indexingBufferBytes,
        long expectedSequenceNumber,
        long primaryTerm,
        RebuildLimits limits,
        NativeOpener opener
    ) throws IOException {
        Objects.requireNonNull(limits);
        if (residentBytes <= 0
            || indexingBufferBytes <= 0
            || indexingBufferBytes > Integer.MAX_VALUE
            || indexingBufferBytes > residentBytes
            || expectedSequenceNumber < SequenceNumbers.NO_OPS_PERFORMED
            || primaryTerm <= 0
            || limits.maximumMetadataBytes() > residentBytes - indexingBufferBytes
            || limits.maximumDeleteBytes() > (residentBytes - indexingBufferBytes - limits.maximumMetadataBytes()) / 2) {
            throw new IllegalArgumentException("committed rebuild exceeds its identity or resident reservation");
        }
        return open(
            admission,
            group,
            serving,
            generation,
            residentBytes,
            indexingBufferBytes,
            opener,
            new Rebuild(expectedSequenceNumber, primaryTerm, limits)
        );
    }

    private record Rebuild(long checkpoint, long term, RebuildLimits limits) {
    }

    static PrivatePreparationResources openEmpty(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long residentBytes,
        long indexingBufferBytes,
        NativeOpener opener
    ) throws IOException {
        return open(admission, group, serving, generation, residentBytes, indexingBufferBytes, opener, null);
    }

    private static PrivatePreparationResources open(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long residentBytes,
        long indexingBufferBytes,
        NativeOpener opener,
        Rebuild rebuild
    ) throws IOException {
        Objects.requireNonNull(serving);
        Objects.requireNonNull(opener);
        if (indexingBufferBytes <= 0 || indexingBufferBytes > Integer.MAX_VALUE || residentBytes < indexingBufferBytes) {
            throw new IllegalArgumentException("private indexing buffer exceeds resident reservation");
        }
        final var ticket = Objects.requireNonNull(admission).acquire(group, serving.config().getShardId(), residentBytes);
        Path root = null;
        Directory directory = null;
        Closeable sourcePin = null;
        Store store = null;
        PrivatePreparationEngine engine = null;
        boolean claimed = false;
        boolean nativeInitializationStarted = false;
        try {
            serving.ensureReplayOpen();
            if (rebuild == null
                && serving.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != SequenceNumbers.NO_OPS_PERFORMED) {
                throw new IOException("nonempty serving history requires verified private-engine rebuild");
            }
            final var publicConfig = serving.config();
            final var mapper = publicConfig.getMapperService();
            final var documentMapper = mapper == null ? null : mapper.documentMapper();
            final boolean gcDeletesEnabled = publicConfig.isEnableGcDeletes();
            final long servingTerm = publicConfig.getPrimaryTermSupplier().getAsLong();
            final long mappingVersion = publicConfig.getIndexSettings().getIndexMetadata().getMappingVersion();
            if (rebuild != null) {
                requireLocalRebuildSource(publicConfig.getStore().directory());
                requireRebuildPrefix(serving, rebuild);
                if (rebuild.term() < servingTerm) {
                    throw new IOException("private rebuild term precedes serving term");
                }
            }
            root = Objects.requireNonNull(generation).toRealPath();
            PrivatePreparationEngineConfig.requireSeparate(
                root,
                DirectoryUtils.unwrapFSDirectory(publicConfig.getStore().directory()).getDirectory().toRealPath()
            );
            PrivatePreparationEngineConfig.requireSeparate(root, publicConfig.getTranslogConfig().getTranslogPath().toRealPath());
            try (var contents = Files.list(root)) {
                if (contents.findAny().isPresent()) {
                    throw new IOException("private preparation generation must be empty");
                }
            }
            // Atomic namespace claim. Never remove it while native initialization/cleanup is uncertain.
            Files.createFile(root.resolve("preparation.claim"));
            claimed = true;
            final Path index = Files.createDirectory(root.resolve("index"));
            final Path translog = Files.createDirectory(root.resolve("translog"));
            directory = FSDirectory.open(index);
            DeleteVersionMapSnapshot deleteVersions = null;
            if (rebuild != null) {
                final var publicStore = publicConfig.getStore();
                publicStore.incRef();
                final GatedCloseable<IndexCommit> commit;
                try {
                    commit = serving.acquireLastIndexCommit(true);
                } catch (RuntimeException | Error failed) {
                    publicStore.decRef();
                    throw failed;
                }
                sourcePin = () -> {
                    try {
                        commit.close();
                    } finally {
                        publicStore.decRef();
                    }
                };
                requireRebuildPrefix(serving, rebuild);
                final var limits = rebuild.limits();
                deleteVersions = serving.captureDeleteVersions(commit.get(), limits.maximumDeleteEntries(), limits.maximumDeleteBytes());
                directory = new PreparationOverlayDirectory(
                    directory,
                    commit.get(),
                    sourcePin,
                    new PreparationOverlayDirectory.Limits(
                        limits.maximumFiles(),
                        limits.maximumMetadataBytes(),
                        limits.maximumPinnedBytes()
                    )
                );
                sourcePin = null; // The overlay now owns the commit AND source-store reference.
            }
            store = new Store(
                publicConfig.getShardId(),
                publicConfig.getIndexSettings(),
                directory,
                new PrivateShardLock(publicConfig.getShardId())
            );
            final long term = rebuild == null ? publicConfig.getPrimaryTermSupplier().getAsLong() : rebuild.term();
            final long checkpoint = rebuild == null ? SequenceNumbers.NO_OPS_PERFORMED : rebuild.checkpoint();
            final var configuration = PrivatePreparationEngineConfig.create(
                publicConfig,
                store,
                translog,
                term,
                indexingBufferBytes,
                () -> checkpoint,
                new Engine.EventListener() {
                }
            );
            nativeInitializationStarted = true;
            if (rebuild == null) {
                store.createEmpty(configuration.getIndexSettings().getIndexVersionCreated().luceneVersion);
            }
            store.associateIndexWithNewTranslog(Translog.createEmptyTranslog(translog, checkpoint, configuration.getShardId(), term));
            engine = Objects.requireNonNull(opener.open(configuration));
            engine.translogManager().recoverFromTranslog(snapshot -> {
                if (snapshot.next() != null) {
                    throw new IOException("fresh private preparation translog unexpectedly contains history");
                }
                return 0;
            }, checkpoint, Long.MAX_VALUE);
            if (rebuild != null) {
                engine.installDeleteVersions(Objects.requireNonNull(deleteVersions));
                requireRebuildPrefix(serving, rebuild);
                if ((mapper != null && mapper.documentMapper() != documentMapper)
                    || publicConfig.getIndexSettings().getIndexMetadata().getMappingVersion() != mappingVersion
                    || publicConfig.isEnableGcDeletes() != gcDeletesEnabled
                    || publicConfig.getPrimaryTermSupplier().getAsLong() != servingTerm) {
                    throw new IOException("serving schema changed during private rebuild");
                }
            }
            final var resources = new PrivatePreparationResources(ticket, root, store, engine, indexingBufferBytes);
            resources.reclaimBuffers(engine);
            return resources;
        } catch (IOException | RuntimeException | Error failed) {
            if (nativeInitializationStarted && engine == null) {
                ticket.quarantine(new UncertainResources(root, store, directory, sourcePin, engine, failed));
            } else {
                try {
                    if (engine != null) {
                        engine.close();
                    }
                    if (store != null) {
                        store.close();
                    } else if (directory != null) {
                        directory.close();
                    }
                    if (sourcePin != null) {
                        sourcePin.close();
                    }
                    if (claimed) {
                        IOUtils.rm(root);
                    }
                    ticket.close();
                } catch (IOException | RuntimeException | Error cleanup) {
                    if (cleanup != failed) {
                        failed.addSuppressed(cleanup);
                    }
                    ticket.quarantine(new UncertainResources(root, store, directory, sourcePin, engine, failed));
                }
            }
            throw failed;
        }
    }

    private static void requireLocalRebuildSource(Directory directory) throws IOException {
        Directory current = directory;
        while (current instanceof FilterDirectory filter) {
            if (current instanceof BlockCacheDirectory
                || current instanceof CompositeDirectory
                || current instanceof RemoteSegmentStoreDirectory) {
                throw new IOException("remote preparation rebuild requires an immutable source-file lease");
            }
            current = filter.getDelegate();
        }
        if (current instanceof FSDirectory == false) {
            throw new IOException("preparation rebuild requires a pinned local filesystem source");
        }
    }

    private static void requireRebuildPrefix(CommittedServingEngine serving, Rebuild rebuild) throws IOException {
        serving.ensureReplayOpen();
        if (serving.getProcessedLocalCheckpoint() != rebuild.checkpoint()
            || serving.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != rebuild.checkpoint()) {
            throw new IOException("committed rebuild requires the exact applied contiguous serving prefix");
        }
    }

    /**
     * Apply a parsed, unassigned primary operation to the private engine. Like IndexShard's
     * primary path, a required dynamic mapping must be resolved before native indexing assigns
     * a sequence number or changes the private history. The host must hold the ownership/schema
     * and lifecycle permits; these checks establish neither a committed schema nor ownership.
     */
    Engine.IndexResult index(Engine.Index operation) throws IOException {
        final var target = engine();
        requireUnassignedPrimary(target, operation);
        final var mapping = operation.parsedDoc().dynamicMappingsUpdate();
        return mapping == null ? (Engine.IndexResult) apply(target, operation) : new Engine.IndexResult(mapping);
    }

    Engine.DeleteResult delete(Engine.Delete operation) throws IOException {
        final var target = engine();
        requireUnassignedPrimary(target, operation);
        return (Engine.DeleteResult) apply(target, operation);
    }

    private Engine.Result apply(PrivatePreparationEngine target, Engine.Operation operation) throws IOException {
        try {
            reclaimBuffers(target);
            final Engine.Result result = operation instanceof Engine.Index index
                ? target.index(index)
                : target.delete((Engine.Delete) operation);
            reclaimBuffers(target);
            return result;
        } catch (IOException | RuntimeException | Error failed) {
            synchronized (this) {
                if (preparationFailure == null) {
                    preparationFailure = failed;
                }
            }
            throw failed;
        }
    }

    private void reclaimBuffers(PrivatePreparationEngine target) throws IOException {
        if (target.buffers().totalBytes() > maximumBufferBytes) {
            // Internal refresh spills native buffers to SSD without publishing a serving reader.
            target.writeIndexingBuffer();
            final long retained = target.buffers().totalBytes();
            if (retained > maximumBufferBytes) {
                // Recent delete versions must not be forgotten to make room: that changes CAS/
                // external-version semantics. This context needs verified rebuild or retirement.
                throw new IOException(
                    "private preparation buffers remain above limit after spilling: " + retained + " > " + maximumBufferBytes
                );
            }
        }
    }

    private static void requireUnassignedPrimary(PrivatePreparationEngine target, Engine.Operation operation) {
        Objects.requireNonNull(operation);
        if (operation.origin() != Engine.Operation.Origin.PRIMARY
            || operation.seqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
            || operation.primaryTerm() != target.config().getPrimaryTermSupplier().getAsLong()) {
            throw new IllegalArgumentException("private preparation requires an unassigned primary operation in its fixed term");
        }
    }

    UUID group() {
        return ticket.group();
    }

    synchronized PrivatePreparationEngine engine() {
        if (closed) {
            throw new IllegalStateException("private preparation resources are closed", closeFailure);
        }
        if (preparationFailure != null) {
            throw new IllegalStateException("private preparation requires rebuild after failure", preparationFailure);
        }
        return engine;
    }

    /** Retains native ownership after an uncertain lifecycle handoff without running cleanup inline. */
    synchronized void quarantineLifecycleFailure(Throwable failure) {
        Objects.requireNonNull(failure);
        if (closed && closeFailure == null) {
            return;
        }
        if (preparationFailure == null) {
            preparationFailure = failure;
        }
        ticket.quarantine(this);
    }

    /**
     * Closes native IO before its store, removes the owned generation, then releases admission.
     * Any uncertain cleanup retains the generation and reservation; the host must keep the admission
     * owner alive and prevent dependent mapper/index-service shutdown until process-level recovery.
     * @throws IOException if cleanup is incomplete
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            if (closeFailure != null) {
                throw new IOException("private preparation cleanup remains quarantined", closeFailure);
            }
            return;
        }
        closed = true;
        try {
            engine.close();
            store.close();
            IOUtils.rm(generation);
            ticket.close();
        } catch (IOException | RuntimeException | Error failed) {
            closeFailure = failed;
            ticket.quarantine(this);
            throw failed;
        }
    }

    private record UncertainResources(Path generation, Store store, Directory directory, Closeable sourcePin,
        PrivatePreparationEngine engine, Throwable failure) {
    }

    // The public shard owns NodeEnvironment's shard lock. The private generation instead has its
    // atomic claim and the native IndexWriter lock; closing it must never release the public lock.
    private static final class PrivateShardLock extends ShardLock {
        private PrivateShardLock(ShardId shard) {
            super(shard);
        }

        @Override
        protected void closeInternal() {}
    }
}
