/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.ShardLock;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.storage.utils.DirectoryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * Owns one admitted, disposable private engine and its fresh local generation. No public shard,
 * index service or search endpoint is registered. This empty-history primitive is not a mutation
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

    static PrivatePreparationResources openEmpty(
        PreparationEngineAdmission admission,
        UUID group,
        CommittedServingEngine serving,
        Path generation,
        long residentBytes,
        long indexingBufferBytes,
        NativeOpener opener
    ) throws IOException {
        Objects.requireNonNull(serving);
        Objects.requireNonNull(opener);
        if (indexingBufferBytes <= 0 || indexingBufferBytes > Integer.MAX_VALUE || residentBytes < indexingBufferBytes) {
            throw new IllegalArgumentException("private indexing buffer exceeds resident reservation");
        }
        final var ticket = Objects.requireNonNull(admission).acquire(group, serving.config().getShardId(), residentBytes);
        Path root = null;
        FSDirectory directory = null;
        Store store = null;
        PrivatePreparationEngine engine = null;
        boolean claimed = false;
        boolean nativeInitializationStarted = false;
        try {
            serving.ensureReplayOpen();
            if (serving.getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != SequenceNumbers.NO_OPS_PERFORMED) {
                throw new IOException("nonempty serving history requires verified private-engine rebuild");
            }
            final var publicConfig = serving.config();
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
            store = new Store(
                publicConfig.getShardId(),
                publicConfig.getIndexSettings(),
                directory,
                new PrivateShardLock(publicConfig.getShardId())
            );
            final long term = publicConfig.getPrimaryTermSupplier().getAsLong();
            final var configuration = PrivatePreparationEngineConfig.create(
                publicConfig,
                store,
                translog,
                term,
                indexingBufferBytes,
                () -> SequenceNumbers.NO_OPS_PERFORMED,
                new Engine.EventListener() {
                }
            );
            nativeInitializationStarted = true;
            store.createEmpty(configuration.getIndexSettings().getIndexVersionCreated().luceneVersion);
            store.associateIndexWithNewTranslog(
                Translog.createEmptyTranslog(translog, SequenceNumbers.NO_OPS_PERFORMED, configuration.getShardId(), term)
            );
            engine = Objects.requireNonNull(opener.open(configuration));
            engine.translogManager().recoverFromTranslog(snapshot -> {
                if (snapshot.next() != null) {
                    throw new IOException("fresh private preparation translog unexpectedly contains history");
                }
                return 0;
            }, SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            return new PrivatePreparationResources(ticket, root, store, engine, indexingBufferBytes);
        } catch (IOException | RuntimeException | Error failed) {
            if (nativeInitializationStarted && engine == null) {
                ticket.quarantine(new UncertainResources(root, store, directory, failed));
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
                    if (claimed) {
                        IOUtils.rm(root);
                    }
                    ticket.close();
                } catch (IOException | RuntimeException | Error cleanup) {
                    if (cleanup != failed) {
                        failed.addSuppressed(cleanup);
                    }
                    ticket.quarantine(new UncertainResources(root, store, directory, failed));
                }
            }
            throw failed;
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

    synchronized PrivatePreparationEngine engine() {
        if (closed) {
            throw new IllegalStateException("private preparation resources are closed", closeFailure);
        }
        if (preparationFailure != null) {
            throw new IllegalStateException("private preparation requires rebuild after failure", preparationFailure);
        }
        return engine;
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

    private record UncertainResources(Path generation, Store store, FSDirectory directory, Throwable failure) {
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
