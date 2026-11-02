/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.index.engine.DeleteVersionMapSnapshot;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writable committed-serving primary. Ordinary client operations cannot allocate identities here.
 * The embedding host must verify committed-log authority before applying resolved operations;
 * an assigned sequence number alone is not proof of commitment. This engine is not enabled by the launcher.
 */
@InternalApi
public final class CommittedServingEngine extends InternalEngine {
    private final AtomicLong assignedTerm;

    private record TermConfiguration(EngineConfig configuration, AtomicLong assignedTerm) {
    }

    /**
     * Configures primary-origin replay with upstream identity and replica planning semantics.
     * @param configuration standard store, mapper and lifecycle configuration
     */
    public CommittedServingEngine(EngineConfig configuration) {
        this(configuration, 0);
    }

    /**
     * Opens with a host-verified operation-term floor, required before recovering higher-term
     * upstream history. This floor is not a verified native recovery base or ownership proof.
     * @param configuration standard native resources and allocation-term supplier
     * @param minimumOperationTerm nonnegative term from verified history; zero uses native allocation
     */
    public CommittedServingEngine(EngineConfig configuration, long minimumOperationTerm) {
        this(termConfiguration(configuration, minimumOperationTerm));
    }

    private CommittedServingEngine(TermConfiguration binding) {
        super(binding.configuration());
        assignedTerm = binding.assignedTerm();
    }

    private static TermConfiguration termConfiguration(EngineConfig configuration, long minimumOperationTerm) {
        Objects.requireNonNull(configuration);
        if (minimumOperationTerm < 0) {
            throw new IllegalArgumentException("invalid upstream operation term floor");
        }
        final var assigned = new AtomicLong(minimumOperationTerm);
        final var nativeTerm = configuration.getPrimaryTermSupplier();
        final var bound = configuration.toBuilder()
            .primaryOperationPolicy(CommittedPrimaryOperationPolicy.INSTANCE)
            .primaryTermSupplier(() -> Math.max(nativeTerm.getAsLong(), assigned.get()))
            .build();
        return new TermConfiguration(bound, assigned);
    }

    /** Caller holds the verified replay/lifecycle exclusion; no pending native mutation is allowed. */
    void installCommittedTerm(long term, long expectedSequence) throws IOException {
        ensureOpen();
        if (term <= 0
            || expectedSequence < SequenceNumbers.NO_OPS_PERFORMED
            || getSeqNoStats(SequenceNumbers.NO_OPS_PERFORMED).getMaxSeqNo() != expectedSequence
            || getProcessedLocalCheckpoint() != expectedSequence) {
            throw new IOException("upstream term requires the exact applied native sequence prefix");
        }
        final long before = config().getPrimaryTermSupplier().getAsLong();
        assignedTerm.accumulateAndGet(term, Math::max);
        try {
            if (config().getPrimaryTermSupplier().getAsLong() > before) {
                translogManager().rollTranslogGeneration();
                translogManager().syncTranslog();
            }
        } catch (IOException | RuntimeException failure) {
            failEngine("failed to install upstream operation term", failure);
            throw failure;
        } catch (Error failure) {
            failEngine("failed to install upstream operation term", new IOException("upstream term installation failed", failure));
            throw failure;
        }
    }

    DeleteVersionMapSnapshot captureDeleteVersions(IndexCommit commit, int maximumEntries, long maximumBytes) throws IOException {
        return snapshotDeleteVersions(commit, maximumEntries, maximumBytes);
    }

    void ensureReplayOpen() {
        ensureOpen();
    }

    @Override
    public Engine.IndexResult index(Engine.Index operation) throws IOException {
        ensureOpen();
        requireResolvedPrimary(operation);
        if (operation.seqNo() >= 0) {
            advanceMaxSeqNoOfUpdatesOrDeletes(operation.seqNo());
        }
        return super.index(operation);
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete operation) throws IOException {
        ensureOpen();
        requireResolvedPrimary(operation);
        if (operation.seqNo() >= 0) {
            advanceMaxSeqNoOfUpdatesOrDeletes(operation.seqNo());
        }
        return super.delete(operation);
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp operation) throws IOException {
        ensureOpen();
        requireResolvedPrimary(operation);
        return super.noOp(operation);
    }

    private void requireResolvedPrimary(Engine.Operation operation) {
        if (operation.primaryTerm() > config().getPrimaryTermSupplier().getAsLong()) {
            throw new IllegalStateException("operation term exceeds the installed upstream term bound");
        }
        if (operation.origin() == Engine.Operation.Origin.PRIMARY
            && (operation.seqNo() < 0
                || operation.primaryTerm() <= 0
                || (operation instanceof Engine.NoOp == false && operation.version() <= 0))) {
            throw new IllegalStateException("committed serving requires an upstream-assigned sequence number, term and version");
        }
    }
}
