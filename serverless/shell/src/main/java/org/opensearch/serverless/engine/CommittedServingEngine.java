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

import java.io.IOException;

/**
 * Writable committed-serving primary. Ordinary client operations cannot allocate identities here.
 * The embedding host must verify committed-log authority before applying resolved operations;
 * an assigned sequence number alone is not proof of commitment. This engine is not enabled by the launcher.
 */
@InternalApi
public final class CommittedServingEngine extends InternalEngine {
    /**
     * Configures primary-origin replay with upstream identity and replica planning semantics.
     * @param configuration standard store, mapper and lifecycle configuration
     */
    public CommittedServingEngine(EngineConfig configuration) {
        super(configuration.toBuilder().primaryOperationPolicy(CommittedPrimaryOperationPolicy.INSTANCE).build());
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

    private static void requireResolvedPrimary(Engine.Operation operation) {
        if (operation.origin() == Engine.Operation.Origin.PRIMARY
            && (operation.seqNo() < 0
                || operation.primaryTerm() <= 0
                || (operation instanceof Engine.NoOp == false && operation.version() <= 0))) {
            throw new IllegalStateException("committed serving requires an upstream-assigned sequence number, term and version");
        }
    }
}
