/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.reindex.AbstractBulkByScrollRequest;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.BulkByScrollTask;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;

import java.util.List;
import java.util.Map;

/**
 * Update-by-query over the same walk delete-by-query uses: each matched document is updated in place, by the request's
 * script or unchanged when it has none, conditional on the sequence identity the page saw.
 *
 * <p>Plugins use it on their own indices -- the anomaly-detection plugin marks its old tasks as no longer the latest before
 * starting a detector. The script runs as an update's script does (core's update-by-query uses the same script context), so
 * {@code ctx._source} is the document and {@code ctx.op = "noop"} leaves it alone. A document changed after its page was read
 * is a version conflict, counted, and stops the operation unless conflicts are to proceed, as in core.
 */
final class UpdateByQueryExecution {

    private UpdateByQueryExecution() {}

    static BulkByScrollResponse run(ShardOperations operations, UpdateByQueryRequest request) throws java.io.IOException {
        return operations.gated(UpdateByQueryAction.NAME, request, admitted -> {
            if ((admitted instanceof UpdateByQueryRequest) == false) {
                throw new IllegalArgumentException("an update-by-query filter must provide UpdateByQueryRequest");
            }
            final UpdateByQueryRequest accepted = (UpdateByQueryRequest) admitted;
            final var validation = accepted.validate();
            if (validation != null) {
                throw validation;
            }
            if (accepted.indices().length != 1 || accepted.getRouting() != null) {
                throw new UnsupportedRequestOptionException("update-by-query requires one concrete index and no routing");
            }
            final var search = accepted.getSearchRequest();
            final var query = search.source() == null ? null : search.source().query();
            final var supportedSource = new UpdateByQueryRequest(accepted.indices()).setQuery(
                query == null ? org.opensearch.index.query.QueryBuilders.matchAllQuery() : query
            ).getSearchRequest().source();
            if ((search.source() != null && search.source().equals(supportedSource) == false)
                || accepted.getSlices() != 1
                || accepted.getRequestsPerSecond() != Float.POSITIVE_INFINITY
                || accepted.getShouldStoreResult()
                || accepted.getPipeline() != null) {
                throw new UnsupportedRequestOptionException(
                    "update-by-query search options, slicing, throttling, pipelines and stored tasks are not supported"
                );
            }
            final String index = accepted.indices()[0];
            final var script = accepted.getScript();
            final long started = System.nanoTime();
            final DeleteByQueryExecution.Walk walk = operations.deletes.walk(
                index,
                query == null ? org.opensearch.index.query.QueryBuilders.matchAllQuery() : query,
                accepted.getMaxDocs() == AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES ? Long.MAX_VALUE : accepted.getMaxDocs(),
                accepted.isRefresh(),
                accepted.isAbortOnVersionConflict(),
                (id, seqNo, primaryTerm, refresh) -> operations.updates.executeAcceptedUpdate(
                    index,
                    id,
                    null,
                    script == null ? Map.of() : null,
                    null,
                    false,
                    false,
                    refresh,
                    seqNo,
                    primaryTerm,
                    script,
                    false
                ).response().getResult() != org.opensearch.action.DocWriteResponse.Result.NOOP
            );
            final var status = new BulkByScrollTask.Status(
                null,
                walk.visited(),
                walk.changed(),
                0L,
                0L,
                walk.batches(),
                walk.conflicts(),
                walk.noops(),
                0L,
                0L,
                TimeValue.ZERO,
                Float.POSITIVE_INFINITY,
                null,
                TimeValue.ZERO
            );
            return new BulkByScrollResponse(
                TimeValue.timeValueNanos(System.nanoTime() - started),
                status,
                walk.conflict() == null
                    ? List.of()
                    : List.of(new org.opensearch.action.bulk.BulkItemResponse.Failure(index, walk.abortedOn(), walk.conflict())),
                List.of(),
                false
            );
        }, org.opensearch.serverless.shell.ActionGate.identity(BulkByScrollResponse.class));
    }
}
