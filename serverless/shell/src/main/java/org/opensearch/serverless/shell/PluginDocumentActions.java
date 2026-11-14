/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.serverless.shard.NoSuchIndexException;
import org.opensearch.serverless.shard.NotHereException;
import org.opensearch.serverless.shard.ShardOperations;

/**
 * Multi-get, bulk and update for plugins, built from the single-document operations the client already runs.
 *
 * <p><b>Why plugins need them.</b> They are how plugins read and write their own state. The security plugin
 * loads its whole configuration with one multi-get and writes its defaults with a bulk; index management
 * updates a managed index's metadata document in place. The client refused all three by name.
 *
 * <p><b>Why item by item.</b> Each item runs through {@link ShardOperations} exactly as a single request from
 * the same plugin would: the same routing, forwarding, activation of a shard nobody holds, conditional
 * checks and action filters. Batching across items is a throughput optimisation for user traffic, which
 * goes through the REST bulk path; a plugin's bulk is a handful of documents, and running it on the path
 * that is already correct for one document is the choice with nothing new to get wrong. A failed item is
 * reported in its slot and does not fail the others, which is the contract both responses define.
 *
 * <p>A bulk's refresh policy applies to each item, so a plugin that asked for its writes to be visible on
 * return gets that for every document it wrote.
 */
final class PluginDocumentActions {

    private PluginDocumentActions() {}

    static MultiGetResponse multiGet(MultiGetRequest request, ShardOperations operations) {
        final MultiGetItemResponse[] responses = new MultiGetItemResponse[request.getItems().size()];
        for (int i = 0; i < responses.length; i++) {
            final MultiGetRequest.Item item = request.getItems().get(i);
            try {
                final GetRequest get = new GetRequest(item.index(), item.id()).routing(item.routing())
                    .storedFields(item.storedFields())
                    .version(item.version())
                    .versionType(item.versionType())
                    .fetchSourceContext(item.fetchSourceContext())
                    .preference(request.preference())
                    .realtime(request.realtime())
                    .refresh(request.refresh());
                responses[i] = new MultiGetItemResponse(operations.get(get), null);
            } catch (Exception e) {
                responses[i] = new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.id(), translate(e)));
            }
        }
        return new MultiGetResponse(responses);
    }

    static BulkResponse bulk(BulkRequest request, ShardOperations operations) {
        final long started = System.nanoTime();
        final BulkItemResponse[] responses = new BulkItemResponse[request.requests().size()];
        for (int i = 0; i < responses.length; i++) {
            final DocWriteRequest<?> item = request.requests().get(i);
            try {
                final ActionResponse response = switch (item.opType()) {
                    case INDEX, CREATE -> operations.index(withRefresh((IndexRequest) item, request));
                    case DELETE -> operations.delete(withRefresh((DeleteRequest) item, request));
                    case UPDATE -> update(withRefresh((UpdateRequest) item, request), operations);
                };
                responses[i] = new BulkItemResponse(i, item.opType(), (org.opensearch.action.DocWriteResponse) response);
            } catch (Exception e) {
                responses[i] = new BulkItemResponse(i, item.opType(), new BulkItemResponse.Failure(item.index(), item.id(), translate(e)));
            }
        }
        return new BulkResponse(responses, java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started));
    }

    static UpdateResponse update(UpdateRequest request, ShardOperations operations) throws Exception {
        return operations.update(request).response();
    }

    /** A plugin's delete-by-query on one index, through the same frozen walk a REST caller's takes. */
    static org.opensearch.index.reindex.BulkByScrollResponse deleteByQuery(
        org.opensearch.index.reindex.DeleteByQueryRequest request,
        ShardOperations operations
    ) throws Exception {
        if (request.indices().length != 1) {
            throw new UnsupportedOperationException("delete-by-query requires one concrete index");
        }
        final var source = request.getSearchRequest().source();
        return operations.deleteByQuery(
            request.indices()[0],
            source == null || source.query() == null ? org.opensearch.index.query.QueryBuilders.matchAllQuery() : source.query(),
            request.getMaxDocs() == org.opensearch.index.reindex.AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES
                ? Long.MAX_VALUE
                : request.getMaxDocs(),
            request.isRefresh(),
            request.isAbortOnVersionConflict()
        ).response();
    }

    private static <R extends WriteRequest<R>> R withRefresh(R item, BulkRequest bulk) {
        if (bulk.getRefreshPolicy() != WriteRequest.RefreshPolicy.NONE) {
            item.setRefreshPolicy(bulk.getRefreshPolicy());
        }
        return item;
    }

    /** The shell's own failures as the core exceptions a plugin catches, as the client translates them. */
    static Exception translate(Exception e) {
        if (e instanceof NoSuchIndexException missing) {
            return new org.opensearch.index.IndexNotFoundException(missing.index());
        }
        if (e instanceof NotHereException notHere) {
            return new org.opensearch.action.NoShardAvailableActionException(null, notHere.getMessage(), notHere);
        }
        return e;
    }
}
