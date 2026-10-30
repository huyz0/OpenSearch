/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code POST /{index}/_update/{id}} merges or scripts a document, then conditionally writes
 * against the sequence identity it read. Conflicting writes may be retried from a fresh read.
 * Native parsing and validation keep the accepted body consistent with core's update API.
 */
public final class UpdateHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public UpdateHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_update_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/{index}/_update/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter read before any early return, or BaseRestHandler turns a deliberate refusal
        // into a 400 about an unconsumed parameter.
        final String index = request.param("index");
        final String id = request.param("id");
        final var refreshPolicy = org.opensearch.action.support.WriteRequest.RefreshPolicy.parse(request.param("refresh", "false"));
        final String ifSeqNoParam = request.param("if_seq_no");
        final String ifPrimaryTermParam = request.param("if_primary_term");
        final String version = request.param("version");
        final String versionType = request.param("version_type");
        final boolean detectNoopParam = request.paramAsBoolean("detect_noop", true);
        final int retryOnConflict = request.paramAsInt("retry_on_conflict", 0);
        final String routing = request.param("routing");
        final boolean requireAlias = request.paramAsBoolean("require_alias", false);
        final org.opensearch.search.fetch.subphase.FetchSourceContext wantSource = org.opensearch.search.fetch.subphase.FetchSourceContext
            .parseFromRestRequest(request);
        // Hints with one possible answer here; see DocumentHandler.
        final String timeout = request.param("timeout");
        final var activeShards = org.opensearch.action.support.ActiveShardCount.parseString(request.param("wait_for_active_shards"));
        request.param("lang");

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (routing != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "routing is not supported: a document is placed by its id alone"
                )
            );
        }
        if (requireAlias) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "require_alias is not supported: writes here name an index, and an alias resolves on read"
                )
            );
        }
        if (wantSource != null && wantSource.fetchSource()) {
            // Refused rather than silently omitted. Core answers _source on an update with a "get" block
            // carrying the document as written; this path does not carry the written document back from the
            // shard, so accepting the parameter would answer without the one thing it asked for.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "_source on an update is not supported: the written document is not returned. GET /{index}/_doc/{id} "
                        + "reads it back, realtime, from the shard that wrote it"
                )
            );
        }
        if (version != null || versionType != null) {
            // Same refusal as a plain write, for the same reason: see DocumentHandler. External
            // versioning is a model this system does not keep; optimistic concurrency is one it does.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "'version' asks for external versioning, which this system does not have. Use if_seq_no "
                        + "and if_primary_term, which compare against the sequence number this system assigns."
                )
            );
        }
        if ((ifSeqNoParam == null) != (ifPrimaryTermParam == null)) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "invalid_condition",
                    "if_seq_no and if_primary_term must be supplied together"
                )
            );
        }
        final var parsed = new org.opensearch.action.update.UpdateRequest(index, id).detectNoop(detectNoopParam)
            .retryOnConflict(retryOnConflict)
            .setRefreshPolicy(refreshPolicy)
            .waitForActiveShards(activeShards);
        if (timeout != null) {
            parsed.timeout(timeout);
        }
        if (retryOnConflict < 0) {
            throw new IllegalArgumentException("retry_on_conflict must be non negative");
        }
        if (ifSeqNoParam != null) {
            parsed.setIfSeqNo(Long.parseLong(ifSeqNoParam));
            parsed.setIfPrimaryTerm(Long.parseLong(ifPrimaryTermParam));
        }
        if (request.hasContent() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "an update body is required")
            );
        }

        try (XContentParser parser = request.contentOrSourceParamParser()) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("an update body must be an object");
            }
            parsed.fromXContent(parser);
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("unexpected content after the update body");
            }
        }
        final var validation = parsed.validate();
        if (validation != null) {
            throw validation;
        }
        if (parsed.scriptedUpsert() && parsed.script() == null) {
            throw new IllegalArgumentException("scripted_upsert was given without a script");
        }
        if (parsed.fetchSource() != null && parsed.fetchSource().fetchSource()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_write", "_source on an update is not supported")
            );
        }
        final ServerlessNode serving = node.get();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            final var operations = new ShardOperations(serving, metadata, true);
            try {
                respond(channel, operations.update(parsed));
            } catch (ShardOperations.NoSuchIndexException e) {
                sendError(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + e.index());
            } catch (ShardOperations.DocumentMissingException e) {
                sendError(channel, RestStatus.NOT_FOUND, "document_missing_exception", e.getMessage());
            } catch (ShardOperations.SystemIndexException e) {
                sendError(channel, RestStatus.FORBIDDEN, "system_index", e.getMessage());
            } catch (ShardOperations.NotHereException e) {
                // The get endpoint's vocabulary, not an update-shaped approximation of it: an update is a
                // get and a write, and either half can be the one that could not find a home. The status
                // is the exception's own: "nobody owns the shard" is the 421 PUT answers, not a 503 that
                // a client treats as "retry" for a state the code itself calls not retryable.
                sendError(channel, e.restStatus(), e.restType(serving.localNode().getId()), e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an update failure", nested);
                }
            }
        });
    }

    private void sendError(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException nested) {
            logger.error("failed to report an update failure", nested);
        }
    }

    private void respond(org.opensearch.rest.RestChannel channel, ShardOperations.UpdateOutcome outcome) throws IOException {
        final var response = outcome.response();
        try (XContentBuilder builder = channel.newBuilder()) {
            if (outcome.servedBy() == null) {
                response.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            } else {
                builder.startObject();
                response.innerToXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
                builder.field("_node", outcome.servedBy());
                builder.endObject();
            }
            channel.sendResponse(new BytesRestResponse(response.status(), builder));
        }
    }
}
