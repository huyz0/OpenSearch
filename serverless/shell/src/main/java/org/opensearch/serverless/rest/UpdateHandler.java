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
import java.util.Map;
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
        final boolean refresh = IndexAdminHandler.refresh(request);
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
        request.param("timeout");
        request.param("wait_for_active_shards");
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
            .retryOnConflict(retryOnConflict);
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
        final Map<String, Object> doc = parsed.doc() == null ? null : parsed.doc().sourceAsMap();
        final Map<String, Object> upsert = parsed.upsertRequest() == null ? null : parsed.upsertRequest().sourceAsMap();
        final long ifSeqNo = parsed.ifSeqNo();
        final long ifPrimaryTerm = parsed.ifPrimaryTerm();
        final ServerlessNode serving = node.get();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            final var operations = new ShardOperations(serving, metadata, true);
            try {
                ShardOperations.UpdateOutcome outcome = null;
                // retry_on_conflict, honoured the way core honours it: an unconditional update is a read
                // followed by a compare-and-swap against what was read, and a lost race is retried from a
                // fresh read up to the count the caller allowed. A caller who passed their own condition
                // gets exactly one attempt, since retrying would be retrying against a token they chose.
                final long attempts = 1L + retryOnConflict;
                for (long attempt = 0; attempt < attempts; attempt++) {
                    try {
                        outcome = operations.update(
                            index,
                            id,
                            doc,
                            upsert,
                            parsed.docAsUpsert(),
                            parsed.detectNoop(),
                            refresh,
                            ifSeqNo,
                            ifPrimaryTerm,
                            parsed.script(),
                            parsed.scriptedUpsert()
                        );
                        break;
                    } catch (org.opensearch.index.engine.VersionConflictEngineException e) {
                        if (attempt == attempts - 1) {
                            throw e;
                        }
                    }
                }
                respond(channel, index, id, outcome);
            } catch (ShardOperations.NoSuchIndexException e) {
                sendError(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
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

    private void respond(org.opensearch.rest.RestChannel channel, String index, String id, ShardOperations.UpdateOutcome outcome)
        throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("_index", index);
            builder.field("_id", id);
            builder.field("result", outcome.result());
            builder.field("_version", outcome.version());
            builder.field("_seq_no", outcome.seqNo());
            builder.field("_primary_term", outcome.primaryTerm());
            builder.startObject("_shards");
            builder.field("total", 1);
            builder.field("successful", 1);
            builder.field("failed", 0);
            builder.endObject();
            builder.field("_node", outcome.servedBy());
            builder.endObject();
            // 201 when the upsert created the document, 200 otherwise -- what core's DocWriteResponse
            // answers, and what a client that branches on the status before reading "result" expects.
            channel.sendResponse(new BytesRestResponse("created".equals(outcome.result()) ? RestStatus.CREATED : RestStatus.OK, builder));
        }
    }
}
