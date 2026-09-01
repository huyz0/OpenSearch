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
 * {@code POST /{index}/_update/{id}} — a partial-document merge, read then written back.
 *
 * <p><b>Not a transaction.</b> The read and the write are two separate calls with nothing holding the
 * document still in between: a write that lands in that window is silently overwritten by this one, the
 * same as two plain writes racing each other would be. Classic OpenSearch's {@code _update} avoids that by
 * retrying under {@code if_seq_no}/{@code if_primary_term} — exactly the version model {@code WalRecord}
 * does not have (see {@link DocumentHandler}, which refuses those parameters outright). This is the honest
 * version of the feature without one: best-effort, not compare-and-swap.
 *
 * <p><b>{@code script} and {@code scripted_upsert} are refused</b>, for the same reason scripting is refused
 * everywhere else on this surface — no engine is registered, and a plugin gets core's own "no lang
 * registered" rather than this shell approximating one. {@code doc}, {@code upsert}, {@code doc_as_upsert}
 * and {@code detect_noop} are the whole of what this endpoint understands.
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
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter read before any early return, or BaseRestHandler turns a deliberate refusal
        // into a 400 about an unconsumed parameter.
        final String index = request.param("index");
        final String id = request.param("id");
        final boolean refresh = request.paramAsBoolean("refresh", false);
        final String ifSeqNo = request.param("if_seq_no");
        final String ifPrimaryTerm = request.param("if_primary_term");
        final String version = request.param("version");
        final boolean detectNoopParam = request.paramAsBoolean("detect_noop", true);

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (ifSeqNo != null || ifPrimaryTerm != null || version != null) {
            // Same refusal as a plain write, for the same reason: see DocumentHandler.
            final String named = ifSeqNo != null ? "if_seq_no" : ifPrimaryTerm != null ? "if_primary_term" : "version";
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "'"
                        + named
                        + "' asks for a conditional write, which this system does not have: every write is a plain "
                        + "overwrite and every delete a plain removal. WalRecord records document state, not history."
                )
            );
        }
        if (request.hasContent() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "an update body is required")
            );
        }

        final Map<String, Object> body;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            body = parser.map();
        }
        if (body.get("script") != null || body.get("scripted_upsert") != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "scripted updates are not supported: no scripting engine is registered, the same as every other "
                        + "script surface on this node"
                )
            );
        }
        final boolean docAsUpsert = Boolean.TRUE.equals(body.get("doc_as_upsert"));
        final Object docField = body.get("doc");
        final Object upsertField = body.get("upsert");
        if ((docField instanceof Map) == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_doc", "an update body needs a 'doc' object")
            );
        }
        if (upsertField != null && (upsertField instanceof Map) == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_upsert", "'upsert' must be an object")
            );
        }
        final Map<String, Object> doc = (Map<String, Object>) docField;
        final Map<String, Object> upsert = (Map<String, Object>) upsertField;
        // detect_noop defaults true classically, and a request body can override the query param -- the
        // query param exists mainly so a canary or a curious operator can flip it without editing JSON.
        final boolean detectNoop = body.get("detect_noop") instanceof Boolean value ? value : detectNoopParam;

        final ServerlessNode serving = node.get();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            final var operations = new ShardOperations(serving, metadata);
            try {
                final var outcome = operations.update(index, id, doc, upsert, docAsUpsert, detectNoop, refresh);
                respond(channel, index, id, outcome);
            } catch (ShardOperations.NoSuchIndexException e) {
                sendError(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
            } catch (ShardOperations.DocumentMissingException e) {
                sendError(channel, RestStatus.NOT_FOUND, "document_missing_exception", e.getMessage());
            } catch (ShardOperations.NotHereException e) {
                // The get endpoint's vocabulary, not an update-shaped approximation of it: an update is a
                // get and a write, and either half can be the one that could not find a home.
                sendError(
                    channel,
                    RestStatus.SERVICE_UNAVAILABLE,
                    e.owner() != null && e.owner().equals(serving.localNode().getId())
                        ? "activation_in_progress"
                        : (e.getMessage().contains("could not forward") ? "forward_failed" : "owner_unreachable"),
                    e.getMessage()
                );
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
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
            builder.field("_node", outcome.servedBy());
            builder.endObject();
            // 200 for all three results, matching classic OpenSearch: created, updated and noop are all a
            // successfully-answered request, and the "result" field is where the distinction lives.
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
