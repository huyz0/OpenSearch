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
 * <p><b>Not a transaction on its own, but it can be made one.</b> The read and the write are two separate
 * calls with nothing holding the document still in between, so an unconditional update silently overwrites
 * a write that lands in that window — the same as two plain writes racing. What has changed is that the
 * caller can now close that window itself: {@code if_seq_no} and {@code if_primary_term} are accepted here
 * and passed to the write, so read-modify-write with a retry loop is the compare-and-swap classic
 * OpenSearch's own {@code _update} performs internally. Unconditional remains the default, and remains
 * best-effort; the response carries the {@code _seq_no} a retry would condition on.
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

    /**
     * Reads the {@code script} field, in either shape a client sends it.
     *
     * <p>A bare string is shorthand for a painless source; an object may name {@code source}, {@code lang},
     * {@code params} or an {@code id}. A stored script is refused by name rather than compiled, because
     * resolving one goes through cluster state this design does not have.
     *
     * @param given the script field's value
     * @return the parsed script
     */
    private static org.opensearch.script.Script parseScript(Object given) {
        if (given instanceof String source) {
            return new org.opensearch.script.Script(source);
        }
        if (given instanceof java.util.Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            final java.util.Map<String, Object> storedParams = map.get("params") instanceof java.util.Map
                ? (java.util.Map<String, Object>) map.get("params")
                : java.util.Map.of();
            if (map.get("id") != null) {
                // A stored script, resolved by the script service from this deployment's own register.
                return new org.opensearch.script.Script(
                    org.opensearch.script.ScriptType.STORED,
                    null,
                    String.valueOf(map.get("id")),
                    storedParams
                );
            }
            final Object source = map.get("source");
            if (source == null) {
                throw new IllegalArgumentException("a script needs a 'source'");
            }
            final String lang = map.get("lang") == null
                ? org.opensearch.script.Script.DEFAULT_SCRIPT_LANG
                : String.valueOf(map.get("lang"));
            @SuppressWarnings("unchecked")
            final java.util.Map<String, Object> params = map.get("params") instanceof java.util.Map
                ? (java.util.Map<String, Object>) map.get("params")
                : java.util.Map.of();
            return new org.opensearch.script.Script(org.opensearch.script.ScriptType.INLINE, lang, String.valueOf(source), params);
        }
        throw new IllegalArgumentException("'script' must be a string or an object");
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
        final long ifSeqNo;
        final long ifPrimaryTerm;
        try {
            ifSeqNo = ifSeqNoParam == null ? org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO : Long.parseLong(ifSeqNoParam);
            ifPrimaryTerm = ifPrimaryTermParam == null ? 0L : Long.parseLong(ifPrimaryTermParam);
        } catch (NumberFormatException e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "invalid_condition",
                    "if_seq_no and if_primary_term must be numbers"
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
        // A scripted update, compiled by the node's own ScriptService under core's UpdateScript context. The
        // refusal that stood here said no scripting engine was registered, which stopped being true the
        // moment one was.
        final org.opensearch.script.Script script;
        if (body.get("script") != null) {
            try {
                script = parseScript(body.get("script"));
            } catch (IllegalArgumentException e) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage())
                );
            }
        } else {
            script = null;
        }
        final boolean scriptedUpsert = Boolean.TRUE.equals(body.get("scripted_upsert"));
        if (script == null && scriptedUpsert) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "scripted_upsert was given without a script"
                )
            );
        }
        final boolean docAsUpsert = Boolean.TRUE.equals(body.get("doc_as_upsert"));
        final Object docField = body.get("doc");
        final Object upsertField = body.get("upsert");
        if (script == null && (docField instanceof Map) == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_doc", "an update body needs a 'doc' object or a 'script'")
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
                ShardOperations.UpdateOutcome outcome = null;
                // retry_on_conflict, honoured the way core honours it: an unconditional update is a read
                // followed by a compare-and-swap against what was read, and a lost race is retried from a
                // fresh read up to the count the caller allowed. A caller who passed their own condition
                // gets exactly one attempt, since retrying would be retrying against a token they chose.
                final int attempts = ifSeqNoParam == null ? 1 + Math.max(0, retryOnConflict) : 1;
                for (int attempt = 0; attempt < attempts; attempt++) {
                    try {
                        outcome = operations.update(
                            index,
                            id,
                            doc,
                            upsert,
                            docAsUpsert,
                            detectNoop,
                            refresh,
                            ifSeqNo,
                            ifPrimaryTerm,
                            script,
                            scriptedUpsert
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
