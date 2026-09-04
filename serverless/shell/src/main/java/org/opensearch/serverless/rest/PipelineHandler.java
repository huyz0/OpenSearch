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
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.TemplateStore;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code _ingest/pipeline/{id}} — storing a pipeline, and refusing one that could not run.
 *
 * <p><b>A pipeline is compiled when it is stored.</b> That is the whole point of doing the work here rather
 * than at write time: a pipeline naming a processor this deployment does not have, or configuring one wrongly,
 * is refused at {@code PUT}, where the operator is standing and can fix it. Accepting it and failing on the
 * first write that referenced it would surface the mistake days later, in somebody else's request, with
 * nothing to connect it to the change that caused it.
 *
 * <p>The refusal names what is available rather than only what is missing, because "unknown processor
 * [foo]" without a list sends an operator to the documentation of a different product.
 */
public final class PipelineHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public PipelineHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_ingest_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_ingest/pipeline/{id}"),
            new Route(RestRequest.Method.POST, "/_ingest/pipeline/{id}"),
            new Route(RestRequest.Method.GET, "/_ingest/pipeline/{id}"),
            new Route(RestRequest.Method.DELETE, "/_ingest/pipeline/{id}"),
            new Route(RestRequest.Method.GET, "/_ingest/pipeline"),
            // Simulation: run a pipeline over documents without writing them. Both AWS and Elastic ship
            // it, and it needs nothing a stored pipeline does not already have -- the same compiler, run
            // over the documents in the body instead of over a write.
            new Route(RestRequest.Method.POST, "/_ingest/pipeline/_simulate"),
            new Route(RestRequest.Method.GET, "/_ingest/pipeline/_simulate"),
            new Route(RestRequest.Method.POST, "/_ingest/pipeline/{id}/_simulate"),
            new Route(RestRequest.Method.GET, "/_ingest/pipeline/{id}/_simulate")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        final String body = request.hasContent() ? request.content().utf8ToString() : null;
        final RestRequest.Method method = request.method();
        final boolean simulate = request.path().endsWith("/_simulate");
        final boolean verbose = request.paramAsBoolean("verbose", false);
        // Hints: there is no cluster manager to time out against.
        request.param("master_timeout");
        request.param("cluster_manager_timeout");
        request.param("timeout");

        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if ((method == RestRequest.Method.PUT || method == RestRequest.Method.POST) && (body == null || body.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a pipeline body is required")
            );
        }
        if (simulate && verbose) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_parameter",
                    "verbose is not supported: it reports each processor's intermediate document, which this does "
                        + "not collect; the final document is reported"
                )
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final TemplateStore store = metadata.pipelines();
                if (simulate) {
                    IndexAdminHandler.gate(
                        serving,
                        org.opensearch.action.ingest.SimulatePipelineAction.NAME,
                        new org.opensearch.action.ingest.SimulatePipelineRequest(
                            new org.opensearch.core.common.bytes.BytesArray(body == null ? "{}" : body),
                            org.opensearch.common.xcontent.XContentType.JSON
                        ),
                        () -> {
                            simulate(channel, serving, store, id, body);
                            return null;
                        }
                    );
                    return;
                }
                final String action = switch (method) {
                    case PUT, POST -> org.opensearch.action.ingest.PutPipelineAction.NAME;
                    case DELETE -> org.opensearch.action.ingest.DeletePipelineAction.NAME;
                    default -> org.opensearch.action.ingest.GetPipelineAction.NAME;
                };
                IndexAdminHandler.gate(
                    serving,
                    action,
                    new org.opensearch.action.ingest.GetPipelineRequest(id == null ? new String[0] : new String[] { id }),
                    () -> {
                        switch (method) {
                            case PUT, POST -> put(channel, serving, store, id, body);
                            case GET -> get(channel, store, id);
                            case DELETE -> delete(channel, store, id);
                            default -> channel.sendResponse(
                                IndexAdminHandler.error(
                                    channel,
                                    RestStatus.METHOD_NOT_ALLOWED,
                                    "method_not_allowed",
                                    method + " is not supported here"
                                )
                            );
                        }
                        return null;
                    }
                );
            } catch (TemplateStore.TooManyTemplatesException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_pipelines", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a pipeline failure", nested);
                }
            }
        });
    }

    private void put(
        org.opensearch.rest.RestChannel channel,
        org.opensearch.serverless.shell.ServerlessNode serving,
        TemplateStore store,
        String id,
        String body
    ) throws IOException {
        try {
            // Compiled, not merely parsed. This is the check that makes storing a pipeline mean something.
            serving.ingestPipelines().compile(id, body);
        } catch (Exception e) {
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "invalid_pipeline",
                (e.getMessage() == null ? e.toString() : e.getMessage())
                    + ". Available processors: "
                    + String.join(", ", serving.ingestPipelines().available())
            );
            return;
        }
        store.put(id, body);
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Runs a pipeline over the documents in the body, and reports each as it would have been written.
     *
     * <p>Core's simulate shape: {@code docs[]} of {@code {doc: {_index, _id, _source, _ingest}}}, an
     * {@code error} object for a document a processor failed on, and an empty object for one the pipeline
     * dropped. The pipeline is the stored one when the path names an id, or the one in the body.
     */
    private void simulate(
        org.opensearch.rest.RestChannel channel,
        org.opensearch.serverless.shell.ServerlessNode serving,
        TemplateStore store,
        String id,
        String body
    ) throws IOException {
        if (body == null || body.isBlank()) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "missing_body", "a simulate body is required, naming docs and a pipeline");
            return;
        }
        final Map<String, Object> parsed;
        try {
            parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
        } catch (Exception e) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "malformed_body", "could not parse the body: " + e.getMessage());
            return;
        }
        final String source;
        if (id != null) {
            final var stored = store.get(id);
            if (stored.isEmpty()) {
                sendQuietly(channel, RestStatus.NOT_FOUND, "pipeline_missing", "no such pipeline: " + id);
                return;
            }
            source = stored.get();
        } else if (parsed.get("pipeline") instanceof Map<?, ?> inline) {
            source = toJson(inline);
        } else {
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "missing_pipeline",
                "name a stored pipeline in the path, or send one under 'pipeline'"
            );
            return;
        }
        final org.opensearch.ingest.Pipeline pipeline;
        try {
            pipeline = serving.ingestPipelines().compile(id == null ? "_simulate" : id, source);
        } catch (Exception e) {
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "invalid_pipeline",
                (e.getMessage() == null ? e.toString() : e.getMessage())
                    + ". Available processors: "
                    + String.join(", ", serving.ingestPipelines().available())
            );
            return;
        }
        if (!(parsed.get("docs") instanceof List<?> docs) || docs.isEmpty()) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "missing_docs", "a simulate body needs a non-empty 'docs' array");
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("docs");
            for (Object each : docs) {
                builder.startObject();
                if (!(each instanceof Map<?, ?> doc)) {
                    writeError(builder, "illegal_argument_exception", "each entry in docs must be an object");
                    builder.endObject();
                    continue;
                }
                final String index = doc.get("_index") == null ? "_index" : String.valueOf(doc.get("_index"));
                final String docId = doc.get("_id") == null ? "_id" : String.valueOf(doc.get("_id"));
                final Object given = doc.get("_source");
                if ((given instanceof Map<?, ?>) == false) {
                    writeError(builder, "illegal_argument_exception", "each document needs a _source object");
                    builder.endObject();
                    continue;
                }
                final String docSource = toJson((Map<?, ?>) given);
                try {
                    final var outcome = serving.ingestPipelines().run(pipeline, index, docId, docSource);
                    if (outcome.dropped() == false) {
                        builder.startObject("doc");
                        builder.field("_index", index);
                        builder.field("_id", docId);
                        builder.rawField(
                            "_source",
                            new java.io.ByteArrayInputStream(outcome.source().getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                            org.opensearch.common.xcontent.XContentType.JSON
                        );
                        builder.startObject("_ingest");
                        builder.field("timestamp", java.time.Instant.now().toString());
                        builder.endObject();
                        builder.endObject();
                    }
                } catch (Exception e) {
                    writeError(builder, "exception", e.getMessage() == null ? e.toString() : e.getMessage());
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    @SuppressWarnings("unchecked")
    private static String toJson(Map<?, ?> map) throws IOException {
        try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.map((Map<String, Object>) map);
            return org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static void writeError(XContentBuilder builder, String type, String reason) throws IOException {
        builder.startObject("error");
        builder.field("type", type);
        builder.field("reason", reason);
        builder.endObject();
    }

    private void get(org.opensearch.rest.RestChannel channel, TemplateStore store, String id) throws IOException {
        final Map<String, String> found;
        if (id == null) {
            found = store.all();
        } else {
            final Optional<String> one = store.get(id);
            if (one.isEmpty()) {
                sendQuietly(channel, RestStatus.NOT_FOUND, "pipeline_missing", "no such pipeline: " + id);
                return;
            }
            found = Map.of(id, one.get());
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (Map.Entry<String, String> each : found.entrySet()) {
                // Keyed by id, the shape OpenSearch returns, and the stored JSON handed back as it arrived.
                builder.rawField(
                    each.getKey(),
                    new java.io.ByteArrayInputStream(each.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    org.opensearch.common.xcontent.XContentType.JSON
                );
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void delete(org.opensearch.rest.RestChannel channel, TemplateStore store, String id) throws IOException {
        if (store.delete(id) == false) {
            sendQuietly(channel, RestStatus.NOT_FOUND, "pipeline_missing", "no such pipeline: " + id);
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a pipeline refusal", e);
        }
    }
}
