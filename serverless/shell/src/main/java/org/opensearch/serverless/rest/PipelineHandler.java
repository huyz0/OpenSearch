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
import org.opensearch.serverless.shell.ServerlessNode;
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
        final RestRequest.Method method = request.method();
        final boolean simulation = request.path().endsWith("/_simulate");
        final boolean verbose = request.paramAsBoolean("verbose", false);
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
        // Keep the accounted HTTP bytes until the operation completes. Decode only the admitted body,
        // off the HTTP worker, rather than retaining a decoded copy while authorization is pending.
        final org.opensearch.core.common.bytes.BytesReference source = request.hasContent()
            ? request.content()
            : org.opensearch.core.common.bytes.BytesArray.EMPTY;
        final org.opensearch.core.xcontent.MediaType mediaType = request.getMediaType() == null
            ? org.opensearch.common.xcontent.XContentType.JSON
            : request.getMediaType();
        final org.opensearch.action.ActionRequest ask;
        final String action;
        if (simulation) {
            final var simulate = new org.opensearch.action.ingest.SimulatePipelineRequest(source, mediaType);
            simulate.setId(id);
            simulate.setVerbose(verbose);
            ask = simulate;
            action = org.opensearch.action.ingest.SimulatePipelineAction.NAME;
        } else if (method == RestRequest.Method.PUT || method == RestRequest.Method.POST) {
            ask = new org.opensearch.action.ingest.PutPipelineRequest(id, source, mediaType);
            action = org.opensearch.action.ingest.PutPipelineAction.NAME;
        } else if (method == RestRequest.Method.DELETE) {
            ask = new org.opensearch.action.ingest.DeletePipelineRequest(id);
            action = org.opensearch.action.ingest.DeletePipelineAction.NAME;
        } else {
            ask = new org.opensearch.action.ingest.GetPipelineRequest(org.opensearch.core.common.Strings.splitStringByCommaToArray(id));
            action = org.opensearch.action.ingest.GetPipelineAction.NAME;
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final TemplateStore store = metadata.pipelines();
                if (simulation) {
                    buffered(channel, serving, action, ask, (admitted, output) -> {
                        final var selected = require(admitted, org.opensearch.action.ingest.SimulatePipelineRequest.class);
                        if (selected.isVerbose()) {
                            throw unsupported("verbose pipeline simulation is not supported");
                        }
                        simulate(output, serving, store, selected.getId(), jsonSource(selected.getSource(), selected.getXContentType()));
                    });
                } else if (method == RestRequest.Method.PUT || method == RestRequest.Method.POST) {
                    final var response = serving.actionGate().run(action, ask, admitted -> {
                        final var selected = require(admitted, org.opensearch.action.ingest.PutPipelineRequest.class);
                        concreteId(selected.getId());
                        return put(serving, store, selected.getId(), jsonSource(selected.getSource(), selected.getMediaType()));
                    },
                        org.opensearch.serverless.shell.ActionGate.identity(
                            org.opensearch.action.support.clustermanager.AcknowledgedResponse.class
                        )
                    );
                    try (XContentBuilder builder = channel.newBuilder()) {
                        response.toXContent(builder, request);
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                } else if (method == RestRequest.Method.DELETE) {
                    final var response = serving.actionGate().run(action, ask, admitted -> {
                        final var selected = require(admitted, org.opensearch.action.ingest.DeletePipelineRequest.class);
                        concreteId(selected.getId());
                        return delete(store, selected.getId());
                    },
                        org.opensearch.serverless.shell.ActionGate.identity(
                            org.opensearch.action.support.clustermanager.AcknowledgedResponse.class
                        )
                    );
                    try (XContentBuilder builder = channel.newBuilder()) {
                        response.toXContent(builder, request);
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                } else {
                    buffered(channel, serving, action, ask, (admitted, output) -> {
                        final var selected = require(admitted, org.opensearch.action.ingest.GetPipelineRequest.class);
                        final String[] ids = selected.getIds();
                        if (ids == null || ids.length == 0 || (ids.length == 1 && ("*".equals(ids[0]) || "_all".equals(ids[0])))) {
                            get(output, store, null);
                        } else if (ids.length == 1) {
                            concreteId(ids[0]);
                            get(output, store, ids[0]);
                        } else {
                            throw unsupported("pipeline reads currently require one concrete ID or all pipelines");
                        }
                    });
                }
            } catch (IndexAdminHandler.RefusedException e) {
                sendQuietly(channel, e.status, e.type, e.getMessage());
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

    private org.opensearch.action.support.clustermanager.AcknowledgedResponse put(
        ServerlessNode serving,
        TemplateStore store,
        String id,
        String body
    ) throws IOException, IndexAdminHandler.RefusedException {
        try {
            serving.ingestPipelines().compile(id, body);
        } catch (Exception e) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "invalid_pipeline",
                (e.getMessage() == null ? e.toString() : e.getMessage())
                    + ". Available processors: "
                    + String.join(", ", serving.ingestPipelines().available())
            );
        }
        store.put(id, body);
        return new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true);
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

    private org.opensearch.action.support.clustermanager.AcknowledgedResponse delete(TemplateStore store, String id) throws IOException,
        IndexAdminHandler.RefusedException {
        if (store.delete(id) == false) {
            throw new IndexAdminHandler.RefusedException(RestStatus.NOT_FOUND, "pipeline_missing", "no such pipeline: " + id);
        }
        return new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true);
    }

    private static org.opensearch.OpenSearchStatusException unsupported(String reason) {
        return new org.opensearch.OpenSearchStatusException(reason, RestStatus.NOT_IMPLEMENTED);
    }

    private static <T> T require(org.opensearch.action.ActionRequest request, Class<T> type) {
        if (type.isInstance(request) == false) {
            throw unsupported("pipeline action requires " + type.getSimpleName());
        }
        return type.cast(request);
    }

    private static void concreteId(String id) {
        if (id == null || id.isBlank() || id.indexOf('*') >= 0 || id.indexOf('?') >= 0 || "_all".equals(id)) {
            throw unsupported("this pipeline action requires one concrete ID");
        }
    }

    private static String jsonSource(
        org.opensearch.core.common.bytes.BytesReference source,
        org.opensearch.core.xcontent.MediaType mediaType
    ) throws IOException {
        if (source == null || source.length() == 0) {
            throw new IllegalArgumentException("a pipeline body is required");
        }
        if (org.opensearch.common.xcontent.XContentType.JSON.equals(mediaType)) {
            return source.utf8ToString();
        }
        throw unsupported("pipeline definitions and simulation requests currently require JSON content");
    }

    private static void buffered(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedBiConsumer<org.opensearch.action.ActionRequest, org.opensearch.rest.RestChannel, Exception> work
    ) throws Exception {
        final var opaque = org.opensearch.serverless.shell.ActionGate.<org.opensearch.rest.RestResponse>opaque();
        final var view = new org.opensearch.serverless.shell.ActionGate.ResponseView<org.opensearch.rest.RestResponse>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(org.opensearch.rest.RestResponse result) {
                return opaque.show(result);
            }

            @Override
            public org.opensearch.rest.RestResponse read(
                org.opensearch.core.action.ActionResponse response,
                org.opensearch.rest.RestResponse original
            ) {
                if (original == null || response instanceof org.opensearch.serverless.shell.ActionGate.Admitted == false) {
                    throw unsupported("cached or rewritten native pipeline read/simulate responses are not supported");
                }
                return original;
            }
        };
        final var response = serving.actionGate().run(action, request, admitted -> {
            final var pending = new PendingRestChannel(channel);
            work.accept(admitted, pending);
            return pending.response();
        }, view);
        channel.sendResponse(response);
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a pipeline refusal", e);
        }
    }
}
