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
import java.util.function.Supplier;

/**
 * {@code /_search/pipeline}: search pipelines, stored in a register and compiled when stored.
 *
 * <p>The same shape as ingest pipelines, for the same reasons: a pipeline naming a processor this node
 * cannot build is refused where the operator is standing, and the refusal names what it can build.
 */
public final class SearchPipelineHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node
     */
    public SearchPipelineHandler(Supplier<MetadataPlane> plane, Supplier<ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_search_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_search/pipeline/{id}"),
            new Route(RestRequest.Method.GET, "/_search/pipeline/{id}"),
            new Route(RestRequest.Method.DELETE, "/_search/pipeline/{id}"),
            new Route(RestRequest.Method.GET, "/_search/pipeline")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        final RestRequest.Method method = request.method();
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
        if (method == RestRequest.Method.PUT) {
            ask = new org.opensearch.action.search.PutSearchPipelineRequest(id, source, mediaType);
            action = org.opensearch.action.search.PutSearchPipelineAction.NAME;
        } else if (method == RestRequest.Method.DELETE) {
            ask = new org.opensearch.action.search.DeleteSearchPipelineRequest(id);
            action = org.opensearch.action.search.DeleteSearchPipelineAction.NAME;
        } else {
            ask = new org.opensearch.action.search.GetSearchPipelineRequest(
                org.opensearch.core.common.Strings.splitStringByCommaToArray(id)
            );
            action = org.opensearch.action.search.GetSearchPipelineAction.NAME;
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final TemplateStore store = metadata.searchPipelines();
                if (method == RestRequest.Method.PUT) {
                    final var response = serving.actionGate().run(action, ask, admitted -> {
                        final var selected = require(admitted, org.opensearch.action.search.PutSearchPipelineRequest.class);
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
                        final var selected = require(admitted, org.opensearch.action.search.DeleteSearchPipelineRequest.class);
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
                        final var selected = require(admitted, org.opensearch.action.search.GetSearchPipelineRequest.class);
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
            serving.searchPipelines().validate(id, body);
        } catch (Exception e) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "invalid_pipeline",
                (e.getMessage() == null ? e.toString() : e.getMessage())
                    + ". Available processors: "
                    + String.join(", ", serving.searchPipelines().available())
            );
        }
        store.put(id, body);
        return new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true);
    }

    private void get(org.opensearch.rest.RestChannel channel, TemplateStore store, String id) throws IOException {
        final Map<String, String> found;
        if (id == null) {
            found = store.all();
        } else {
            final var one = store.get(id);
            if (one.isEmpty()) {
                sendQuietly(channel, RestStatus.NOT_FOUND, "pipeline_missing", "no such search pipeline: " + id);
                return;
            }
            found = Map.of(id, one.get());
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (Map.Entry<String, String> each : found.entrySet()) {
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
        if (id == null || id.isBlank() || id.startsWith("_") || id.indexOf('*') >= 0 || id.indexOf('?') >= 0 || "_all".equals(id)) {
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
                    throw unsupported("cached or rewritten native search pipeline read responses are not supported");
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
            logger.error("failed to report a search-pipeline refusal", e);
        }
    }
}
