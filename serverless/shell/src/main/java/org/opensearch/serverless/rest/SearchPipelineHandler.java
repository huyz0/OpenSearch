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
        final String body = request.hasContent() ? request.content().utf8ToString() : null;
        final RestRequest.Method method = request.method();
        request.param("cluster_manager_timeout");
        request.param("master_timeout");
        request.param("timeout");
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if (id != null && id.startsWith("_")) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'" + id + "' is not a pipeline id: names beginning with an underscore are reserved for APIs"
                )
            );
        }
        if (method == RestRequest.Method.PUT && (body == null || body.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a pipeline body is required")
            );
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final TemplateStore store = metadata.searchPipelines();
                final String action = switch (method) {
                    case PUT -> org.opensearch.action.search.PutSearchPipelineAction.NAME;
                    case DELETE -> org.opensearch.action.search.DeleteSearchPipelineAction.NAME;
                    default -> org.opensearch.action.search.GetSearchPipelineAction.NAME;
                };
                IndexAdminHandler.gate(
                    serving,
                    action,
                    new org.opensearch.action.search.GetSearchPipelineRequest(id == null ? new String[0] : new String[] { id }),
                    () -> {
                        switch (method) {
                            case PUT -> put(channel, serving, store, id, body);
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
                    logger.error("failed to report a search-pipeline failure", nested);
                }
            }
        });
    }

    private void put(org.opensearch.rest.RestChannel channel, ServerlessNode serving, TemplateStore store, String id, String body)
        throws IOException {
        try {
            serving.searchPipelines().validate(id, body);
        } catch (Exception e) {
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "invalid_pipeline",
                (e.getMessage() == null ? e.toString() : e.getMessage())
                    + ". Available processors: "
                    + String.join(", ", serving.searchPipelines().available())
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

    private void delete(org.opensearch.rest.RestChannel channel, TemplateStore store, String id) throws IOException {
        if (store.delete(id) == false) {
            sendQuietly(channel, RestStatus.NOT_FOUND, "pipeline_missing", "no such search pipeline: " + id);
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
            logger.error("failed to report a search-pipeline refusal", e);
        }
    }
}
