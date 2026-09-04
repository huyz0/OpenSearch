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
 * {@code _index_template} and {@code _component_template} — configuration a new index inherits.
 *
 * <p><b>The refusal this replaces was wrong about itself.</b> It said "there is no cluster state for a
 * template to live in". A template needs somewhere to live, not specifically cluster state, and a register on
 * an object store is somewhere — which is where every index descriptor already lives. That is the fifth stale
 * reason this comparison work has turned up, and they all have the same shape: a reason that was true of one
 * thing, copied to another it was never true of.
 *
 * <p><b>Why enumerating templates is allowed where enumerating indices is not.</b> Index creation has to read
 * every template to find the ones that match, so this is an enumeration on a hot path — the thing this design
 * refuses everywhere else. The difference is that the number of templates is bounded <em>on the way in</em>:
 * {@code TemplateStore} refuses the thousand-and-first. Bounding an input is not the same as truncating an
 * answer, and it puts the cost on the operation that caused it rather than on index creation.
 *
 * <p><b>Templates are stored verbatim.</b> Parsing one into a typed model here would be a second account of a
 * shape OpenSearch defines, and the only parts this shell reads are the patterns, the priority and what is
 * composed.
 */
public final class TemplateHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public TemplateHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_template_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_index_template/{name}"),
            new Route(RestRequest.Method.POST, "/_index_template/{name}"),
            new Route(RestRequest.Method.GET, "/_index_template/{name}"),
            new Route(RestRequest.Method.HEAD, "/_index_template/{name}"),
            new Route(RestRequest.Method.DELETE, "/_index_template/{name}"),
            new Route(RestRequest.Method.GET, "/_index_template"),
            new Route(RestRequest.Method.PUT, "/_component_template/{name}"),
            new Route(RestRequest.Method.POST, "/_component_template/{name}"),
            new Route(RestRequest.Method.GET, "/_component_template/{name}"),
            new Route(RestRequest.Method.HEAD, "/_component_template/{name}"),
            new Route(RestRequest.Method.DELETE, "/_component_template/{name}"),
            new Route(RestRequest.Method.GET, "/_component_template")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean component = request.path().startsWith("/_component_template");
        final String name = request.param("name");
        final String body = request.hasContent() ? request.content().utf8ToString() : null;
        final RestRequest.Method method = request.method();
        final boolean create = request.paramAsBoolean("create", false);
        // Hints: there is no cluster manager to time out against, and cause is a log annotation.
        request.param("cause");
        request.param("master_timeout");
        request.param("cluster_manager_timeout");
        request.param("timeout");
        request.param("flat_settings");
        request.param("local");

        if (name != null && name.startsWith("_")) {
            // Not a template name: an underscore-prefixed segment here is an API this shell does not
            // implement. Without this, POST /_index_template/_simulate -- core's own simulate endpoint --
            // matched the {name} placeholder and stored a live template called "_simulate" with a 200.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'"
                        + name
                        + "' is not a template: names beginning with an underscore are reserved for APIs, and "
                        + "this shell does not implement this one"
                )
            );
        }

        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        if ((method == RestRequest.Method.PUT || method == RestRequest.Method.POST) && (body == null || body.isBlank())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a template body is required")
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final TemplateStore store = component ? metadata.componentTemplates() : metadata.indexTemplates();
                final String action = switch (method) {
                    case PUT, POST -> component
                        ? org.opensearch.action.admin.indices.template.put.PutComponentTemplateAction.NAME
                        : org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction.NAME;
                    case DELETE -> component
                        ? org.opensearch.action.admin.indices.template.delete.DeleteComponentTemplateAction.NAME
                        : org.opensearch.action.admin.indices.template.delete.DeleteComposableIndexTemplateAction.NAME;
                    default -> component
                        ? org.opensearch.action.admin.indices.template.get.GetComponentTemplateAction.NAME
                        : org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction.NAME;
                };
                IndexAdminHandler.gate(
                    serving,
                    action,
                    new org.opensearch.action.admin.indices.template.get.GetComposableIndexTemplateAction.Request(name),
                    () -> {
                        switch (method) {
                            case PUT, POST -> put(channel, store, component, name, body, create);
                            case GET -> get(channel, store, component, name);
                            case HEAD -> head(channel, store, name);
                            case DELETE -> delete(channel, store, name);
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
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_templates", e.getMessage());
            } catch (IllegalArgumentException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a template failure", nested);
                }
            }
        });
    }

    private void put(
        org.opensearch.rest.RestChannel channel,
        TemplateStore store,
        boolean component,
        String name,
        String body,
        boolean create
    ) throws IOException {
        final Map<String, Object> parsed;
        try {
            parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(body.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
        } catch (Exception e) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "malformed_body", "could not parse the template: " + e.getMessage());
            return;
        }
        if (component == false && parsed.get("index_patterns") == null) {
            // An index template with no patterns matches nothing, so storing one would be storing something
            // that can never do anything -- a quiet no-op wearing the shape of configuration.
            throw new IllegalArgumentException("an index template must name index_patterns; without them it can never match an index");
        }
        if (component && parsed.get("index_patterns") != null) {
            throw new IllegalArgumentException(
                "a component template must not name index_patterns: it is composed by an index template, which "
                    + "decides what it applies to"
            );
        }
        // Stored verbatim and read selectively, so what the resolver does not read must be refused here
        // rather than kept: a template carrying aliases round-tripped through GET looking honoured, and
        // never created one. version and _meta are stored and returned, which is all core does with them.
        if (parsed.get("template") instanceof Map<?, ?> template
            && template.get("aliases") instanceof Map<?, ?> aliases
            && aliases.isEmpty() == false) {
            sendQuietly(
                channel,
                RestStatus.NOT_IMPLEMENTED,
                "unsupported_template",
                "template.aliases is not supported: aliases are not created with an index here. Create the index, "
                    + "then PUT /{index}/_alias/{name}"
            );
            return;
        }
        if (parsed.get("data_stream") != null && (parsed.get("data_stream") instanceof Map<?, ?>) == false) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", "data_stream must be an object");
            return;
        }
        if (create && store.get(name).isPresent()) {
            // create=true is "add, do not replace", and replacing would be exactly what it asked not to do.
            sendQuietly(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", "template [" + name + "] already exists");
            return;
        }
        store.put(name, body);
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void get(org.opensearch.rest.RestChannel channel, TemplateStore store, boolean component, String name) throws IOException {
        final String key = component ? "component_templates" : "index_templates";
        final String nameKey = component ? "name" : "name";
        final Map<String, String> found;
        if (name == null) {
            found = store.all();
        } else if (name.endsWith("*")) {
            found = store.withPrefix(name.substring(0, name.length() - 1));
        } else {
            final Optional<String> one = store.get(name);
            if (one.isEmpty()) {
                sendQuietly(channel, RestStatus.NOT_FOUND, "template_missing", "no such template: " + name);
                return;
            }
            found = Map.of(name, one.get());
        }

        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray(key);
            for (Map.Entry<String, String> each : found.entrySet()) {
                builder.startObject();
                builder.field(nameKey, each.getKey());
                // The stored JSON, handed back as it arrived. Re-serialising a parsed copy would risk
                // returning something subtly different from what was stored, which on a configuration API is
                // the difference between a round trip and a rewrite.
                builder.rawField(
                    component ? "component_template" : "index_template",
                    new java.io.ByteArrayInputStream(each.getValue().getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    org.opensearch.common.xcontent.XContentType.JSON
                );
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void head(org.opensearch.rest.RestChannel channel, TemplateStore store, String name) throws IOException {
        final boolean there = name != null && store.get(name).isPresent();
        channel.sendResponse(new BytesRestResponse(there ? RestStatus.OK : RestStatus.NOT_FOUND, BytesRestResponse.TEXT_CONTENT_TYPE, ""));
    }

    private void delete(org.opensearch.rest.RestChannel channel, TemplateStore store, String name) throws IOException {
        if (store.delete(name) == false) {
            sendQuietly(channel, RestStatus.NOT_FOUND, "template_missing", "no such template: " + name);
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
            logger.error("failed to report a template refusal", e);
        }
    }
}
