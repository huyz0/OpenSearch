/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.StoredScriptSource;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.StoredScriptStore;
import org.opensearch.serverless.script.StoredScripts;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code /_scripts/{id}}: stored scripts, in this deployment's own register rather than in cluster state.
 *
 * <p>The refusal this replaces said stored scripts resolve through cluster state, and they did -- through
 * one method of {@code ScriptService}, now overridden to read a register. The script is compiled at
 * {@code PUT} when a context is named, as core does, so a script that cannot compile is refused where the
 * operator is standing rather than on somebody else's later request.
 */
public final class StoredScriptHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node
     */
    public StoredScriptHandler(Supplier<MetadataPlane> plane, Supplier<ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_stored_script_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_scripts/{id}"),
            new Route(RestRequest.Method.POST, "/_scripts/{id}"),
            new Route(RestRequest.Method.PUT, "/_scripts/{id}/{context}"),
            new Route(RestRequest.Method.POST, "/_scripts/{id}/{context}"),
            new Route(RestRequest.Method.GET, "/_scripts/{id}"),
            new Route(RestRequest.Method.DELETE, "/_scripts/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param("id");
        final String context = request.param("context");
        // Hints: there is no cluster manager to time out against.
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
        if (id.startsWith("_")) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'" + id + "' is not a script id: names beginning with an underscore are reserved for APIs"
                )
            );
        }
        final String body = request.hasContent() ? request.content().utf8ToString() : null;
        final RestRequest.Method method = request.method();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final StoredScriptStore store = metadata.scripts();
                final String action = switch (method) {
                    case PUT, POST -> org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptAction.NAME;
                    case DELETE -> org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction.NAME;
                    default -> org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptAction.NAME;
                };
                IndexAdminHandler.gate(
                    serving,
                    action,
                    new org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest(id),
                    () -> {
                        switch (method) {
                            case PUT, POST -> put(channel, serving, store, id, context, body);
                            case GET -> get(channel, store, id, request);
                            case DELETE -> delete(channel, serving, store, id);
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
            } catch (org.opensearch.serverless.metadata.TemplateStore.TooManyTemplatesException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "too_many_scripts", e.getMessage());
            } catch (IllegalArgumentException e) {
                sendQuietly(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a stored-script failure", nested);
                }
            }
        });
    }

    private void put(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        StoredScriptStore store,
        String id,
        String context,
        String body
    ) throws IOException {
        if (body == null || body.isBlank()) {
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "missing_body",
                "a script body is required: {\"script\": {\"lang\": ..., \"source\": ...}}"
            );
            return;
        }
        final StoredScriptSource source;
        try {
            source = StoredScripts.parse(body);
        } catch (Exception e) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", "could not parse the script: " + e.getMessage());
            return;
        }
        // Compiled where it is stored, the way core does when a context is named: a script that does not
        // compile is refused here rather than on the first request that names it.
        final ScriptContext<?> named = context == null ? null : ScriptModule.CORE_CONTEXTS.get(context);
        if (context != null && named == null) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", "unknown script context [" + context + "]");
            return;
        }
        if (serving.scriptService().isLangSupported(source.getLang()) == false) {
            // Refused where the operator is standing. This used to be swallowed with the compile failure
            // below, so a script in a language no node can run was stored and failed on first use.
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "script_lang not supported [" + source.getLang() + "]"
            );
            return;
        }
        try {
            compile(serving, source, named == null ? org.opensearch.script.TemplateScript.CONTEXT : named);
        } catch (Exception e) {
            if (named != null || "mustache".equals(source.getLang())) {
                // Named contexts are checked, and a mustache template has only one context; a painless
                // script with no context named is stored unchecked, as core stores it, since it may be
                // meant for any of them.
                sendQuietly(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "the script does not compile: " + (e.getMessage() == null ? e.toString() : e.getMessage())
                );
                return;
            }
        }
        store.put(id, body);
        serving.storedScripts().refresh(true);
        acknowledge(channel);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static void compile(ServerlessNode serving, StoredScriptSource source, ScriptContext context) {
        serving.scriptService()
            .compile(
                new org.opensearch.script.Script(
                    org.opensearch.script.ScriptType.INLINE,
                    source.getLang(),
                    source.getSource(),
                    source.getOptions(),
                    java.util.Map.of()
                ),
                context
            );
    }

    private void get(org.opensearch.rest.RestChannel channel, StoredScriptStore store, String id, ToXContent.Params params)
        throws IOException {
        final Optional<String> stored = store.get(id);
        // Core's GetStoredScriptResponse shape.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("_id", id);
            builder.field("found", stored.isPresent());
            if (stored.isPresent()) {
                builder.field("script");
                StoredScripts.parse(stored.get()).toXContent(builder, params);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(stored.isPresent() ? RestStatus.OK : RestStatus.NOT_FOUND, builder));
        }
    }

    private void delete(org.opensearch.rest.RestChannel channel, ServerlessNode serving, StoredScriptStore store, String id)
        throws IOException {
        if (store.delete(id) == false) {
            sendQuietly(
                channel,
                RestStatus.NOT_FOUND,
                "resource_not_found_exception",
                "stored script [" + id + "] does not exist and cannot be deleted"
            );
            return;
        }
        serving.storedScripts().refresh(true);
        acknowledge(channel);
    }

    private static void acknowledge(org.opensearch.rest.RestChannel channel) throws IOException {
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
            logger.error("failed to report a stored-script refusal", e);
        }
    }
}
