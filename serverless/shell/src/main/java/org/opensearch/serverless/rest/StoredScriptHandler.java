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
        final var content = request.hasContent() ? request.content() : org.opensearch.core.common.bytes.BytesArray.EMPTY;
        final var mediaType = request.getMediaType() == null ? org.opensearch.common.xcontent.XContentType.JSON : request.getMediaType();
        final RestRequest.Method method = request.method();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final StoredScriptStore store = metadata.scripts();
                if (method == RestRequest.Method.PUT || method == RestRequest.Method.POST) {
                    final var ask = new org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest().id(id)
                        .context(context)
                        .content(content, mediaType);
                    final var response = serving.actionGate()
                        .run(org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptAction.NAME, ask, admitted -> {
                            final var selected = require(
                                admitted,
                                org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest.class
                            );
                            validate(selected, selected.id());
                            return put(serving, store, selected.id(), selected.context(), selected.source());
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
                    final var response = serving.actionGate()
                        .run(
                            org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptAction.NAME,
                            new org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest(id),
                            admitted -> {
                                final var selected = require(
                                    admitted,
                                    org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest.class
                                );
                                validate(selected, selected.id());
                                return delete(serving, store, selected.id());
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
                    final var response = serving.actionGate()
                        .run(
                            org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptAction.NAME,
                            new org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest(id),
                            admitted -> {
                                final var selected = require(
                                    admitted,
                                    org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest.class
                                );
                                validate(selected, selected.id());
                                return get(store, selected.id());
                            },
                            org.opensearch.serverless.shell.ActionGate.identity(
                                org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse.class
                            )
                        );
                    try (XContentBuilder builder = channel.newBuilder()) {
                        response.toXContent(builder, request);
                        channel.sendResponse(new BytesRestResponse(response.status(), builder));
                    }
                }
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

    private org.opensearch.action.support.clustermanager.AcknowledgedResponse put(
        ServerlessNode serving,
        StoredScriptStore store,
        String id,
        String context,
        StoredScriptSource source
    ) throws IOException {
        if (source == null) {
            throw new IllegalArgumentException("a stored script source is required");
        }
        // Compiled where it is stored, the way core does when a context is named: a script that does not
        // compile is refused here rather than on the first request that names it.
        final ScriptContext<?> named = context == null ? null : ScriptModule.CORE_CONTEXTS.get(context);
        if (context != null && named == null) {
            throw new IllegalArgumentException("unknown script context [" + context + "]");
        }
        if (serving.scriptService().isLangSupported(source.getLang()) == false) {
            throw new IllegalArgumentException("script_lang not supported [" + source.getLang() + "]");
        }
        try {
            compile(serving, source, named == null ? org.opensearch.script.TemplateScript.CONTEXT : named);
        } catch (Exception e) {
            if (named != null || "mustache".equals(source.getLang())) {
                // Named contexts are checked, and a mustache template has only one context; a painless
                // script with no context named is stored unchecked, as core stores it, since it may be
                // meant for any of them.
                throw new IllegalArgumentException("the script does not compile: " + e.getMessage(), e);
            }
        }
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject().field("script");
            source.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
            store.put(id, org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString());
        }
        serving.storedScripts().refresh(true);
        return new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true);
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

    private static org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse get(StoredScriptStore store, String id)
        throws IOException {
        final var stored = store.get(id);
        // The native response's value constructor is package-private; use its public XContent parser.
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject().field("_id", id).field("found", stored.isPresent());
            if (stored.isPresent()) {
                builder.field("script");
                StoredScripts.parse(stored.get()).toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();
            try (
                var parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                    .createParser(
                        org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                        org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        org.opensearch.core.common.bytes.BytesReference.bytes(builder).streamInput()
                    )
            ) {
                return org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse.fromXContent(parser);
            }
        }
    }

    private static org.opensearch.action.support.clustermanager.AcknowledgedResponse delete(
        ServerlessNode serving,
        StoredScriptStore store,
        String id
    ) throws IOException {
        if (store.delete(id) == false) {
            throw new org.opensearch.ResourceNotFoundException("stored script [" + id + "] does not exist and cannot be deleted");
        }
        serving.storedScripts().refresh(true);
        return new org.opensearch.action.support.clustermanager.AcknowledgedResponse(true);
    }

    private static <T> T require(org.opensearch.action.ActionRequest request, Class<T> type) {
        if (type.isInstance(request) == false) {
            throw new IllegalArgumentException("expected admitted request " + type.getSimpleName());
        }
        return type.cast(request);
    }

    private static void validate(org.opensearch.action.ActionRequest request, String id) {
        final var invalid = request.validate();
        if (invalid != null) {
            throw new IllegalArgumentException(invalid.getMessage(), invalid);
        }
        if (id.startsWith("_")) {
            throw new IllegalArgumentException("stored script ids beginning with an underscore are reserved for APIs");
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
