/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsAction;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.ClusterConfig;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code /_cluster/settings} — §9.3's {@code /cluster/config} register, at the surface a caller reaches.
 *
 * <p>The one piece of {@code /_cluster/*} that is not refused. Everything else under that prefix answers
 * for cluster-wide state this design does not have — routing tables, node lists, health computed across a
 * fleet — and a node-local answer to any of those would mislead. Settings are different: they are one
 * register, exactly like an index descriptor, and this node can answer for it the same honest way it
 * answers for an index. See {@link ClusterConfig} for what it does and does not validate.
 *
 * <p><b>Only {@code persistent}.</b> A {@code transient} block that is not empty is refused rather than
 * silently dropped or silently applied as if it were persistent — the same discipline as everywhere else
 * on this surface: a caller who asked for one thing must not be quietly given another.
 */
public final class ClusterSettingsHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, for the read and write action gates
     */
    public ClusterSettingsHandler(Supplier<MetadataPlane> plane, Supplier<ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_cluster_settings_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_cluster/settings"), new Route(RestRequest.Method.PUT, "/_cluster/settings"));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        // There is no defaults registry or cluster manager to time out against. Formatting is passed
        // to the native response serializer after the action filters finish.
        request.param("include_defaults");
        request.param("master_timeout");
        request.param("cluster_manager_timeout");
        request.param("timeout");
        request.paramAsBoolean("flat_settings", false);
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        if (request.method() == RestRequest.Method.GET) {
            final ServerlessNode reader = node.get();
            if (reader == null) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "node_unavailable", "the serving node is unavailable")
                );
            }
            final var nativeRequest = new org.opensearch.action.admin.cluster.state.ClusterStateRequest().clear().metadata(true);
            final var indicesOptions = nativeRequest.indicesOptions();
            return channel -> reader.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    final var result = reader.actionGate()
                        .run(org.opensearch.action.admin.cluster.state.ClusterStateAction.NAME, nativeRequest, admitted -> {
                            final var selected = (org.opensearch.action.admin.cluster.state.ClusterStateRequest) admitted;
                            final var invalid = selected.validate();
                            if (invalid != null) {
                                throw invalid;
                            }
                            if (selected.routingTable()
                                || selected.nodes()
                                || selected.blocks()
                                || selected.customs()
                                || selected.indices().length != 0
                                || selected.indicesOptions().equals(indicesOptions) == false
                                || selected.waitForMetadataVersion() != null) {
                                throw new org.opensearch.OpenSearchStatusException(
                                    "cluster settings reads support global configuration metadata only; fleet state, index selection "
                                        + "and metadata-version waits are not supported",
                                    RestStatus.NOT_IMPLEMENTED
                                );
                            }
                            final Settings current = selected.metadata() ? metadata.clusterConfig().read().settings() : Settings.EMPTY;
                            final var clusterName = new org.opensearch.cluster.ClusterName(reader.clusterName());
                            // This endpoint exposes the global settings projection, not fleet topology or index metadata.
                            final var state = org.opensearch.cluster.ClusterState.builder(clusterName)
                                .metadata(org.opensearch.cluster.metadata.Metadata.builder().persistentSettings(current))
                                .build();
                            return new org.opensearch.action.admin.cluster.state.ClusterStateResponse(clusterName, state, false);
                        },
                            org.opensearch.serverless.shell.ActionGate.identity(
                                org.opensearch.action.admin.cluster.state.ClusterStateResponse.class
                            )
                        );
                    if (result.isWaitForTimedOut()) {
                        throw new org.opensearch.OpenSearchStatusException("cluster settings read timed out", RestStatus.REQUEST_TIMEOUT);
                    }
                    final var state = java.util.Objects.requireNonNull(result.getState(), "cluster settings response has no state");
                    final var settings = new org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse(
                        state.metadata().persistentSettings(),
                        state.metadata().transientSettings(),
                        Settings.EMPTY
                    );
                    try (var builder = channel.newBuilder()) {
                        settings.toXContent(builder, channel.request());
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                } catch (Exception e) {
                    try {
                        channel.sendResponse(IndexAdminHandler.failure(channel, e));
                    } catch (IOException nested) {
                        logger.error("failed to report a cluster settings read failure", nested);
                    }
                }
            });
        }

        final var content = request.hasContent() ? request.content() : null;
        final var mediaType = request.getMediaType();
        final ServerlessNode serving = node.get();
        if (serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "node_unavailable", "the serving node is unavailable")
            );
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            try {
                if (content == null) {
                    throw new IllegalArgumentException("a settings update needs a 'persistent' object");
                }
                final Map<String, Object> body;
                try (
                    var parser = mediaType.xContent()
                        .createParser(
                            org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                            org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            content.streamInput()
                        )
                ) {
                    body = parser.map();
                }
                if (body.get("persistent") instanceof Map == false) {
                    throw new IllegalArgumentException("a settings update needs a 'persistent' object");
                }
                if (body.containsKey("transient") && body.get("transient") instanceof Map == false) {
                    throw new IllegalArgumentException("'transient' settings must be an object");
                }
                final var nativeRequest = new ClusterUpdateSettingsRequest().persistentSettings(
                    (Map<String, Object>) body.get("persistent")
                );
                if (body.get("transient") instanceof Map<?, ?> transientSettings) {
                    nativeRequest.transientSettings((Map<String, Object>) transientSettings);
                }
                final var result = serving.actionGate().run(ClusterUpdateSettingsAction.NAME, nativeRequest, admitted -> {
                    final var selected = (ClusterUpdateSettingsRequest) admitted;
                    final var invalid = selected.validate();
                    if (invalid != null) {
                        throw invalid;
                    }
                    if (selected.transientSettings().isEmpty() == false) {
                        throw new org.opensearch.OpenSearchStatusException(
                            "transient settings are not supported; use persistent",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    final Map<String, Object> flat = settingsMap(selected.persistentSettings());
                    final String listKey = ClusterConfig.firstListValue(flat);
                    if (listKey != null) {
                        throw new org.opensearch.OpenSearchStatusException(
                            "'" + listKey + "' is a list; list-valued settings are not supported on this register",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    return nativeResponse(metadata.clusterConfig().update(flat));
                }, org.opensearch.serverless.shell.ActionGate.identity(ClusterUpdateSettingsResponse.class));
                try (var builder = channel.newBuilder()) {
                    result.toXContent(builder, channel.request());
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a cluster settings write failure", nested);
                }
            }
        });
    }

    /** Serializes through the public Settings API so lists and null removals retain their types. */
    private static Map<String, Object> settingsMap(Settings settings) throws IOException {
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject();
            settings.toXContent(builder, new org.opensearch.core.xcontent.ToXContent.MapParams(Map.of("flat_settings", "true")));
            builder.endObject();
            return org.opensearch.common.xcontent.XContentHelper.convertToMap(
                org.opensearch.core.common.bytes.BytesReference.bytes(builder),
                true,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
        }
    }

    private static ClusterUpdateSettingsResponse nativeResponse(Settings settings) throws IOException {
        // Core's value constructor is package private; the public parser builds the native response.
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.startObject("persistent");
            settings.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.startObject("transient").endObject();
            builder.endObject();
            try (
                var parser = org.opensearch.common.xcontent.XContentType.JSON.xContent()
                    .createParser(
                        org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                        org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        org.opensearch.core.common.bytes.BytesReference.bytes(builder).streamInput()
                    )
            ) {
                return ClusterUpdateSettingsResponse.fromXContent(parser);
            }
        }
    }

}
