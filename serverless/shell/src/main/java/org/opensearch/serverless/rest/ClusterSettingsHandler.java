/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
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
     * @param node supplies the node, for the write path's action gate
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
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        if (request.method() == RestRequest.Method.GET) {
            return channel -> {
                final var current = metadata.clusterConfig().read();
                respond(channel, current.settings(), false);
            };
        }

        // PUT. Every parameter and the body read before any early return, or BaseRestHandler turns a
        // deliberate refusal into a 400 about an unconsumed parameter.
        final Map<String, Object> body;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            body = parser.map();
        }
        final Object persistentField = body.get("persistent");
        final Object transientField = body.get("transient");
        if (transientField instanceof Map<?, ?> transientMap && transientMap.isEmpty() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "transient settings are not supported; use persistent. There is no cluster manager and no restart "
                        + "to reset a transient setting on, which is the only reason to want one."
                )
            );
        }
        if ((persistentField instanceof Map) == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "missing_persistent",
                    "a settings update needs a 'persistent' object"
                )
            );
        }
        final Map<String, Object> flat = ClusterConfig.flatten((Map<String, Object>) persistentField);
        final String listKey = ClusterConfig.firstListValue(flat);
        if (listKey != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_write",
                    "'" + listKey + "' is a list; list-valued settings are not supported on this register"
                )
            );
        }

        final ServerlessNode serving = node.get();
        // Off the transport thread: the update is a compare-and-swap against the object store, retried
        // under contention, and every other write on this surface already learned not to block the thread
        // that should be reading the next request on remote IO. This one had not, yet -- found by the
        // filter-surface sweep tripping core's own "not a transport thread" assertion.
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.WRITE).execute(() -> {
            try {
                final Settings written = gated(serving, () -> metadata.clusterConfig().update(flat));
                respond(channel, written, true);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a cluster settings write failure", nested);
                }
            }
        });
    }

    private static Settings gated(ServerlessNode serving, org.opensearch.common.CheckedSupplier<Settings, Exception> work)
        throws Exception {
        if (serving == null) {
            return work.get();
        }
        return serving.actionGate()
            .run(
                org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsAction.NAME,
                new org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest(),
                work
            );
    }

    private void respond(org.opensearch.rest.RestChannel channel, Settings settings, boolean acknowledged) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            // Only on the write path -- real OpenSearch's own GET has no such field, and a caller reading
            // it there would find nothing rather than a stale or invented value.
            if (acknowledged) {
                builder.field("acknowledged", true);
            }
            builder.startObject("persistent");
            settings.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
            // Always present and always empty: a client that reads response.transient expecting an
            // object, rather than checking whether the key exists at all, gets one.
            builder.startObject("transient").endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
