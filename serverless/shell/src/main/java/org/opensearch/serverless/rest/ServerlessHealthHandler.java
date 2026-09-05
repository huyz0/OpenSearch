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
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * {@code GET /_serverless/health} — whether this node is serving, and how many peers it can see.
 *
 * <p>Deliberately not {@code _cluster/health}. There is no cluster-wide health in this design: there is
 * no elected node to compute it and no global state to compute it from. Reporting a green cluster from
 * a node that can only see itself would be exactly the confident-empty-answer failure D2 exists to
 * prevent, so the endpoint reports what this node actually knows and says so in its own name.
 */
public final class ServerlessHealthHandler extends BaseRestHandler {

    private final BooleanSupplier started;
    private final IntSupplier visibleMembers;
    private final java.util.function.Supplier<String> nodeIdSupplier;

    /**
     * Creates the handler.
     *
     * @param started whether the node has started
     * @param visibleMembers how many nodes this node currently observes, itself included
     * @param nodeIdSupplier supplies this node's id
     */
    public ServerlessHealthHandler(
        BooleanSupplier started,
        IntSupplier visibleMembers,
        java.util.function.Supplier<String> nodeIdSupplier
    ) {
        this.started = started;
        this.visibleMembers = visibleMembers;
        this.nodeIdSupplier = nodeIdSupplier;
    }

    @Override
    public String getName() {
        return "serverless_health_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_serverless/health"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean up = started.getAsBoolean();
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("status", up ? "serving" : "starting");
                builder.field("node_id", nodeIdSupplier.get());
                // Named for what it is. This node's observation, not a cluster-wide fact.
                builder.field("members_visible_to_this_node", visibleMembers.getAsInt());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(up ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE, builder));
            }
        };
    }
}
