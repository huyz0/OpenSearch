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
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * {@code GET /_serverless/ready}: 200 once the node is started and, if it serves searches, warm; 503 while it starts,
 * warms or drains.
 *
 * <p>Meant for a Kubernetes readiness probe. It reads the same flag that puts the search role in the node's lease, so
 * the ingress starts sending this node requests at about the moment coordinators start ranking it as a reader, and
 * stops when it drains. {@code /_serverless/health} stays the liveness answer: a warming node is healthy, not ready.
 */
public final class ServerlessReadyHandler extends BaseRestHandler {

    private final BooleanSupplier started;
    private final Supplier<SearchReadiness> readiness;

    /**
     * Creates the handler.
     *
     * @param started whether the node has started
     * @param readiness the node's search readiness
     */
    public ServerlessReadyHandler(BooleanSupplier started, Supplier<SearchReadiness> readiness) {
        this.started = started;
        this.readiness = readiness;
    }

    @Override
    public String getName() {
        return "serverless_ready_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_serverless/ready"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean up = started.getAsBoolean();
        final SearchReadiness search = readiness.get();
        final boolean ready = up && search.ready();
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("ready", ready);
                builder.field("state", up ? search.state().name().toLowerCase(java.util.Locale.ROOT) : "starting");
                builder.field("serves_searches", search.servesSearches());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(ready ? RestStatus.OK : RestStatus.SERVICE_UNAVAILABLE, builder));
            }
        };
    }
}
