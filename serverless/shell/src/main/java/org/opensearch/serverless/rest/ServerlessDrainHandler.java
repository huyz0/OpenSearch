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
import org.opensearch.serverless.shell.SearchDrain;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * {@code POST /_serverless/drain}: takes this search node out of search traffic and hands its warm shards to its peers,
 * answering once they have taken them ({@link SearchDrain}).
 *
 * <p>Meant for a Kubernetes {@code preStop} hook, which runs before the pod is sent SIGTERM and may take as long as the
 * termination grace period allows. The readiness endpoint answers 503 from the moment this is called. A second call
 * answers at once with nothing done.
 */
public final class ServerlessDrainHandler extends BaseRestHandler {

    private final Supplier<SearchDrain.Outcome> drain;
    private final Executor executor;

    /**
     * Creates the handler.
     *
     * @param drain drains the node
     * @param executor where the drain runs, off the HTTP thread
     */
    public ServerlessDrainHandler(Supplier<SearchDrain.Outcome> drain, Executor executor) {
        this.drain = drain;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return "serverless_drain_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_serverless/drain"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> executor.execute(() -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                final SearchDrain.Outcome outcome = drain.get();
                builder.startObject();
                builder.field("peers_asked", outcome.peersAsked());
                builder.field("peers_failed", outcome.peersFailed());
                builder.field("shards_taken_over", outcome.shardsTakenOver());
                builder.field("blocks_handed_over", outcome.blocksHandedOver());
                builder.field("took_millis", outcome.tookMillis());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException inner) {
                    e.addSuppressed(inner);
                }
            }
        });
    }
}
