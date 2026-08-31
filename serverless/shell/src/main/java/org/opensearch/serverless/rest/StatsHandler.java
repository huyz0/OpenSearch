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
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code GET /_serverless/stats} — what this node is holding, and how close it is to its limits.
 *
 * <p><b>Why not {@code /_nodes/stats}.</b> That endpoint answers for a cluster: it reports every node, and
 * a node here cannot speak for any other without the cluster-wide state this design does not have. It stays
 * refused with the rest of {@code /_cluster/*} for exactly that reason. This answers only for the node it
 * was sent to, which is the honest scope, and lives under {@code /_serverless/} with the rest of what is
 * ours rather than borrowing a name whose meaning is different.
 *
 * <p><b>What it is for.</b> Real circuit breakers and a bound on in-flight writes both turn a node that
 * would have died into a node that refuses a request. That is the right
 * trade and it leaves an operator with a 429 and no way to see the trend that produced it — whether the
 * node has been near its limit for an hour or was hit by one enormous request. These are the numbers behind
 * those refusals.
 *
 * <p><b>Counters, not a diagnosis.</b> Nothing here is derived, thresholded or averaged. A number that has
 * been through a formula is a number whose formula becomes the thing you have to understand before you can
 * trust it, and every one of these has a meaning an operator already knows.
 */
public final class StatsHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param node supplies the node being asked about
     */
    public StatsHandler(Supplier<ServerlessNode> node) {
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_serverless/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ServerlessNode serving = node.get();
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("node", serving.localNode().getId());
                builder.field("name", serving.localNode().getName());

                // What it is serving, split the way the design splits it: a writer holds a shard-head and
                // can accept writes, a reader holds a commit and cannot. Reporting one number for both
                // would hide the distinction the whole architecture turns on.
                final var open = serving.reconciler().openShards();
                final var readers = serving.reconciler().readerShards();
                builder.startObject("shards");
                builder.field("open", open.size());
                builder.field("readers", readers.size());
                builder.field("writers", open.size() - readers.size());
                builder.endObject();

                builder.startArray("roles");
                for (String role : serving.roles()) {
                    builder.value(role);
                }
                builder.endArray();

                // The breakers, as core reports them. "limit" and "estimated" are the two numbers a
                // refusal is decided from, so they are the two an operator needs to see it coming.
                builder.startObject("breakers");
                // From the service's own stats rather than by asking for breakers by name: the parent is
                // not a child in the registry, so a hand-written list of names silently omitted the one
                // that actually trips first. This is the same source _nodes/stats reads.
                for (var breaker : serving.circuitBreakerService().stats().getAllStats()) {
                    builder.startObject(breaker.getName());
                    builder.field("limit_bytes", breaker.getLimit());
                    builder.field("estimated_bytes", breaker.getEstimated());
                    builder.field("tripped", breaker.getTrippedCount());
                    builder.endObject();
                }
                builder.endObject();

                // In-flight writes, which the breakers do not cover: these are the bytes of the writes
                // themselves rather than what computing an answer allocated.
                final var pressure = serving.indexingPressure().stats();
                builder.startObject("indexing_pressure");
                builder.field("current_bytes", pressure.getCurrentCombinedCoordinatingAndPrimaryBytes());
                builder.field("total_bytes", pressure.getTotalCombinedCoordinatingAndPrimaryBytes());
                builder.field("rejections", pressure.getCoordinatingRejections());
                builder.endObject();

                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
