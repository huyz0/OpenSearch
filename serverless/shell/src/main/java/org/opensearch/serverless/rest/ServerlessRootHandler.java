/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.Version;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code GET /} — who this node is.
 *
 * <p>Answers without touching the data plane or the metadata plane, so it stays truthful while the node
 * is otherwise unable to serve.
 */
public final class ServerlessRootHandler extends BaseRestHandler {

    private final String nodeName;
    private final String clusterName;
    private final Supplier<String> nodeIdSupplier;

    /**
     * Creates the handler.
     *
     * @param nodeName this node's name
     * @param clusterName the configured cluster name
     * @param nodeIdSupplier supplies the node id, which is known only after the environment is built
     */
    public ServerlessRootHandler(String nodeName, String clusterName, Supplier<String> nodeIdSupplier) {
        this.nodeName = nodeName;
        this.clusterName = clusterName;
        this.nodeIdSupplier = nodeIdSupplier;
    }

    @Override
    public String getName() {
        return "serverless_root_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("name", nodeName);
                builder.field("node_id", nodeIdSupplier.get());
                builder.field("cluster_name", clusterName);
                builder.field("flavour", "serverless");
                builder.startObject("version");
                builder.field("number", Version.CURRENT.toString());
                builder.endObject();
                builder.field("tagline", "The object store is the only source of truth");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
