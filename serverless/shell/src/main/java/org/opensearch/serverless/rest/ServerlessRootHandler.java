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
        // HEAD is what every client library's ping() sends. Core registers both; this registered only GET,
        // so the first thing a client did on connecting was fail.
        return List.of(new Route(RestRequest.Method.GET, "/"), new Route(RestRequest.Method.HEAD, "/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method() == RestRequest.Method.HEAD) {
            return channel -> channel.sendResponse(new BytesRestResponse(RestStatus.OK, ""));
        }
        // Core's MainResponse field for field, because this is the document a client reads before it reads
        // anything else: Dashboards checks version.distribution and version.number here, the Java client
        // checks that it is talking to something at all. Two fields carry this deployment's own facts
        // where core's are node facts -- node_id, and flavour -- and are additive.
        final org.opensearch.Build build = org.opensearch.Build.CURRENT;
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("name", nodeName);
                builder.field("cluster_name", clusterName);
                // There is no cluster state to mint a uuid in. Core's own placeholder for "not known", so a
                // client comparing it against a stored value sees the value core would show before state
                // is recovered rather than a made-up one.
                builder.field("cluster_uuid", org.opensearch.cluster.ClusterState.UNKNOWN_UUID);
                builder.field("node_id", nodeIdSupplier.get());
                builder.field("flavour", "serverless");
                builder.startObject("version")
                    .field("distribution", build.getDistribution())
                    .field("number", build.getQualifiedVersion())
                    .field("build_type", build.type().displayName())
                    .field("build_hash", build.hash())
                    .field("build_date", build.date())
                    .field("build_snapshot", build.isSnapshot())
                    .field("lucene_version", Version.CURRENT.luceneVersion.toString())
                    .field("minimum_wire_compatibility_version", Version.CURRENT.minimumCompatibilityVersion().toString())
                    .field("minimum_index_compatibility_version", Version.CURRENT.minimumIndexCompatibilityVersion().toString())
                    .endObject();
                builder.field("tagline", org.opensearch.action.main.MainResponse.TAGLINE);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }
}
