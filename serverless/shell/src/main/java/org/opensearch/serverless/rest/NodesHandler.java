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
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code GET /_nodes} and {@code GET /_cat/nodes}, answered from the lease registry.
 *
 * <p><b>Why this stopped being a refusal.</b> These endpoints were refused with "there is no cluster-wide
 * state in a serverless cluster; no node can answer this". The first clause is true and the second is not,
 * and it stopped being true the moment leases became an address book. Every node publishes a lease to a
 * register in the object store and reads the others' to know where to forward a write; that registry
 * <em>is</em> the membership list. A refusal that says nobody can answer, from a node that answers this exact
 * question on every heartbeat in order to route traffic, is a stale refusal — the same kind M50 found in
 * {@code _bulk}, and worth naming as such rather than quietly fixing.
 *
 * <p><b>What is honest about the answer, and what is not claimed.</b> This is the set of nodes holding an
 * unexpired lease, as observed by this node, now. It is not a consensus snapshot and nothing here pretends
 * it is: a node that died a moment ago is listed until its lease expires, and a node that started a moment
 * ago appears when its first lease lands. That staleness is bounded by the lease TTL and is the same
 * staleness every routing decision in this shell already runs on. The classic answer is also one node's
 * view — the cluster manager's — so this is a different source for the same kind of claim, not a weaker
 * kind of claim.
 *
 * <p><b>What costs what.</b> One listing of the members container plus one read per member. That is the cost
 * the heartbeat already pays, it is bounded by the size of the fleet, and it does not grow with the number of
 * indices — which is the property that separates this from the endpoints still refused.
 */
public final class NodesHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node, for the cluster name
     */
    public NodesHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_nodes_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_nodes"),
            new Route(RestRequest.Method.GET, "/_nodes/{nodeId}"),
            new Route(RestRequest.Method.GET, "/_nodes/{nodeId}/{metric}"),
            new Route(RestRequest.Method.GET, "/_cat/nodes")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean cat = request.path().startsWith("/_cat");
        final String wanted = request.param("nodeId");
        final String metric = request.param("metric");
        // Hints: there is no cluster manager to time out against, and settings are not rendered.
        request.param("timeout");
        request.param("flat_settings");
        if (metric != null || (wanted != null && "_all".equals(wanted) == false && NODE_METRICS.contains(wanted))) {
            // GET /_nodes/os is a metric, not a node id, and GET /_nodes/{id}/jvm asks for one. This used
            // to answer "no live node matches [os]", a confident wrong answer about something that was
            // never a name.
            final String asked = metric != null ? metric : wanted;
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "the node info metric '"
                        + asked
                        + "' is not reported here: GET /_nodes reports each node's name, address, roles and version, "
                        + "and GET /_serverless/stats answers for the node it is sent to"
                )
            );
        }
        final String unsupported = cat ? CatTable.unsupported(request) : null;
        // Consumed whether or not it is used, because BaseRestHandler rejects a request whose parameters
        // were not all read -- the same trap that turned refusals on placeholder paths into 400s.
        request.param("format");
        request.paramAsBoolean("v", false);

        if (unsupported != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_cat_parameter", unsupported)
            );
        }

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        return channel -> dispatch(channel, () -> {
            metadata.membership().refresh();
            final List<NodeLease> live = new ArrayList<>(metadata.membership().current());
            live.sort(Comparator.comparing(NodeLease::nodeId));

            final List<NodeLease> selected = new ArrayList<>();
            for (NodeLease lease : live) {
                if (wanted == null || matches(lease, wanted)) {
                    selected.add(lease);
                }
            }
            if (wanted != null && NODE_SUB_APIS.contains(wanted)) {
                // A node sub-API, not a node id. These do not begin with an underscore, so the backstop
                // above does not catch them, and "no live node matches [hot_threads]" is the same confusing
                // answer in a different costume. Named explicitly because the list is short and closed.
                sendQuietly(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'"
                        + wanted
                        + "' is a node API rather than a node id, and this shell does not implement it. "
                        + "GET /_serverless/stats answers for the node it is sent to"
                );
                return;
            }
            if (wanted != null && wanted.startsWith("_") && "_all".equals(wanted) == false) {
                // A selector, not a node. "_local", "_master" and "_cluster_manager" pick nodes by role or
                // by which one received the request; answering "no live node matches [_local]" would send a
                // caller looking for a node that was never a name. Same class as an index API read as an
                // index name.
                sendQuietly(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'"
                        + wanted
                        + "' is a node selector, and this shell does not implement selectors. Name a node, "
                        + "or use _all; there is no cluster manager for _master or _cluster_manager to pick"
                );
                return;
            }
            if (wanted != null && selected.isEmpty()) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "node_not_found", "no live node matches [" + wanted + "]")
                );
                return;
            }

            if (cat) {
                final CatTable table = new CatTable("id", "name", "address", "roles", "lease_expires_at_millis");
                for (NodeLease lease : selected) {
                    table.row(lease.nodeId(), lease.name(), lease.address(), String.join(",", sorted(lease)), lease.expiresAtMillis());
                }
                table.send(channel, request);
                return;
            }

            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                // Real OpenSearch's own fan-out accounting. Every node here came from a lease this node
                // read, so nothing failed and nothing was skipped -- but the field is reported rather than
                // omitted, because a client that checks it should find it, and because the day this does
                // fan out is the day it has to be truthful.
                builder.startObject("_nodes");
                builder.field("total", selected.size());
                builder.field("successful", selected.size());
                builder.field("failed", 0);
                builder.endObject();
                builder.field("cluster_name", clusterName());
                builder.startObject("nodes");
                for (NodeLease lease : selected) {
                    builder.startObject(lease.nodeId());
                    builder.field("name", lease.name());
                    builder.field("ephemeral_id", lease.ephemeralId());
                    builder.field("transport_address", lease.address());
                    if (lease.version() != null) {
                        // Absent rather than guessed. A lease written before the field existed does not say,
                        // and reporting this node's version for a peer would be wrong during exactly the
                        // rolling upgrade where the answer matters.
                        builder.field("version", lease.version());
                    }
                    // This deployment's own role names, not classic's. A node here is an ingest node or a
                    // search node; there is no cluster-manager role to report and no data/non-data split,
                    // because both roles hold shards. Translating them into classic's vocabulary would be
                    // inventing a distinction this architecture does not make.
                    builder.field("roles", sorted(lease));
                    builder.startObject("attributes");
                    builder.field("lease_expires_at_millis", Long.toString(lease.expiresAtMillis()));
                    builder.endObject();
                    builder.endObject();
                }
                builder.endObject();
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        });
    }

    /**
     * Segments that follow {@code /_nodes/} and are APIs rather than node ids.
     *
     * <p>A node could in principle be named any of these; nothing stops an operator. The trade is between
     * refusing a node whose name collides with an API and telling every caller of a real API that their node
     * is missing, and the second is the far more likely mistake to make.
     */
    /** The metric names core's nodes-info API takes as its second path segment. */
    private static final java.util.Set<String> NODE_METRICS = java.util.Set.of(
        "settings",
        "os",
        "process",
        "jvm",
        "thread_pool",
        "transport",
        "http",
        "plugins",
        "ingest",
        "aggregations",
        "indices",
        "search_pipelines",
        "_all"
    );

    private static final java.util.Set<String> NODE_SUB_APIS = java.util.Set.of(
        "hot_threads",
        "usage",
        "reload_secure_settings",
        "info",
        "stats"
    );

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a nodes refusal", e);
        }
    }

    private static boolean matches(NodeLease lease, String wanted) {
        return wanted.equals(lease.nodeId()) || wanted.equals(lease.name()) || "_all".equals(wanted);
    }

    private static String[] sorted(NodeLease lease) {
        return lease.roles().stream().sorted().toArray(String[]::new);
    }

    private String clusterName() {
        final var serving = node.get();
        return serving == null ? "serverless" : serving.clusterName();
    }

    private void dispatch(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        final var serving = node.get();
        if (serving == null) {
            run(channel, work);
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> run(channel, work));
    }

    private void run(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        try {
            IndexAdminHandler.gate(
                node.get(),
                org.opensearch.action.admin.cluster.node.info.NodesInfoAction.NAME,
                new org.opensearch.action.admin.cluster.node.info.NodesInfoRequest(),
                () -> {
                    work.run();
                    return null;
                }
            );
        } catch (Exception e) {
            try {
                channel.sendResponse(IndexAdminHandler.failure(channel, e));
            } catch (IOException nested) {
                logger.error("failed to report a nodes failure", nested);
            }
        }
    }
}
