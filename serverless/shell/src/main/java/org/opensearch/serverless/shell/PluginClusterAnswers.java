/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Cluster health and cluster state, answered for plugins from the metadata plane.
 *
 * <p><b>Why plugins ask.</b> A classic plugin learns whether its index is usable from cluster health -- the
 * security plugin waits for its configuration index to be at least yellow before it loads anything -- and
 * reads index metadata through {@code admin().cluster().state(...)}. The shell's client refused both, so such
 * a plugin waited forever or failed at start.
 *
 * <p><b>What health means here.</b> An index the metadata plane describes is green: every shard is either
 * owned or dormant, and a dormant shard is opened by the next request that needs it, so there is nothing a
 * caller could wait for. The shell's REST health endpoint says the same, and additionally reads each shard's
 * head to report one it cannot serve; this answer does not, so that a plugin polling health costs one cached
 * descriptor read per index rather than a head read per shard. Red means the metadata plane could not be read.
 * An index that does not exist is waited for until the request's timeout, as core does, and then reported red.
 *
 * <p><b>What state means here.</b> The node's own view, which holds the indices it serves and the live nodes.
 * A request that names indices gets exactly those, described from the metadata plane when this node has not
 * opened them, so a plugin asking about such an index still finds its settings and mappings.
 */
final class PluginClusterAnswers {

    private static final long POLL_MILLIS = 200L;

    /** The most names a wildcard in a plugin's request may expand to before the request is refused. */
    private static final int WILDCARD_CAP = 10_000;

    private PluginClusterAnswers() {}

    static ClusterHealthResponse health(ServerlessNode node, MetadataPlane plane, ClusterHealthRequest request) throws Exception {
        final long deadline = System.nanoTime() + request.timeout().nanos();
        while (true) {
            final List<IndexMetadata> found = new ArrayList<>();
            final boolean complete;
            try {
                complete = describe(node, plane, request.indices(), found);
            } catch (IOException e) {
                return red(node, request, false);
            }
            if (complete) {
                return healthOf(node, found);
            }
            if (System.nanoTime() >= deadline) {
                // A named index that never appeared: core answers red and timed out, and so does this.
                return red(node, request, true);
            }
            Thread.sleep(POLL_MILLIS);
        }
    }

    static ClusterStateResponse state(ServerlessNode node, MetadataPlane plane, ClusterStateRequest request) throws IOException {
        final ClusterState view = node.clusterService().state();
        final String[] named = request.indices();
        if (named == null || named.length == 0) {
            return new ClusterStateResponse(view.getClusterName(), view, false);
        }
        final List<IndexMetadata> found = new ArrayList<>();
        describe(node, plane, named, found);
        // Only the named indices, as core filters a state request by its indices: index management resolves the
        // indices a policy is attached to from this, and given the whole view it attached a policy to its own
        // configuration index too.
        final Metadata.Builder metadata = Metadata.builder(view.metadata()).removeAllIndices();
        final RoutingTable.Builder routing = RoutingTable.builder();
        for (IndexMetadata index : found) {
            final String name = index.getIndex().getName();
            final IndexMetadata held = view.metadata().index(name);
            metadata.put(held != null ? held : index, false);
            if (held != null && view.routingTable().hasIndex(name)) {
                routing.add(view.routingTable().index(name));
            }
        }
        return new ClusterStateResponse(
            view.getClusterName(),
            ClusterState.builder(view).metadata(metadata).routingTable(routing.build()).build(),
            false
        );
    }

    /**
     * Collects the metadata of the named indices: every index in the node's view when none is named, the names
     * a trailing wildcard matches, and each concrete name.
     *
     * @return false if a concrete name has no index
     */
    private static boolean describe(ServerlessNode node, MetadataPlane plane, String[] names, List<IndexMetadata> into) throws IOException {
        final ClusterState view = node.clusterService().state();
        if (names == null || names.length == 0 || (names.length == 1 && ("_all".equals(names[0]) || "*".equals(names[0])))) {
            for (IndexMetadata index : view.metadata()) {
                into.add(index);
            }
            return true;
        }
        final Map<String, IndexMetadata> found = new LinkedHashMap<>();
        boolean complete = true;
        for (String name : names) {
            if (name.endsWith("*") && name.indexOf('*') == name.length() - 1) {
                for (String match : plane.namesWithPrefix(name.substring(0, name.length() - 1), WILDCARD_CAP)) {
                    describeOne(plane, match).ifPresent(index -> found.put(match, index));
                }
            } else if (name.contains("*")) {
                throw new IllegalArgumentException("only a trailing wildcard is supported here, got [" + name + "]");
            } else {
                final Optional<IndexMetadata> index = describeOne(plane, name);
                if (index.isPresent()) {
                    found.put(name, index.get());
                } else {
                    complete = false;
                }
            }
        }
        into.addAll(found.values());
        return complete;
    }

    private static Optional<IndexMetadata> describeOne(MetadataPlane plane, String name) throws IOException {
        final Optional<IndexDescriptor> descriptor = plane.describe(name);
        if (descriptor.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(ViewAliases.of(descriptor.get().toIndexMetadata(Map.of()), descriptor.get(), plane));
    }

    /** Health computed by core from a state in which every shard of every found index is started here. */
    private static ClusterHealthResponse healthOf(ServerlessNode node, List<IndexMetadata> found) {
        final ClusterState view = node.clusterService().state();
        final String localId = view.nodes().getLocalNodeId();
        final Metadata.Builder metadata = Metadata.builder();
        final RoutingTable.Builder routing = RoutingTable.builder();
        final String[] names = new String[found.size()];
        for (int i = 0; i < found.size(); i++) {
            // Readers are not replicas in core's sense, and an unassigned replica would turn every answer yellow.
            final IndexMetadata index = IndexMetadata.builder(found.get(i)).numberOfReplicas(0).build();
            names[i] = index.getIndex().getName();
            metadata.put(index, false);
            final IndexRoutingTable.Builder table = IndexRoutingTable.builder(index.getIndex());
            for (int shard = 0; shard < index.getNumberOfShards(); shard++) {
                final ShardId shardId = new ShardId(index.getIndex(), shard);
                table.addIndexShard(
                    new IndexShardRoutingTable.Builder(shardId).addShard(
                        ShardRouting.newUnassigned(
                            shardId,
                            true,
                            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "served on demand")
                        ).initialize(localId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted()
                    ).build()
                );
            }
            routing.add(table.build());
        }
        final ClusterState health = ClusterState.builder(view).metadata(metadata).routingTable(routing.build()).build();
        return new ClusterHealthResponse(view.getClusterName().value(), names, health);
    }

    private static ClusterHealthResponse red(ServerlessNode node, ClusterHealthRequest request, boolean timedOut) {
        final ClusterState view = node.clusterService().state();
        final ClusterHealthResponse response = new ClusterHealthResponse(
            view.getClusterName().value(),
            new String[0],
            ClusterState.builder(view).metadata(Metadata.EMPTY_METADATA).routingTable(RoutingTable.EMPTY_ROUTING_TABLE).build()
        );
        response.setStatus(ClusterHealthStatus.RED);
        response.setTimedOut(timedOut);
        return response;
    }
}
