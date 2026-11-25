/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.node;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchService;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.LocalViewProjector;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.shard.ShardReconciler;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Map;

/**
 * Starts {@link NodeContext}: its write log, its own lease, the services it runs and the local view.
 */
public final class NodeStartup {

    private final NodeContext node;

    NodeStartup(NodeContext node) {
        this.node = node;
    }

    /**
     * Starts the node. {@link IndicesService} and {@link SearchService} reject work until started.
     */
    public void start() {
        // Before anything can be sent to this node. A plugin's action is reachable over the transport
        // from the moment its handler registers, which is well before the registry that names it exists
        // -- see pluginActionNames for the ordering. Naming them here is what lets the interceptor
        // demand authentication on the very first one.
        node.beforeAcceptingRequests();

        node.clusterService.start();
        node.indicesService.start();
        node.searchService.start();

        node.transportService.start();
        // Say so, or refuse, when the transport is plaintext. Secure means either the secure netty
        // transport or a plugin that supplies the secure settings a TLS transport is built from.
        final String transportType = NetworkModule.TRANSPORT_TYPE_SETTING.get(node.settings);
        final boolean secureTransport = Netty4ModulePlugin.NETTY_SECURE_TRANSPORT_NAME.equals(transportType)
            || node.services.secureSettingsFactories().isEmpty() == false;
        if (secureTransport == false) {
            if (NodeSettings.REQUIRE_SECURE_TRANSPORT.get(node.settings)) {
                throw new IllegalStateException(
                    "transport.type is ["
                        + transportType
                        + "] and "
                        + NodeSettings.REQUIRE_SECURE_TRANSPORT.getKey()
                        + " is true: forwarded requests would carry the deployment secret in the clear"
                );
            }
            NodeContext.logger.warn(
                "transport.type is [{}]: forwarded requests carry the deployment secret in the clear; install a SecureSettingsFactory"
                    + " plugin or keep the transport port off every reachable network",
                transportType
            );
        }
        node.transportService.acceptIncomingRequests();
        node.httpServerTransport.start();
        // After HTTP, so a client that finds the node over gRPC finds one that already answers REST.
        for (org.opensearch.transport.AuxTransport aux : node.auxTransports) {
            aux.start();
        }

        // The identity in the initial view carried a placeholder address, because the real one is not
        // known until the transport binds. Replace it now that it is.
        node.localNode = node.transportService.getLocalNode();
        node.transportBound();

        // Built here rather than in the constructor: both need the node identity, and the identity is
        // not final until the transport has bound and told us the address it got.
        node.projector = new LocalViewProjector(ClusterName.CLUSTER_NAME_SETTING.get(node.settings), node.localNode);
        node.reconciler = new ShardReconciler(node.indicesService, node.localNode);
        final boolean nodePacksByDefault = WriteSettings.NODE_PACK_ENABLED.get(node.settings);
        node.reconciler.setNodePackPlacement(
            shard -> SegmentPlacement.nodePacks(
                shard.indexSettings().getSettings(),
                node.descriptorRefresh.clusterSegmentPlacement,
                nodePacksByDefault
            )
        );
        node.reconciler.setOnShardFailed((failed, cause) -> {
            // Off the failing thread, which may hold engine locks the release needs.
            try {
                node.threadPool.generic().execute(() -> node.leases.onShardFailed(failed, cause));
            } catch (Exception rejected) {
                NodeContext.logger.warn("Could not schedule failed shard release (error_type={})", rejected.getClass().getSimpleName());
            }
        });
        node.started = true;

        node.afterStarted();
    }

    /**
     * Projects the view this node should hold: every index it serves or hosts, with every open reader
     * and writer shard assigned here, plus whatever is being opened right now.
     *
     * <p>One projection for both roles. The reader path and the writer path each used to project their
     * own half and apply it, so on a dual-role node each open replaced the other's indices in the applied
     * state. Assignments for shards that are no longer open are pruned as they are met, so the record of
     * writer terms follows the reconciler rather than growing with every shard ever held.
     */
    ClusterState projectView(java.util.Collection<IndexDescriptor> extra, java.util.Collection<ShardAssignment> opening)
        throws java.io.IOException {
        final Map<String, IndexDescriptor> descriptors = new java.util.LinkedHashMap<>();
        for (IndexDescriptor descriptor : node.readerServing.served.values()) {
            descriptors.merge(descriptor.name(), descriptor, NodeStartup::newerOf);
        }
        for (IndexDescriptor descriptor : node.descriptorRefresh.hosted.values()) {
            descriptors.merge(descriptor.name(), descriptor, NodeStartup::newerOf);
        }
        for (IndexDescriptor descriptor : node.pluginDescriptors()) {
            descriptors.merge(descriptor.name(), descriptor, NodeStartup::newerOf);
        }
        for (IndexDescriptor descriptor : extra) {
            descriptors.put(descriptor.name(), descriptor);
        }
        final Map<String, org.opensearch.core.index.shard.ShardId> openByKey = new java.util.HashMap<>();
        for (org.opensearch.core.index.shard.ShardId open : node.reconciler.openShards()) {
            openByKey.put(NodeContext.shardKey(open.getIndexName(), open.id()), open);
        }
        final java.util.Set<org.opensearch.core.index.shard.ShardId> readers = node.reconciler.readerShards();
        final Map<String, ShardAssignment> assignments = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : node.readerServing.readerShards) {
            final String key = NodeContext.shardKey(entry.getKey(), entry.getValue());
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(key);
            if (open != null && readers.contains(open)) {
                assignments.put(
                    key,
                    new ShardAssignment(entry.getKey(), entry.getValue(), node.readerServing.readerTerms.getOrDefault(key, 1L))
                );
            }
        }
        for (Map.Entry<String, ShardAssignment> entry : new java.util.ArrayList<>(node.fences.writerAssignments.entrySet())) {
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(entry.getKey());
            if (open != null && readers.contains(open) == false) {
                assignments.put(entry.getKey(), entry.getValue());
            } else if (node.readerServing.openingShards.contains(entry.getKey()) == false
                && opening.stream().noneMatch(a -> NodeContext.shardKey(a.indexName(), a.shardId()).equals(entry.getKey()))) {
                    node.fences.writerAssignments.remove(entry.getKey(), entry.getValue());
                }
        }
        for (ShardAssignment assignment : opening) {
            assignments.put(NodeContext.shardKey(assignment.indexName(), assignment.shardId()), assignment);
        }
        final ClusterState projected = node.projector.project(descriptors.values(), assignments.values());
        return node.withPluginAliases(projected, descriptors.values());
    }

    /** Of two copies of one descriptor, the one with the newer mapping and settings. */
    static IndexDescriptor newerOf(IndexDescriptor a, IndexDescriptor b) {
        if (b.mappingVersion() != a.mappingVersion()) {
            return b.mappingVersion() > a.mappingVersion() ? b : a;
        }
        return b.settingsVersion() >= a.settingsVersion() ? b : a;
    }
}
