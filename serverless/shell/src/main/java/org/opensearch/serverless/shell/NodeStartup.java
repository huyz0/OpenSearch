/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

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
 * Starts {@link ServerlessNode}: its write log, its own lease, the services it runs and the local view.
 */
public final class NodeStartup {

    private final ServerlessNode node;

    NodeStartup(ServerlessNode node) {
        this.node = node;
    }

    /**
     * Returns this node's write log, starting it on first use: it reads the epochs an earlier process of this node left in
     * the lease, before anything renews over them.
     */
    NodeLogWriter nodeLog(org.opensearch.serverless.metadata.MetadataPlane plane) throws java.io.IOException {
        final NodeLogWriter started = node.nodeLogWriter;
        if (started != null && node.nodeLogHooked) {
            return started;
        }
        synchronized (node) {
            if (node.nodeLogWriter == null) {
                final NodeLogWriter writer = NodeLogWriter.start(plane, node.localNode.getId(), (epochs, opensEpoch) -> {
                    final long validUntil = node.ownLeaseValidUntilMillis;
                    // Only for a new epoch: a renewal is how a lapsed lease comes back, and refusing it left the node
                    // unable ever to renew again.
                    if (opensEpoch && validUntil != 0L && plane.clock().getAsLong() >= validUntil) {
                        // A lapsed lease is renewed only after this node lets go of what it held; recording an epoch is
                        // no way around that.
                        throw new java.io.IOException("this node's lease has lapsed; no new epoch of its log is recorded");
                    }
                    writeOwnLease(plane, epochs);
                },
                    (shardId, term) -> node.leases.ownsAtTerm(plane, shardId, term),
                    node.settings.getAsLong(
                        "serverless.wal.node_log.window_millis",
                        org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_WINDOW_MILLIS
                    ),
                    node.settings.getAsLong(
                        "serverless.wal.node_log.max_bytes",
                        org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_MAX_BYTES
                    ),
                    node.settings.getAsInt(
                        "serverless.wal.node_log.max_records",
                        org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_MAX_RECORDS
                    )
                );
                final long flushAfter = 2 * node.settings.getAsLong(
                    "serverless.wal.node_log.window_millis",
                    org.opensearch.serverless.store.NodeLogCommitter.DEFAULT_WINDOW_MILLIS
                );
                writer.flushLoggedCommitsWith(
                    task -> node.threadPool()
                        .schedule(
                            task,
                            org.opensearch.common.unit.TimeValue.timeValueMillis(flushAfter),
                            org.opensearch.threadpool.ThreadPool.Names.GENERIC
                        )
                );
                writer.runAfterOpensOn(node.threadPool().generic());
                node.nodeLogWriter = writer;
            }
            if (node.reconciler != null && node.nodeLogHooked == false) {
                node.reconciler.setNodeLog(node.nodeLogWriter);
                node.nodeLogHooked = true;
            }
            return node.nodeLogWriter;
        }
    }

    void writeOwnLease(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        java.util.List<org.opensearch.serverless.store.NodeLogEpoch> nodeLogEpochs
    ) throws java.io.IOException {
        // Stamped from when the renewal began: the lease is valid at least that long after it.
        final long startedAt = plane.clock().getAsLong();
        node.ownLeaseValidUntilMillis = 0L;
        plane.membership()
            .renew(
                new org.opensearch.serverless.membership.NodeLease(
                    node.localNode.getId(),
                    node.localNode.getEphemeralId(),
                    node.localNode.getAddress().toString(),
                    // Less the search role while the node warms or drains, so no coordinator ranks it as a reader.
                    SearchReadiness.of(node).advertised(node.roles),
                    0L,
                    node.localNode.getName(),
                    org.opensearch.Version.CURRENT.toString()
                ).withLoad(
                    node.reconciler == null ? 0 : node.reconciler.heldShards().size(),
                    node.nodeCapacity().admission.maxShardsHeld(),
                    node.nodeCapacity().inUseLastMinute(),
                    node.nodeCapacity().refusedLastMinute()
                ).withNodeLog(nodeLogEpochs).withZone(org.opensearch.serverless.cluster.Zones.localZone(node.settings()))
            );
        node.ownLeaseValidUntilMillis = startedAt + plane.leaseTtlMillis();
    }

    /**
     * Starts the node. {@link IndicesService} and {@link SearchService} reject work until started.
     */
    public void start() {
        // Before anything can be sent to this node. A plugin's action is reachable over the transport
        // from the moment its handler registers, which is well before the registry that names it exists
        // -- see pluginActionNames for the ordering. Naming them here is what lets the interceptor
        // demand authentication on the very first one.
        node.pluginActionNames = node.wiring.declaredPluginActionNames();

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
            if (ServerlessNode.REQUIRE_SECURE_TRANSPORT_SETTING.get(node.settings)) {
                throw new IllegalStateException(
                    "transport.type is ["
                        + transportType
                        + "] and "
                        + ServerlessNode.REQUIRE_SECURE_TRANSPORT_SETTING.getKey()
                        + " is true: forwarded requests would carry the deployment secret in the clear"
                );
            }
            ServerlessNode.logger.warn(
                "transport.type is [{}]: forwarded requests carry the deployment secret in the clear; install a SecureSettingsFactory"
                    + " plugin or keep the transport port off every reachable network",
                transportType
            );
        }
        node.transportService.acceptIncomingRequests();
        node.httpServerTransport.start();

        // The identity in the initial view carried a placeholder address, because the real one is not
        // known until the transport binds. Replace it now that it is.
        node.localNode = node.transportService.getLocalNode();
        node.router = new org.opensearch.serverless.transport.ShardRouter(node, node.transportService, () -> node.metadataPlane);
        node.router.registerHandlers();
        org.opensearch.serverless.transport.CacheWarmCopy.of(node).registerHandlers();
        org.opensearch.serverless.transport.NewestCommits.of(node).registerHandlers();
        node.router.commitPush().registerHandlers();
        node.router.tailPush().registerHandlers();
        node.router.tailOverlays().registerHandlers();

        // Built here rather than in the constructor: both need the node identity, and the identity is
        // not final until the transport has bound and told us the address it got.
        node.projector = new LocalViewProjector(ClusterName.CLUSTER_NAME_SETTING.get(node.settings), node.localNode);
        node.reconciler = new ShardReconciler(node.indicesService, node.localNode);
        final boolean nodePacksByDefault = node.packSegments && node.settings.getAsBoolean(NodePackWiring.ENABLED, false);
        node.reconciler.setNodePackPlacement(
            shard -> node.packSegments
                && SegmentPlacement.nodePacks(shard.indexSettings().getSettings(), node.clusterSegmentPlacement, nodePacksByDefault)
        );
        node.reconciler.setOnShardFailed((failed, cause) -> {
            // Off the failing thread, which may hold engine locks the release needs.
            try {
                node.threadPool.generic().execute(() -> node.leases.onShardFailed(failed, cause));
            } catch (Exception rejected) {
                ServerlessNode.logger.warn("could not schedule the release of failed shard " + failed, rejected);
            }
        });
        node.started = true;

        // Last, and after `started`, because a plugin's createComponents may use the client -- which
        // refuses to work on a node that has not started. Everything a plugin can reach now exists.
        try {
            node.plugins.createComponents(node, new org.opensearch.env.Environment(node.settings, null));
        } catch (Exception e) {
            node.started = false;
            throw new IllegalStateException("a plugin failed to start; the node will not serve", e);
        }

        // After createComponents, because a plugin's action almost always takes one of its own components,
        // and before onNodeStarted, because a plugin told the node is up may immediately call its own
        // action.
        node.pluginActions = node.wiring.buildPluginActions();

        // And then tell them the node is up. A plugin that has to read its own index cannot do it while
        // components are being built; this is the event it waits for.
        node.plugins.onNodeStarted(node.localNode);
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
        for (IndexDescriptor descriptor : node.served.values()) {
            descriptors.merge(descriptor.name(), descriptor, ServerlessNode::newerOf);
        }
        for (IndexDescriptor descriptor : node.hosted.values()) {
            descriptors.merge(descriptor.name(), descriptor, ServerlessNode::newerOf);
        }
        for (IndexDescriptor descriptor : extra) {
            descriptors.put(descriptor.name(), descriptor);
        }
        final Map<String, org.opensearch.core.index.shard.ShardId> openByKey = new java.util.HashMap<>();
        for (org.opensearch.core.index.shard.ShardId open : node.reconciler.openShards()) {
            openByKey.put(ServerlessNode.shardKey(open.getIndexName(), open.id()), open);
        }
        final java.util.Set<org.opensearch.core.index.shard.ShardId> readers = node.reconciler.readerShards();
        final Map<String, ShardAssignment> assignments = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry : node.readerShards) {
            final String key = ServerlessNode.shardKey(entry.getKey(), entry.getValue());
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(key);
            if (open != null && readers.contains(open)) {
                assignments.put(key, new ShardAssignment(entry.getKey(), entry.getValue(), node.readerTerms.getOrDefault(key, 1L)));
            }
        }
        for (Map.Entry<String, ShardAssignment> entry : new java.util.ArrayList<>(node.writerAssignments.entrySet())) {
            final org.opensearch.core.index.shard.ShardId open = openByKey.get(entry.getKey());
            if (open != null && readers.contains(open) == false) {
                assignments.put(entry.getKey(), entry.getValue());
            } else if (node.openingShards.contains(entry.getKey()) == false
                && opening.stream().noneMatch(a -> ServerlessNode.shardKey(a.indexName(), a.shardId()).equals(entry.getKey()))) {
                    node.writerAssignments.remove(entry.getKey(), entry.getValue());
                }
        }
        for (ShardAssignment assignment : opening) {
            assignments.put(ServerlessNode.shardKey(assignment.indexName(), assignment.shardId()), assignment);
        }
        return node.projector.project(descriptors.values(), assignments.values());
    }
}
