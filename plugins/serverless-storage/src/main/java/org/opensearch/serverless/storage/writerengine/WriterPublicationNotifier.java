/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.TransportActionNodeProxy;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.readerengine.action.PollNowAction;
import org.opensearch.serverless.storage.readerengine.action.PollNowRequest;
import org.opensearch.serverless.storage.readerengine.action.PollNowResponse;
import org.opensearch.transport.TransportService;

/**
 * The writer-side half of rfc-serverless-opensearch.md &sect;8's publication notification
 * mechanism, closing the gap that section's own status note used to describe: {@code
 * ObjectStoreReaderEngine#pollNow}/{@code PollNowAction} (the receiving side) already let a caller
 * force a specific reader engine to check for a newer manifest generation immediately, but nothing
 * called it automatically after a real publish.
 *
 * <p>Reader locations come from the routing table (the RFC's own "in early phases, from the routing
 * table (classic control plane)" option) via {@link IndexShardRoutingTable#searchOnlyReplicas()},
 * not the directory tier's {@code ShardDirectory} entry -- that entry is deliberately a single soft
 * hint per shard (see its own javadoc), so it could only ever notify one reader copy even when a
 * shard has several. The routing table already enumerates every currently-assigned copy.
 *
 * <p>Each notified node gets an actual network RPC via {@link TransportActionNodeProxy}, not a
 * plain {@link org.opensearch.transport.client.Client#execute} call: {@link
 * org.opensearch.serverless.storage.readerengine.action.TransportPollNowAction} only ever checks
 * the *local* node's own reader registry, so reaching a specific remote node requires the same
 * transport-level, {@link DiscoveryNode}-targeted dispatch core itself uses for other single-node
 * actions (see {@link TransportActionNodeProxy}'s own javadoc).
 *
 * <p>Purely an optimization, matching this section's own "notifications are an optimization... the
 * object store is the truth" framing: every failure here (a node not found, a request that never
 * lands, an exception resolving the routing table) is logged and swallowed, never surfaced to the
 * publish that triggered it -- {@code waitForGeneration} and the reader's own background poll
 * schedule both already converge correctly with nobody ever calling this.
 */
public final class WriterPublicationNotifier {

    private static final Logger logger = LogManager.getLogger(WriterPublicationNotifier.class);

    private final ClusterService clusterService;
    private final TransportActionNodeProxy<PollNowRequest, PollNowResponse> pollNowProxy;

    /**
     * Creates a notifier.
     *
     * @param settings node settings, used only to resolve {@link PollNowAction}'s transport options.
     * @param transportService used to dispatch the node-targeted {@link PollNowAction} request.
     * @param clusterService supplies the cluster state used to resolve reader locations per shard.
     */
    public WriterPublicationNotifier(Settings settings, TransportService transportService, ClusterService clusterService) {
        this.clusterService = clusterService;
        this.pollNowProxy = new TransportActionNodeProxy<>(settings, PollNowAction.INSTANCE, transportService);
    }

    /**
     * Notifies every currently-assigned search-only replica of {@code (indexUuid, shardId)} to poll
     * for a newer manifest generation now, fire-and-forget. A no-op if the index can't be resolved
     * (e.g. deleted concurrently with this call) or has no assigned reader copies right now.
     *
     * @param indexUuid UUID of the index the just-published shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     */
    public void notifyReaders(String indexUuid, int shardId) {
        ClusterState state = clusterService.state();
        IndexMetadata indexMetadata = findByUuid(state.metadata(), indexUuid);
        if (indexMetadata == null) {
            return;
        }
        String indexName = indexMetadata.getIndex().getName();
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexName);
        if (indexRoutingTable == null) {
            return;
        }
        IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
        if (shardRoutingTable == null) {
            return;
        }
        for (ShardRouting shardRouting : shardRoutingTable.searchOnlyReplicas()) {
            if (shardRouting.unassigned()) {
                continue;
            }
            DiscoveryNode node = state.nodes().get(shardRouting.currentNodeId());
            if (node == null) {
                continue;
            }
            pollNowProxy.execute(
                node,
                new PollNowRequest(indexUuid, shardId),
                ActionListener.wrap(
                    response -> {},
                    e -> logger.warn(
                        "failed to notify reader on node ["
                            + node.getId()
                            + "] of new publication for shard ["
                            + indexUuid
                            + "]["
                            + shardId
                            + "]",
                        e
                    )
                )
            );
        }
    }

    private static IndexMetadata findByUuid(Metadata metadata, String indexUuid) {
        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            if (indexUuid.equals(indexMetadata.getIndexUUID())) {
                return indexMetadata;
            }
        }
        return null;
    }
}
