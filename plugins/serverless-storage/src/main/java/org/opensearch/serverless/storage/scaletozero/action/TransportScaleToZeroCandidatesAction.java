/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.readerengine.action.ShardLagEntry;
import org.opensearch.serverless.storage.writerengine.action.IdleShardEntry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The actual work behind {@link ScaleToZeroCandidatesAction}: fans {@link NodeRequest} out to
 * every data node (each node answering with exactly the same node-local snapshot {@link
 * org.opensearch.serverless.storage.writerengine.action.TransportNodeIdleShardsAction} and {@link
 * org.opensearch.serverless.storage.readerengine.action.TransportNodeManifestLagAction} would each
 * produce, just bundled together), then lets {@link ScaleToZeroCandidatesResponse}'s own
 * constructor merge and threshold-evaluate the results.
 *
 * <p>No I/O on the node-local side, same "safe to answer directly, no need to dispatch off the
 * transport thread" reasoning as those two single-node actions -- {@link
 * ThreadPool.Names#SAME} is used for both the per-node operation and final response assembly.
 */
public class TransportScaleToZeroCandidatesAction extends TransportNodesAction<
    ScaleToZeroCandidatesRequest,
    ScaleToZeroCandidatesResponse,
    TransportScaleToZeroCandidatesAction.NodeRequest,
    NodeScaleToZeroCandidatesResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param threadPool used by {@link TransportNodesAction} to dispatch the fan-out.
     * @param clusterService resolves the cluster's data nodes and name.
     * @param transportService used by {@link TransportNodesAction} to register this action.
     * @param actionFilters applied by {@link TransportNodesAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#shardActivityRegistry()}
     *               and {@link ServerlessStoragePlugin#readerShardActivityRegistry()}, and this
     *               node's configured default thresholds.
     */
    @Inject
    public TransportScaleToZeroCandidatesAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin
    ) {
        super(
            ScaleToZeroCandidatesAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            ScaleToZeroCandidatesRequest::new,
            NodeRequest::new,
            ThreadPool.Names.SAME,
            NodeScaleToZeroCandidatesResponse.class
        );
        this.plugin = plugin;
    }

    /**
     * @param request the original cluster-wide request, carrying any caller-supplied threshold overrides.
     * @param responses every node's raw {@link NodeScaleToZeroCandidatesResponse}.
     * @param failures any per-node failures encountered while fanning out.
     * @return the merged, threshold-evaluated {@link ScaleToZeroCandidatesResponse}.
     */
    @Override
    protected ScaleToZeroCandidatesResponse newResponse(
        ScaleToZeroCandidatesRequest request,
        List<NodeScaleToZeroCandidatesResponse> responses,
        List<FailedNodeException> failures
    ) {
        long idleThresholdMillis = request.idleThresholdMillis() == ScaleToZeroCandidatesRequest.USE_DEFAULT_IDLE_THRESHOLD
            ? plugin.scaleToZeroIdleThresholdMillis()
            : request.idleThresholdMillis();
        long lagThreshold = request.lagThreshold() == ScaleToZeroCandidatesRequest.USE_DEFAULT_LAG_THRESHOLD
            ? plugin.scaleToZeroLagThreshold()
            : request.lagThreshold();
        return new ScaleToZeroCandidatesResponse(clusterService.getClusterName(), responses, failures, idleThresholdMillis, lagThreshold);
    }

    @Override
    protected NodeRequest newNodeRequest(ScaleToZeroCandidatesRequest request) {
        return new NodeRequest();
    }

    @Override
    protected NodeScaleToZeroCandidatesResponse newNodeResponse(StreamInput in) throws IOException {
        return new NodeScaleToZeroCandidatesResponse(in);
    }

    /**
     * @param request unused -- this action takes no per-node parameters.
     * @return this node's raw idle-shard and manifest-lag snapshots, unfiltered and unevaluated.
     */
    @Override
    protected NodeScaleToZeroCandidatesResponse nodeOperation(NodeRequest request) {
        Map<String, Long> idleSnapshot = plugin.shardActivityRegistry().snapshotAll();
        List<IdleShardEntry> idleEntries = new ArrayList<>(idleSnapshot.size());
        for (Map.Entry<String, Long> entry : idleSnapshot.entrySet()) {
            idleEntries.add(toIdleShardEntry(entry));
        }

        Map<String, Long> lagSnapshot = plugin.readerShardActivityRegistry().snapshotAll();
        List<ShardLagEntry> lagEntries = new ArrayList<>(lagSnapshot.size());
        for (Map.Entry<String, Long> entry : lagSnapshot.entrySet()) {
            lagEntries.add(toShardLagEntry(entry));
        }

        Map<String, Long> readerIdleSnapshot = plugin.readerShardActivityRegistry().snapshotQueryIdleMillis();
        List<IdleShardEntry> readerIdleEntries = new ArrayList<>(readerIdleSnapshot.size());
        for (Map.Entry<String, Long> entry : readerIdleSnapshot.entrySet()) {
            readerIdleEntries.add(toIdleShardEntry(entry));
        }

        return new NodeScaleToZeroCandidatesResponse(clusterService.localNode(), idleEntries, lagEntries, readerIdleEntries);
    }

    private static IdleShardEntry toIdleShardEntry(Map.Entry<String, Long> entry) {
        int separator = entry.getKey().lastIndexOf('/');
        String indexUuid = entry.getKey().substring(0, separator);
        int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
        return new IdleShardEntry(indexUuid, shardId, entry.getValue());
    }

    private static ShardLagEntry toShardLagEntry(Map.Entry<String, Long> entry) {
        int separator = entry.getKey().lastIndexOf('/');
        String indexUuid = entry.getKey().substring(0, separator);
        int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
        return new ShardLagEntry(indexUuid, shardId, entry.getValue());
    }

    /**
     * Inner node request; carries no fields of its own since {@link #nodeOperation} needs nothing
     * beyond the receiving node's own local registries.
     */
    public static class NodeRequest extends TransportRequest {

        /**
         * Deserializes an (empty) node request.
         *
         * @param in stream positioned at a previously-{@link #writeTo}-written (empty) {@link NodeRequest}.
         */
        public NodeRequest(StreamInput in) throws IOException {
            super(in);
        }

        /** Creates an (empty) node request. */
        NodeRequest() {}

        /** @param out stream to write this (empty) request to. */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
