/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataInPlaceSplitShardService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;

/**
 * Transport action for {@link InPlaceSplitShardAction}: closes core's previously-missing public
 * API for {@link MetadataInPlaceSplitShardService} -- see that class's own javadoc for what the
 * split itself actually does; this action is only the reachability seam.
 *
 * <p>Before submitting the split's cluster-state update, forces a real flush of the source index
 * (dynamic-partitioning-progress.md's "Task 18": {@code ShardCloner} (in the serverless-storage
 * plugin), reused verbatim by a child's {@code Engine#recoverFromInPlaceSplit}, clones
 * whatever the parent's latest <em>published</em> manifest generation happens to be at the moment
 * of cloning -- without a preceding flush, writes acknowledged to a client after the parent's last
 * publish but before the split would be silently absent from every child). This flushes the whole
 * index rather than just the target shard (no shard-scoped {@link FlushRequest} variant exists),
 * a correct but coarser-than-ideal mitigation -- narrowing it to just the target shard is separate,
 * still-open follow-up work, not attempted here.
 *
 * @opensearch.internal
 */
public class TransportInPlaceSplitShardAction extends TransportClusterManagerNodeAction<
    InPlaceSplitShardAction.Request,
    AcknowledgedResponse> {

    private final MetadataInPlaceSplitShardService metadataInPlaceSplitShardService;
    private final Client client;

    @Inject
    public TransportInPlaceSplitShardAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataInPlaceSplitShardService metadataInPlaceSplitShardService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            InPlaceSplitShardAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            InPlaceSplitShardAction.Request::new,
            indexNameExpressionResolver
        );
        this.metadataInPlaceSplitShardService = metadataInPlaceSplitShardService;
        this.client = client;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(InPlaceSplitShardAction.Request request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void clusterManagerOperation(
        final InPlaceSplitShardAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        client.admin()
            .indices()
            .flush(
                new FlushRequest(request.index()),
                ActionListener.wrap((FlushResponse flushResponse) -> submitSplit(request, listener), listener::onFailure)
            );
    }

    private void submitSplit(InPlaceSplitShardAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        InPlaceSplitShardClusterStateUpdateRequest updateRequest = new InPlaceSplitShardClusterStateUpdateRequest(
            "in-place split via REST/transport action",
            request.index(),
            request.shardId(),
            request.splitInto()
        );
        updateRequest.ackTimeout(request.ackTimeout());
        updateRequest.clusterManagerNodeTimeout(request.clusterManagerNodeTimeout());

        metadataInPlaceSplitShardService.split(
            updateRequest,
            ActionListener.map(listener, (ClusterStateUpdateResponse response) -> new AcknowledgedResponse(response.isAcknowledged()))
        );
    }
}
