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
import org.opensearch.cluster.metadata.MetadataInPlaceMergeShardService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.io.IOException;

/**
 * Transport action for {@link InPlaceMergeShardAction}: the reachability seam for
 * {@link MetadataInPlaceMergeShardService} -- see that class's own javadoc for what the merge itself
 * does; this action only reaches it.
 *
 * <p>Before submitting the merge's cluster-state update, forces a real flush of the index -- the
 * merge symmetric of the split's own pre-flush (dynamic-partitioning-progress.md's "Task 18"). The
 * engine hook that revives the parent folds each child's latest <em>published</em> manifest back
 * together; a child's writes acknowledged to a client but not yet published would otherwise be
 * silently dropped by the merge. Flushes the whole index (no shard-scoped {@link FlushRequest} variant
 * exists), the same correct-but-coarser mitigation the split action uses.
 *
 * <p>Precondition violations surface cleanly: {@link MetadataInPlaceMergeShardService} validates the
 * merge (parent really is a committed split parent, no child split further, children live and started)
 * and throws {@link IllegalArgumentException}, which the cluster-state task machinery routes to this
 * action's listener and the REST layer maps to a 400, not a raw 500.
 *
 * @opensearch.internal
 */
public class TransportInPlaceMergeShardAction extends TransportClusterManagerNodeAction<
    InPlaceMergeShardAction.Request,
    AcknowledgedResponse> {

    private final MetadataInPlaceMergeShardService metadataInPlaceMergeShardService;
    private final Client client;

    @Inject
    public TransportInPlaceMergeShardAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataInPlaceMergeShardService metadataInPlaceMergeShardService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Client client
    ) {
        super(
            InPlaceMergeShardAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            InPlaceMergeShardAction.Request::new,
            indexNameExpressionResolver
        );
        this.metadataInPlaceMergeShardService = metadataInPlaceMergeShardService;
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
    protected ClusterBlockException checkBlock(InPlaceMergeShardAction.Request request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void clusterManagerOperation(
        final InPlaceMergeShardAction.Request request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        client.admin()
            .indices()
            .flush(
                new FlushRequest(request.index()),
                ActionListener.wrap((FlushResponse flushResponse) -> submitMerge(request, listener), listener::onFailure)
            );
    }

    private void submitMerge(InPlaceMergeShardAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        InPlaceMergeShardClusterStateUpdateRequest updateRequest = new InPlaceMergeShardClusterStateUpdateRequest(
            "in-place merge via REST/transport action",
            request.index(),
            request.parentShardId()
        );
        updateRequest.ackTimeout(request.ackTimeout());
        updateRequest.clusterManagerNodeTimeout(request.clusterManagerNodeTimeout());

        metadataInPlaceMergeShardService.merge(
            updateRequest,
            ActionListener.map(listener, (ClusterStateUpdateResponse response) -> new AcknowledgedResponse(response.isAcknowledged()))
        );
    }
}
