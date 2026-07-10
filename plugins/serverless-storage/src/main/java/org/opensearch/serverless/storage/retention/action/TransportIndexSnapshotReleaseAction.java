/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * The actual work behind {@link IndexSnapshotReleaseAction}: resolves the index's UUID and shard
 * count from cluster state, then releases {@code snapshotId} from each shard in turn via {@link
 * SnapshotReleaseAction}. See {@link IndexSnapshotReleaseAction}'s own javadoc for why this needs
 * no rollback, unlike {@link TransportIndexSnapshotPinAction}.
 */
public class TransportIndexSnapshotReleaseAction extends HandledTransportAction<IndexSnapshotReleaseRequest, IndexSnapshotReleaseResponse> {

    private final ClusterService clusterService;
    private final NodeClient client;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the request's index name to a UUID and shard count.
     * @param client dispatches each shard's {@link SnapshotReleaseAction} call.
     */
    @Inject
    public TransportIndexSnapshotReleaseAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NodeClient client
    ) {
        super(IndexSnapshotReleaseAction.NAME, transportService, actionFilters, IndexSnapshotReleaseRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the index and snapshot to release.
     * @param listener notified with the result once every shard has released the pin, or with the
     *                 first failure encountered.
     */
    @Override
    protected void doExecute(Task task, IndexSnapshotReleaseRequest request, ActionListener<IndexSnapshotReleaseResponse> listener) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(request.indexName());
        if (indexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(request.indexName()));
            return;
        }
        releaseShard(indexMetadata.getIndexUUID(), indexMetadata.getNumberOfShards(), 0, request.snapshotId(), listener);
    }

    private void releaseShard(
        String indexUuid,
        int numberOfShards,
        int shardId,
        String snapshotId,
        ActionListener<IndexSnapshotReleaseResponse> listener
    ) {
        if (shardId == numberOfShards) {
            listener.onResponse(new IndexSnapshotReleaseResponse(numberOfShards));
            return;
        }
        client.execute(
            SnapshotReleaseAction.INSTANCE,
            new SnapshotReleaseRequest(indexUuid, shardId, snapshotId),
            ActionListener.wrap(response -> releaseShard(indexUuid, numberOfShards, shardId + 1, snapshotId, listener), listener::onFailure)
        );
    }
}
