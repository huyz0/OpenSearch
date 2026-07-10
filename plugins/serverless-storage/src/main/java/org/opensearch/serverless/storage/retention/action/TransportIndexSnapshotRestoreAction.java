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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.Set;

/**
 * The actual work behind {@link IndexSnapshotRestoreAction}: a read-only validation pass across
 * every shard (does {@code snapshotId} actually name a pin there?), then, only if every shard
 * passes, a sequential restore pass via {@link SnapshotRestoreAction} per shard. See {@link
 * IndexSnapshotRestoreAction}'s own javadoc for exactly what guarantee this two-pass shape does
 * and does not provide.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC} for the validation pass (real blob-store
 * reads); the restore pass itself needs no dispatch of its own since {@link SnapshotRestoreAction}
 * is already dispatched by {@link TransportSnapshotRestoreAction}.
 */
public class TransportIndexSnapshotRestoreAction extends HandledTransportAction<IndexSnapshotRestoreRequest, IndexSnapshotRestoreResponse> {

    private final ClusterService clusterService;
    private final ServerlessStoragePlugin plugin;
    private final NodeClient client;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the request's index name to a UUID and shard count.
     * @param plugin resolves each shard's {@link BlobContainer} for the validation pass.
     * @param client dispatches each shard's {@link SnapshotRestoreAction} call for the restore pass.
     * @param threadPool dispatches the validation pass off the transport thread.
     */
    @Inject
    public TransportIndexSnapshotRestoreAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ServerlessStoragePlugin plugin,
        NodeClient client,
        ThreadPool threadPool
    ) {
        super(IndexSnapshotRestoreAction.NAME, transportService, actionFilters, IndexSnapshotRestoreRequest::new);
        this.clusterService = clusterService;
        this.plugin = plugin;
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the index and snapshot to restore from.
     * @param listener notified with the result once every shard is restored, or with the
     *                 validation/restore failure encountered.
     */
    @Override
    protected void doExecute(Task task, IndexSnapshotRestoreRequest request, ActionListener<IndexSnapshotRestoreResponse> listener) {
        IndexMetadata indexMetadata = clusterService.state().metadata().index(request.indexName());
        if (indexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(request.indexName()));
            return;
        }
        String indexUuid = indexMetadata.getIndexUUID();
        int numberOfShards = indexMetadata.getNumberOfShards();

        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                for (int shardId = 0; shardId < numberOfShards; shardId++) {
                    BlobContainer container = plugin.blobContainerForDirectoryFactory(indexUuid, shardId);
                    Set<PinRecord> pins = new BlobContainerDurablePinRegistry(container).getPins(indexUuid, shardId);
                    boolean pinned = pins.stream().anyMatch(pin -> pin.pinId().equals(request.snapshotId()));
                    if (pinned == false) {
                        listener.onFailure(
                            new IllegalStateException(
                                "no snapshot ["
                                    + request.snapshotId()
                                    + "] pinned on shard ["
                                    + indexUuid
                                    + "/"
                                    + shardId
                                    + "]; refusing to restore any shard of ["
                                    + request.indexName()
                                    + "] until every shard has a matching pin"
                            )
                        );
                        return;
                    }
                }
                restoreShard(indexUuid, numberOfShards, 0, request.snapshotId(), listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void restoreShard(
        String indexUuid,
        int numberOfShards,
        int shardId,
        String snapshotId,
        ActionListener<IndexSnapshotRestoreResponse> listener
    ) {
        if (shardId == numberOfShards) {
            listener.onResponse(new IndexSnapshotRestoreResponse(numberOfShards));
            return;
        }
        client.execute(
            SnapshotRestoreAction.INSTANCE,
            new SnapshotRestoreRequest(indexUuid, shardId, snapshotId),
            ActionListener.wrap(response -> restoreShard(indexUuid, numberOfShards, shardId + 1, snapshotId, listener), listener::onFailure)
        );
    }
}
