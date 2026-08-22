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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link SnapshotReleaseAction}: a single {@link
 * org.opensearch.serverless.storage.retention.DurablePinRegistry#removePin(String, int, String)}
 * call, removing every pin under the request's {@code snapshotId} -- safe because one snapshot
 * pins exactly one generation per shard (unlike PITR, which pins several generations under the
 * same reason and needs the narrower {@code removePin(String, int, PinRecord)} overload instead).
 *
 * <p>Same dispatch shape as {@link TransportSnapshotPinAction}: no specific-node routing needed
 * (pure object-store I/O against the shard's own container), dispatched onto {@link
 * ThreadPool.Names#GENERIC} rather than the transport thread -- and the same {@code
 * TransportSnapshotPinAction#requireRealShard} guard on the way in, for the same reason: an
 * unvalidated {@code indexUuid} becomes a blob path the caller chose rather than a shard the caller
 * owns.
 */
public class TransportSnapshotReleaseAction extends HandledTransportAction<SnapshotReleaseRequest, SnapshotReleaseResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual release attempt off the transport thread.
     * @param clusterService resolves whether the requested {@code (indexUuid, shardId)} is a real shard.
     */
    @Inject
    public TransportSnapshotReleaseAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(SnapshotReleaseAction.NAME, transportService, actionFilters, SnapshotReleaseRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard and snapshot to release.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, SnapshotReleaseRequest request, ActionListener<SnapshotReleaseResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                TransportSnapshotPinAction.requireRealShard(
                    uuidIndex,
                    clusterService.state().metadata(),
                    request.indexUuid(),
                    request.shardId()
                );

                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): removePin is
                // a CAS-based mutate, not a raw blob delete, so this container is wrapped
                // delete-denied.
                BlobContainer container = new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId()),
                    false
                );
                BlobContainerDurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);
                pinRegistry.removePin(request.indexUuid(), request.shardId(), request.snapshotId());
                listener.onResponse(new SnapshotReleaseResponse(true));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
