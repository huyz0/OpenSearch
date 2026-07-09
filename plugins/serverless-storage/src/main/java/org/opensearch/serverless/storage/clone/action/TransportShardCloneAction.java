/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link ShardCloneAction}: builds the same shape of stores {@link
 * ShardCloner#clone} already needs -- exactly as {@code ServerlessStorageLazyDirectoryFactory} and
 * {@code ServerlessStoragePlugin#releaseCloneLineageForDeletedIndex} already do for their own
 * callers -- from {@link ServerlessStoragePlugin#blobContainerForDirectoryFactory}, then delegates.
 *
 * <p>Requires no routing to a specific data node: {@link ShardCloner#clone} operates purely against
 * the shared object store via {@link BlobContainer}s, never against any node-local shard state, so
 * whichever node receives this request can execute it directly -- see {@link RestShardCloneAction}
 * for the {@code executeLocally} call that relies on this.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * {@link ShardCloner#clone} does real blob-store I/O (reads and writes across two shards' worth of
 * manifests, pins, and lineage), which must never block a transport/network thread.
 */
public class TransportShardCloneAction extends HandledTransportAction<ShardCloneRequest, ShardCloneResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    @Inject
    public TransportShardCloneAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(ShardCloneAction.NAME, transportService, actionFilters, ShardCloneRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    @Override
    protected void doExecute(Task task, ShardCloneRequest request, ActionListener<ShardCloneResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                BlobContainer sourceContainer = plugin.blobContainerForDirectoryFactory(request.sourceIndexUuid(), request.sourceShardId());
                BlobContainer targetContainer = plugin.blobContainerForDirectoryFactory(request.targetIndexUuid(), request.targetShardId());

                BlobContainerManifestStore sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
                ShardStateStore sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
                DurablePinRegistry sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);

                BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
                ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
                BlobContainerCloneLineageStore targetLineageStore = new BlobContainerCloneLineageStore(targetContainer);

                ShardCloner.clone(
                    request.sourceIndexUuid(),
                    request.sourceShardId(),
                    sourceManifestStore,
                    sourceShardStateStore,
                    sourcePinRegistry,
                    request.targetIndexUuid(),
                    request.targetShardId(),
                    targetManifestStore,
                    targetShardStateStore,
                    targetLineageStore,
                    threadPool.absoluteTimeInMillis()
                );
                listener.onResponse(new ShardCloneResponse(true));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
