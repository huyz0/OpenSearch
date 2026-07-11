/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore;
import org.opensearch.serverless.storage.resharding.ShardSplitter;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link ShardSplitAction}: builds the same shape of stores {@link
 * org.opensearch.serverless.storage.clone.action.TransportShardCloneAction} already needs, from
 * {@link ServerlessStoragePlugin#blobContainerForDirectoryFactory}, then delegates to {@link
 * ShardSplitter#split}.
 *
 * <p>Requires no routing to a specific data node, same reasoning as {@code
 * TransportShardCloneAction}: {@link ShardSplitter#split} operates purely against the shared
 * object store via {@link BlobContainer}s, never against any node-local shard state.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * real blob-store I/O across two shards' worth of manifests, pins, lineage, and the new partition
 * descriptor, which must never block a transport/network thread.
 */
public class TransportShardSplitAction extends HandledTransportAction<ShardSplitRequest, ShardSplitResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's source/target {@link BlobContainer}s.
     * @param threadPool dispatches the actual split work off the transport thread.
     */
    @Inject
    public TransportShardSplitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(ShardSplitAction.NAME, transportService, actionFilters, ShardSplitRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the source and target shard, and the target's partition assignment.
     * @param listener notified with the result once the split (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, ShardSplitRequest request, ActionListener<ShardSplitResponse> listener) {
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
                BlobContainerShardPartitionStore targetPartitionStore = new BlobContainerShardPartitionStore(targetContainer);

                ShardSplitter.split(
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
                    targetPartitionStore,
                    request.partitionIndex(),
                    request.numPartitions(),
                    threadPool.absoluteTimeInMillis()
                );
                listener.onResponse(new ShardSplitResponse(true));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
