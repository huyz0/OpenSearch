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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
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
 *
 * <p><b>Validates both shards are real, already-provisioned OpenSearch shards before touching the
 * object store</b> (rfc-serverless-opensearch.md &sect;16 Phase 4's own gap note: {@code
 * ShardSplitter#split} "does not create or allocate the target shard"). Neither
 * {@code (indexUuid, shardId)} pair used to be checked against anything -- {@link
 * ServerlessStoragePlugin#blobContainerForDirectoryFactory} resolves a container from any string
 * whatsoever, so a typo'd or not-yet-created target index used to silently "succeed" at writing
 * object-store state nothing could ever open, only failing (confusingly) much later when a real
 * shard tried to activate against it. This still does not automatically create or allocate the
 * target shard -- an operator (or a future controller) must create the real target index/shard
 * first, exactly as before -- it only turns "silently write into the void" into a clear, immediate
 * error when that prerequisite wasn't met. Real routing cutover (retiring the source, redirecting
 * client-facing requests to whichever target partition now owns their documents) remains
 * undesigned, not just unimplemented -- there is no existing alias/redirect mechanism anywhere in
 * this plugin to build it on, unlike the delete-ratio or disk-cache gaps closed elsewhere this
 * session, which had a clear mechanism to extend.
 */
public class TransportShardSplitAction extends HandledTransportAction<ShardSplitRequest, ShardSplitResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    // H1d: a real Guice singleton, same concurrent-access reasoning as TransportMigrateShardAction's
    // own copy of this field.
    private final IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's source/target {@link BlobContainer}s.
     * @param threadPool dispatches the actual split work off the transport thread.
     * @param clusterService resolves whether the source/target index/shard pairs are real.
     */
    @Inject
    public TransportShardSplitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(ShardSplitAction.NAME, transportService, actionFilters, ShardSplitRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
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
                Metadata metadata = clusterService.state().metadata();
                requireRealShard(metadata, "source", request.sourceIndexUuid(), request.sourceShardId());
                requireRealShard(metadata, "target", request.targetIndexUuid(), request.targetShardId());

                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): a split
                // never deletes anything -- ShardSplitter#split only pins the source and
                // publishes the target -- so both containers are wrapped delete-denied.
                BlobContainer sourceContainer = new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(request.sourceIndexUuid(), request.sourceShardId()),
                    false
                );
                BlobContainer targetContainer = new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(request.targetIndexUuid(), request.targetShardId()),
                    false
                );

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

    /**
     * @throws IllegalArgumentException if {@code indexUuid} doesn't resolve to a real, currently-existing
     *                                   index, or {@code shardId} is out of bounds for it.
     */
    private void requireRealShard(Metadata metadata, String role, String indexUuid, int shardId) {
        IndexMetadata indexMetadata = uuidIndex.findByUuid(metadata, indexUuid);
        if (indexMetadata == null) {
            throw new IllegalArgumentException(role + " index [" + indexUuid + "] does not exist");
        }
        if (shardId >= indexMetadata.getNumberOfShards()) {
            throw new IllegalArgumentException(
                role
                    + " shard ["
                    + shardId
                    + "] is out of bounds for index ["
                    + indexMetadata.getIndex().getName()
                    + "]["
                    + indexUuid
                    + "], which has "
                    + indexMetadata.getNumberOfShards()
                    + " shard(s)"
            );
        }
    }
}
