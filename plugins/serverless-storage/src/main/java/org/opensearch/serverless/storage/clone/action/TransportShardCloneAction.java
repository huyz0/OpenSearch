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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
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
 *
 * <p><b>Validates both shards are real, already-provisioned OpenSearch shards before touching the
 * object store</b>, exactly as {@code TransportShardSplitAction} already does for the split it is
 * modelled on -- see {@link #requireRealShard}. As there, this still does not create or allocate the
 * target shard: an operator (or a future controller) must create the real target index/shard first,
 * exactly as before. It only turns "silently write object-store state nothing can ever open" -- and,
 * worse, "write it wherever the caller's string happens to point" -- into a clear, immediate error.
 */
public class TransportShardCloneAction extends HandledTransportAction<ShardCloneRequest, ShardCloneResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    // A real Guice singleton (see this constructor's own @Inject), so doExecute can use this
    // concurrently across different in-flight requests -- IndexMetadataUuidIndex's own volatile-field
    // cache is documented safe under exactly that access pattern.
    private final IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's source/target {@link BlobContainer}s.
     * @param threadPool dispatches the actual clone work off the transport thread.
     * @param clusterService resolves whether the source/target index/shard pairs are real.
     */
    @Inject
    public TransportShardCloneAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(ShardCloneAction.NAME, transportService, actionFilters, ShardCloneRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * Resolves {@code indexUuid} to a real index in {@code metadata}, and {@code shardId} to a real
     * shard of it, or throws.
     *
     * <p>{@link ServerlessStoragePlugin#blobContainerForDirectoryFactory} builds a blob path out of
     * whatever string it is handed ({@code BlobPath.cleanPath().add(indexUuid)}), and the underlying
     * store resolves path segments without normalising them -- so an unvalidated uuid is a path the
     * caller chooses, not a shard the caller owns: {@code ".."} segments escape the repository base
     * on a filesystem repository, and on an object store a real-but-unrelated uuid simply reaches
     * another index's shards. Resolving through cluster metadata makes the check "this names a shard
     * that exists" rather than the far weaker "this string looks harmless". The same resolution, and
     * the same {@code role}-prefixed error-message shape, as {@code
     * TransportShardSplitAction#requireRealShard}.
     *
     * <p>The shard bound accepts an id at or beyond {@code getNumberOfShards()} when the index's
     * split metadata knows it: an in-place split reserves child shard ids past the base range
     * <em>without</em> growing {@code number_of_shards} (see {@code
     * IndexMetadata#inSyncAllocationIds}), and a split child is a perfectly ordinary clone source.
     *
     * @throws IllegalArgumentException if {@code indexUuid} doesn't resolve to a real,
     *                                  currently-existing index, or {@code shardId} isn't one of its
     *                                  shards.
     */
    // Visible for testing: pure metadata resolution, exercisable without a plugin or a blob store.
    static void requireRealShard(IndexMetadataUuidIndex uuidIndex, Metadata metadata, String role, String indexUuid, int shardId) {
        IndexMetadata indexMetadata = uuidIndex.findByUuid(metadata, indexUuid);
        if (indexMetadata == null) {
            throw new IllegalArgumentException(role + " index [" + indexUuid + "] does not exist");
        }
        if (shardId >= indexMetadata.getNumberOfShards() && indexMetadata.getSplitShardsMetadata().getRangeOfShard(shardId) == null) {
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

    /**
     * @param task the task tracking this request, unused.
     * @param request names the source and target shard to clone.
     * @param listener notified with the result once the clone (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, ShardCloneRequest request, ActionListener<ShardCloneResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                Metadata metadata = clusterService.state().metadata();
                requireRealShard(uuidIndex, metadata, "source", request.sourceIndexUuid(), request.sourceShardId());
                requireRealShard(uuidIndex, metadata, "target", request.targetIndexUuid(), request.targetShardId());

                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): this action
                // never deletes anything -- ShardCloner#clone only reads/writes -- so both
                // containers are wrapped delete-denied, same defense-in-depth shape already applied
                // at ServerlessStoragePlugin#getEngineFactory's own construction seam.
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

                // Cloning a clone is supported, and the bytes a clone reads live wherever the chain
                // physically stores them -- not necessarily on the immediate source, which for a pure clone
                // holds no bundles at all. Handing the cloner a resolver lets it pin every hop of the
                // source's lineage, so deleting a middle index cannot unpin the shard that actually holds
                // this clone's segments. Delete-denied, same as the two containers above: pinning is a
                // register mutation, never a delete.
                ShardCloner.ContainerResolver ancestorResolver = (indexUuid, shardId) -> new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(indexUuid, shardId),
                    false
                );

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
                    threadPool.absoluteTimeInMillis(),
                    null,
                    null,
                    ancestorResolver
                );
                listener.onResponse(new ShardCloneResponse(true));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
