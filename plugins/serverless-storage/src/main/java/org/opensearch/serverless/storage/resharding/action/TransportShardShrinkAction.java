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
import org.opensearch.serverless.storage.clone.CloneLineage;
import org.opensearch.serverless.storage.clone.FallbackBundleFileReader;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.resharding.ShardShrinker;
import org.opensearch.serverless.storage.resharding.ShardShrinker.ShrinkSource;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The actual work behind {@link ShardShrinkAction}: builds the same shape of stores {@link
 * org.opensearch.serverless.storage.resharding.ShardShrinker} needs for every source shard, from
 * {@link ServerlessStoragePlugin#blobContainerForDirectoryFactory}, then delegates.
 *
 * <p>Each source's materializer reads through a {@link FallbackBundleFileReader} resolved from that
 * source's own {@link CloneLineage}, exactly like {@link TransportShardPartitionRewriteAction}
 * already does for a single target -- a source being shrunk may itself be an unrewritten split
 * target whose manifest still references its own pre-split source's bundles.
 *
 * <p>Requires no routing to a specific data node: the merge operates purely against the shared
 * object store via {@link BlobContainer}s and CAS-guarded heads, never against any node-local
 * shard state.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly: a
 * shrink does real blob-store I/O across every source (materializes each) plus a real Lucene merge,
 * which must never block a transport/network thread.
 */
public class TransportShardShrinkAction extends HandledTransportAction<ShardShrinkRequest, ShardShrinkResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}s.
     * @param threadPool dispatches the actual merge off the transport thread.
     */
    @Inject
    public TransportShardShrinkAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(ShardShrinkAction.NAME, transportService, actionFilters, ShardShrinkRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names every source shard and the target.
     * @param listener notified with the result once the merge (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, ShardShrinkRequest request, ActionListener<ShardShrinkResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                List<ShrinkSource> sources = new ArrayList<>(request.sources().size());
                for (ShardRef sourceRef : request.sources()) {
                    sources.add(resolveShrinkSource(sourceRef));
                }

                BlobContainer targetContainer = plugin.blobContainerForDirectoryFactory(request.targetIndexUuid(), request.targetShardId());
                ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
                ObjectStoreCommitPublisher targetCommitPublisher = new ObjectStoreCommitPublisher(
                    new BlobContainerBundleStore(targetContainer),
                    new BlobContainerManifestStore(targetContainer)
                );

                ShardShrinker.shrink(
                    sources,
                    request.targetIndexUuid(),
                    request.targetShardId(),
                    targetShardStateStore,
                    targetCommitPublisher,
                    threadPool.absoluteTimeInMillis()
                );
                listener.onResponse(new ShardShrinkResponse(true));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private ShrinkSource resolveShrinkSource(ShardRef sourceRef) throws IOException {
        BlobContainer sourceContainer = plugin.blobContainerForDirectoryFactory(sourceRef.indexUuid(), sourceRef.shardId());
        ShardStateStore sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        BlobContainerManifestStore sourceManifestStore = new BlobContainerManifestStore(sourceContainer);

        Optional<VersionedShardHead> sourceHead = sourceShardStateStore.get(sourceRef.indexUuid(), sourceRef.shardId());
        if (sourceHead.isEmpty() || sourceHead.get().head().latestManifestGeneration() == 0) {
            throw new IOException(
                "source shard " + sourceRef.indexUuid() + "/" + sourceRef.shardId() + " has no published manifest to shrink from"
            );
        }
        CommitManifest sourceManifest = sourceManifestStore.readManifest(
            sourceHead.get().head().primaryTerm(),
            sourceHead.get().head().latestManifestGeneration()
        );

        BundleFileReader readPath = new BlobContainerBundleStore(sourceContainer);
        Optional<CloneLineage> lineage = new BlobContainerCloneLineageStore(sourceContainer).readLineage();
        if (lineage.isPresent()) {
            BlobContainer sourceOfSourceContainer = plugin.blobContainerForDirectoryFactory(
                lineage.get().sourceIndexUuid(),
                lineage.get().sourceShardId()
            );
            readPath = new FallbackBundleFileReader(readPath, new BlobContainerBundleStore(sourceOfSourceContainer));
        }

        return new ShrinkSource(sourceManifest, new ObjectStoreCommitMaterializer(readPath));
    }
}
