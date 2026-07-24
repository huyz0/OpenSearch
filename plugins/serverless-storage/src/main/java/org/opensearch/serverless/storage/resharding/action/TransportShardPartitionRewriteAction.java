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
import org.opensearch.serverless.storage.clone.CloneLineage;
import org.opensearch.serverless.storage.clone.FallbackBundleFileReader;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore;
import org.opensearch.serverless.storage.resharding.PartitionRewritePublisher;
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * The actual work behind {@link ShardPartitionRewriteAction}: builds the same shape of stores
 * {@link org.opensearch.serverless.storage.resharding.PartitionRewritePublisher} needs, from {@link
 * ServerlessStoragePlugin#blobContainerForDirectoryFactory} -- exactly as {@link
 * org.opensearch.serverless.storage.compaction.action.TransportCompactionTriggerAction} already
 * does for its own on-demand trigger -- then delegates.
 *
 * <p>A split target's manifest can reference bundles that still physically live in the
 * <em>source</em> shard's own container -- zero-copy clone/split never copies bundle bytes, only
 * manifest references (see {@link org.opensearch.serverless.storage.clone.ShardCloner}'s own
 * javadoc) -- so the materializer used here reads through the same {@link FallbackBundleFileReader}
 * a cloned/split shard's live reader engine already relies on, resolving the source via the
 * target's own {@link CloneLineage} ({@link org.opensearch.serverless.storage.resharding.ShardSplitter#split}
 * records one for every split target, reusing {@code ShardCloner.clone} unmodified). The rewritten
 * bundle itself is always written back to the target's own container, never the source's.
 *
 * <p>Requires no routing to a specific data node: a rewrite operates purely against the shared
 * object store via {@link BlobContainer}s and the shard's CAS-guarded head, never against any
 * node-local shard state.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly: a
 * rewrite does real blob-store I/O (materializes the full pre-split manifest, writes a filtered
 * bundle, publishes a new manifest, CASes a new head), which must never block a transport/network
 * thread.
 */
public class TransportShardPartitionRewriteAction extends HandledTransportAction<
    ShardPartitionRewriteRequest,
    ShardPartitionRewriteResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual rewrite off the transport thread.
     */
    @Inject
    public TransportShardPartitionRewriteAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(ShardPartitionRewriteAction.NAME, transportService, actionFilters, ShardPartitionRewriteRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to attempt a partition rewrite on.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, ShardPartitionRewriteRequest request, ActionListener<ShardPartitionRewriteResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): the target's
                // own container is NOT delete-denied here -- PartitionRewritePublisher#rewrite's
                // last step (clearDescriptor) genuinely deletes the now-superseded partition
                // descriptor blob as a required part of its own contract (see that class's own
                // javadoc for why that ordering is deliberate), not a defense-in-depth violation to
                // guard against. The clone source below, read only for fallback bundle reads, never
                // deletes and stays wrapped delete-denied.
                BlobContainer container = plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId());
                BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
                BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
                ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
                BlobContainerShardPartitionStore partitionStore = new BlobContainerShardPartitionStore(container);

                // Walks the full clone-lineage chain, not just one hop -- see
                // ShardCloner#resolveLineageChain's own javadoc for why a single-hop fallback isn't
                // enough for a clone (or split target) of an already-cloned shard.
                List<BlobContainer> lineageChain = ShardCloner.resolveLineageChain(
                    container,
                    request.indexUuid(),
                    request.shardId(),
                    (ancestorIndexUuid, ancestorShardId) -> new RestrictingBlobContainer(
                        plugin.blobContainerForDirectoryFactory(ancestorIndexUuid, ancestorShardId),
                        false
                    )
                );
                BundleFileReader readPath = bundleStore;
                if (lineageChain.size() > 1) {
                    List<BundleFileReader> readers = new ArrayList<>(lineageChain.size());
                    readers.add(bundleStore);
                    for (int i = 1; i < lineageChain.size(); i++) {
                        readers.add(new BlobContainerBundleStore(lineageChain.get(i)));
                    }
                    readPath = FallbackBundleFileReader.chain(readers);
                }
                ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(readPath);
                ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

                PartitionRewritePublisher publisher = new PartitionRewritePublisher(
                    request.indexUuid(),
                    request.shardId(),
                    shardStateStore,
                    manifestStore,
                    bundleStore,
                    materializer,
                    commitPublisher,
                    partitionStore
                );
                boolean rewritten = publisher.rewrite();
                listener.onResponse(new ShardPartitionRewriteResponse(rewritten));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
