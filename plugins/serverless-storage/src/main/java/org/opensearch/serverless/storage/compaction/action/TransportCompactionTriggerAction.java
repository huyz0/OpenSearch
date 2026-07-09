/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.compaction.CompactionPolicy;
import org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor;
import org.opensearch.serverless.storage.compaction.CompactionSchedulerTask;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link CompactionTriggerAction}: builds the same shape of stores {@link
 * CompactionSchedulerTask}'s own background schedule already needs, from {@link
 * ServerlessStoragePlugin#blobContainerForDirectoryFactory} -- exactly as {@link
 * org.opensearch.serverless.storage.clone.action.TransportShardCloneAction} already does for
 * clone -- then delegates to the same {@code maybeCompact} logic the scheduled task uses, so a
 * one-off trigger and the recurring background tick can never drift apart.
 *
 * <p>Requires no routing to a specific data node: the compaction attempt operates purely against
 * the shared object store via {@link BlobContainer}s and the shard's CAS-guarded head, never
 * against any node-local shard state (not even a live writer's lease, which
 * {@code CompactionSchedulerTask}'s own javadoc explains this deliberately does not gate on), so
 * whichever node receives this request can execute it directly.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * a compaction attempt does real blob-store I/O (reads the manifest, potentially opens and merges
 * segments, and CASes a new head), which must never block a transport/network thread.
 */
public class TransportCompactionTriggerAction extends HandledTransportAction<CompactionTriggerRequest, CompactionTriggerResponse> {

    /** Bounded rebase-and-retry budget for a one-off trigger, matching this plugin's other CAS-retry defaults. */
    private static final int MAX_REBASE_ATTEMPTS = 5;

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual compaction attempt off the transport thread.
     */
    @Inject
    public TransportCompactionTriggerAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(CompactionTriggerAction.NAME, transportService, actionFilters, CompactionTriggerRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to attempt compaction on.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, CompactionTriggerRequest request, ActionListener<CompactionTriggerResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                BlobContainer container = plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId());
                BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
                ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
                ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(container));
                ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
                    new BlobContainerBundleStore(container),
                    manifestStore
                );
                CompactionPolicy policy = CompactionPolicy.withDefaults();
                CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, MAX_REBASE_ATTEMPTS);

                boolean attempted = CompactionSchedulerTask.maybeCompact(
                    request.indexUuid(),
                    request.shardId(),
                    shardStateStore,
                    manifestStore,
                    materializer,
                    commitPublisher,
                    policy,
                    rebaseExecutor
                );
                listener.onResponse(new CompactionTriggerResponse(attempted));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
