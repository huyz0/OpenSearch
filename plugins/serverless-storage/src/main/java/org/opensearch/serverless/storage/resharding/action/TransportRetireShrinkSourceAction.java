/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata;
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Optional;

/**
 * The actual work behind {@link RetireShrinkSourceAction} -- see that class's own javadoc for why
 * this is a deliberate, verified, opt-in retirement rather than a bare {@code DELETE /source-index}.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * verifying the target's published head does real blob-store I/O, which must never block a
 * transport/network thread.
 */
public class TransportRetireShrinkSourceAction extends HandledTransportAction<RetireShrinkSourceRequest, AcknowledgedResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ClusterService clusterService;
    private final Client client;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves the shrink target's {@link BlobContainer}.
     * @param clusterService reads the source index's real settings.
     * @param client dispatches the actual index deletion, once verified safe.
     * @param threadPool dispatches the verification work off the transport thread.
     */
    @Inject
    public TransportRetireShrinkSourceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ClusterService clusterService,
        Client client,
        ThreadPool threadPool
    ) {
        super(RetireShrinkSourceAction.NAME, transportService, actionFilters, RetireShrinkSourceRequest::new);
        this.plugin = plugin;
        this.clusterService = clusterService;
        this.client = client;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the source to retire and the shrink target it claims to have been merged into.
     * @param listener notified with the deletion result once verification (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, RetireShrinkSourceRequest request, ActionListener<AcknowledgedResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                IndexMetadata sourceMetadata = clusterService.state().metadata().index(request.sourceIndexName());
                if (sourceMetadata == null) {
                    listener.onFailure(new IllegalArgumentException("source index [" + request.sourceIndexName() + "] does not exist"));
                    return;
                }
                if (ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.get(sourceMetadata.getSettings()) == false) {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "index ["
                                + request.sourceIndexName()
                                + "] is not a serverless-storage index -- this action only "
                                + "retires serverless-storage shrink sources, never an ordinary index"
                        )
                    );
                    return;
                }
                if (SourceSplitFenceMetadata.isFencedSource(sourceMetadata) == false && request.acknowledgeUnfencedSource() == false) {
                    listener.onFailure(
                        new IllegalStateException(
                            "source ["
                                + request.sourceIndexName()
                                + "] is not fenced against direct writes (see FenceSplitSourceAction) -- retiring it now "
                                + "could permanently delete data still being written directly to it if this is actually an "
                                + "unfenced resharding-by-copy split source. If this is genuinely a shrink source (never "
                                + "part of a split) and you have already confirmed writes have stopped, retry with "
                                + "acknowledgeUnfencedSource=true"
                        )
                    );
                    return;
                }

                BlobContainer targetContainer = new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(request.targetIndexUuid(), request.targetShardId()),
                    false
                );
                ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
                Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> targetHead = targetShardStateStore.get(
                    request.targetIndexUuid(),
                    request.targetShardId()
                );
                if (targetHead.isEmpty() || targetHead.get().head().latestManifestGeneration() == 0) {
                    listener.onFailure(
                        new IllegalStateException(
                            "shrink target "
                                + request.targetIndexUuid()
                                + "/"
                                + request.targetShardId()
                                + " has no published manifest yet -- refusing to retire source ["
                                + request.sourceIndexName()
                                + "] before its shrink target genuinely exists"
                        )
                    );
                    return;
                }

                client.admin()
                    .indices()
                    .delete(
                        new DeleteIndexRequest(request.sourceIndexName()),
                        ActionListener.wrap(
                            deleteResponse -> listener.onResponse(new AcknowledgedResponse(deleteResponse.isAcknowledged())),
                            listener::onFailure
                        )
                    );
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
