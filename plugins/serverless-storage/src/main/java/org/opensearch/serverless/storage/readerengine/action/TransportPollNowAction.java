/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link PollNowAction}: looks up the receiving node's own {@link
 * ServerlessStoragePlugin#readerShardActivityRegistry()} for the requested shard's reader engine
 * and calls its {@link org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine#pollNow}.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * {@code pollNow} does real blob-store I/O (a {@code ShardStateStore} read and, if a newer
 * generation exists, a manifest read plus materialization), same reasoning as {@code
 * TransportCompactionTriggerAction}'s own dispatch.
 *
 * <p>Requires routing to the specific node actually hosting a reader copy, same "the caller
 * already knows which node to ask" contract as {@code TransportWaitForGenerationAction}: a
 * request for a shard not hosted on the receiving node gets {@code polled=false}, not an error.
 *
 * <p>Also the one place this node's {@link TransportService} reaches {@link ServerlessStoragePlugin}:
 * {@link ServerlessStoragePlugin#createComponents} has no {@code TransportService} parameter, so
 * there is no other seam for the plugin to obtain one. Every registered transport action is bound
 * as an eager Guice singleton (core's {@code ActionModule} binds every {@code getActions()} entry
 * that way), so this constructor is guaranteed to run during node startup, well before any shard --
 * and therefore {@code ObjectStoreWriterEngine}, the actual consumer via {@link
 * org.opensearch.serverless.storage.writerengine.WriterPublicationNotifier} -- is ever constructed.
 *
 * <p>{@link IndicesClusterStateService} reaches the plugin the identical way and for the identical
 * reason: {@code createComponents} has no parameter for it either, and this constructor is the same
 * guaranteed-early Guice singleton. {@code ReaderShardPreWarmCoordinator} (D1) is the consumer -- see
 * {@link IndicesClusterStateService#onDemandOpenIndices()}'s own javadoc for why it needs this.
 */
public class TransportPollNowAction extends HandledTransportAction<PollNowRequest, PollNowResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#readerShardActivityRegistry()}.
     * @param threadPool dispatches the actual poll off the transport thread.
     * @param indicesClusterStateService pushed into the plugin so {@code ReaderShardPreWarmCoordinator}
     *                                   can find this node's own on-demand-opened gated indices.
     */
    @Inject
    public TransportPollNowAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool,
        IndicesClusterStateService indicesClusterStateService
    ) {
        super(PollNowAction.NAME, transportService, actionFilters, PollNowRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
        plugin.setTransportService(transportService);
        plugin.setIndicesClusterStateService(indicesClusterStateService);
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to poll.
     * @param listener notified with the result once the poll (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, PollNowRequest request, ActionListener<PollNowResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                boolean polled = plugin.readerShardActivityRegistry().pollNow(request.indexUuid(), request.shardId());
                listener.onResponse(new PollNowResponse(polled));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
