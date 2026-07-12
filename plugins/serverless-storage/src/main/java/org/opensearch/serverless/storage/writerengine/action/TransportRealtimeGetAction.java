/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.writerengine.RealtimeGetResult;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Optional;

/**
 * The actual work behind {@link RealtimeGetAction}: looks up the receiving node's own {@link
 * ServerlessStoragePlugin#shardActivityRegistry()} for the requested shard's writer engine and
 * calls its {@link org.opensearch.serverless.storage.writerengine.ObjectStoreWriterEngine#realtimeGet}
 * -- see that method's own javadoc for why a reader engine could never answer this authoritatively.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * unlike {@code TransportShardIdleTimeAction}'s plain map lookup, this acquires a real Lucene
 * searcher and may read from the translog (a real-time get's own {@code readFromTranslog=true}),
 * genuine local I/O that should not run on a transport/network thread.
 *
 * <p>Requires routing to the specific node actually hosting the shard's writer engine, same "the
 * caller already knows which node to ask" contract as {@code TransportShardIdleTimeAction}: a
 * request for a shard not hosted on the receiving node gets {@code shardTracked=false}, not an
 * error.
 */
public class TransportRealtimeGetAction extends HandledTransportAction<RealtimeGetRequest, RealtimeGetResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#shardActivityRegistry()}.
     * @param threadPool dispatches the actual lookup off the transport thread.
     */
    @Inject
    public TransportRealtimeGetAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(RealtimeGetAction.NAME, transportService, actionFilters, RealtimeGetRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard and document id to look up.
     * @param listener notified with the result once the lookup (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, RealtimeGetRequest request, ActionListener<RealtimeGetResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                Optional<RealtimeGetResult> result = plugin.shardActivityRegistry()
                    .realtimeGet(request.indexUuid(), request.shardId(), request.id());
                listener.onResponse(
                    result.<RealtimeGetResponse>map(r -> new RealtimeGetResponse(true, r))
                        .orElseGet(() -> new RealtimeGetResponse(false, null))
                );
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
