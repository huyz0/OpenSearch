/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link NodeWalBacklogAction}: a plain node-local snapshot of {@link
 * ServerlessStoragePlugin#sharedWalChunkService()}, same "no I/O, no dispatch needed" reasoning as
 * {@code TransportNodeCacheStatsAction}.
 */
public class TransportNodeWalBacklogAction extends HandledTransportAction<NodeWalBacklogRequest, NodeWalBacklogResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#sharedWalChunkService()}.
     */
    @Inject
    public TransportNodeWalBacklogAction(TransportService transportService, ActionFilters actionFilters, ServerlessStoragePlugin plugin) {
        super(NodeWalBacklogAction.NAME, transportService, actionFilters, NodeWalBacklogRequest::new);
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request unused -- this action takes no parameters.
     * @param listener notified with the result immediately, since this lookup does no I/O.
     */
    @Override
    protected void doExecute(Task task, NodeWalBacklogRequest request, ActionListener<NodeWalBacklogResponse> listener) {
        WalChunkService walChunkService = plugin.sharedWalChunkService();
        if (walChunkService == null) {
            listener.onResponse(new NodeWalBacklogResponse(0, 0L));
            return;
        }
        listener.onResponse(new NodeWalBacklogResponse(walChunkService.bufferedRecordCount(), walChunkService.totalBufferedBytes()));
    }
}
