/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link NodeObjectStoreRequestStatsAction}: a plain node-local snapshot of
 * {@link ServerlessStoragePlugin#objectStoreRequestCounter()}, same "no I/O, no dispatch needed"
 * reasoning as {@code TransportNodeCacheStatsAction}.
 */
public class TransportNodeObjectStoreRequestStatsAction extends HandledTransportAction<
    NodeObjectStoreRequestStatsRequest,
    NodeObjectStoreRequestStatsResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#objectStoreRequestCounter()}.
     */
    @Inject
    public TransportNodeObjectStoreRequestStatsAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin
    ) {
        super(NodeObjectStoreRequestStatsAction.NAME, transportService, actionFilters, NodeObjectStoreRequestStatsRequest::new);
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request unused -- this action takes no parameters.
     * @param listener notified with the result immediately, since this lookup does no I/O.
     */
    @Override
    protected void doExecute(
        Task task,
        NodeObjectStoreRequestStatsRequest request,
        ActionListener<NodeObjectStoreRequestStatsResponse> listener
    ) {
        ObjectStoreRequestCounter counter = plugin.objectStoreRequestCounter();
        listener.onResponse(
            new NodeObjectStoreRequestStatsResponse(counter.getCount(), counter.putCount(), counter.deleteCount(), counter.listCount())
        );
    }
}
