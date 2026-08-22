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
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The actual work behind {@link NodeIdleShardsAction}: a plain node-local snapshot of {@link
 * ServerlessStoragePlugin#shardActivityRegistry()}, same "no I/O, no dispatch needed" reasoning as
 * {@link TransportShardIdleTimeAction}.
 */
public class TransportNodeIdleShardsAction extends HandledTransportAction<NodeIdleShardsRequest, NodeIdleShardsResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#shardActivityRegistry()}.
     */
    @Inject
    public TransportNodeIdleShardsAction(TransportService transportService, ActionFilters actionFilters, ServerlessStoragePlugin plugin) {
        super(NodeIdleShardsAction.NAME, transportService, actionFilters, NodeIdleShardsRequest::new);
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request unused -- this action takes no parameters.
     * @param listener notified with the result immediately, since this lookup does no I/O.
     */
    @Override
    protected void doExecute(Task task, NodeIdleShardsRequest request, ActionListener<NodeIdleShardsResponse> listener) {
        Map<String, Long> snapshot = plugin.shardActivityRegistry().snapshotAll();
        List<IdleShardEntry> entries = new ArrayList<>(snapshot.size());
        for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
            int separator = entry.getKey().lastIndexOf('/');
            String indexUuid = entry.getKey().substring(0, separator);
            int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
            entries.add(new IdleShardEntry(indexUuid, shardId, entry.getValue()));
        }
        listener.onResponse(new NodeIdleShardsResponse(entries));
    }
}
