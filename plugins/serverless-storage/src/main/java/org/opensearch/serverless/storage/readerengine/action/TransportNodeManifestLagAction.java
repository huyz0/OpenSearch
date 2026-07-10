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
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The actual work behind {@link NodeManifestLagAction}: a plain node-local snapshot of {@link
 * ServerlessStoragePlugin#readerShardActivityRegistry()}, same "no I/O, no dispatch needed"
 * reasoning as {@code org.opensearch.serverless.storage.writerengine.action.TransportShardIdleTimeAction}.
 */
public class TransportNodeManifestLagAction extends HandledTransportAction<NodeManifestLagRequest, NodeManifestLagResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#readerShardActivityRegistry()}.
     */
    @Inject
    public TransportNodeManifestLagAction(TransportService transportService, ActionFilters actionFilters, ServerlessStoragePlugin plugin) {
        super(NodeManifestLagAction.NAME, transportService, actionFilters, NodeManifestLagRequest::new);
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request unused -- this action takes no parameters.
     * @param listener notified with the result immediately, since this lookup does no I/O.
     */
    @Override
    protected void doExecute(Task task, NodeManifestLagRequest request, ActionListener<NodeManifestLagResponse> listener) {
        Map<String, Long> snapshot = plugin.readerShardActivityRegistry().snapshotAll();
        List<ShardLagEntry> entries = new ArrayList<>(snapshot.size());
        for (Map.Entry<String, Long> entry : snapshot.entrySet()) {
            int separator = entry.getKey().lastIndexOf('/');
            String indexUuid = entry.getKey().substring(0, separator);
            int shardId = Integer.parseInt(entry.getKey().substring(separator + 1));
            entries.add(new ShardLagEntry(indexUuid, shardId, entry.getValue()));
        }
        listener.onResponse(new NodeManifestLagResponse(entries));
    }
}
