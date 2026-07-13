/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.format.CacheStatsRegistry;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link NodeCacheStatsAction}: a plain node-local snapshot of {@link
 * ServerlessStoragePlugin#sharedBundleCache()} and {@link ServerlessStoragePlugin#cacheStatsRegistry()},
 * same "no I/O, no dispatch needed" reasoning as {@code
 * org.opensearch.serverless.storage.readerengine.action.TransportNodeManifestLagAction}.
 */
public class TransportNodeCacheStatsAction extends HandledTransportAction<NodeCacheStatsRequest, NodeCacheStatsResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#sharedBundleCache()} and
     *               {@link ServerlessStoragePlugin#cacheStatsRegistry()}.
     */
    @Inject
    public TransportNodeCacheStatsAction(TransportService transportService, ActionFilters actionFilters, ServerlessStoragePlugin plugin) {
        super(NodeCacheStatsAction.NAME, transportService, actionFilters, NodeCacheStatsRequest::new);
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request unused -- this action takes no parameters.
     * @param listener notified with the result immediately, since this lookup does no I/O.
     */
    @Override
    protected void doExecute(Task task, NodeCacheStatsRequest request, ActionListener<NodeCacheStatsResponse> listener) {
        InMemoryPlaintextBundleCache inMemoryCache = plugin.sharedBundleCache();
        long hitCount = inMemoryCache == null ? 0L : inMemoryCache.hitCount();
        long missCount = inMemoryCache == null ? 0L : inMemoryCache.missCount();
        java.util.List<ShardCacheStatsEntry> entries = new java.util.ArrayList<>();
        for (CacheStatsRegistry.ShardCacheStats shardStats : plugin.cacheStatsRegistry().snapshotAll()) {
            entries.add(
                new ShardCacheStatsEntry(
                    shardStats.indexUuid(),
                    shardStats.shardId(),
                    shardStats.hitCount(),
                    shardStats.missCount(),
                    shardStats.averageColdReadLatencyMillis()
                )
            );
        }
        listener.onResponse(new NodeCacheStatsResponse(hitCount, missCount, entries));
    }
}
