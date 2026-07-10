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

import java.util.Optional;

/**
 * The actual work behind {@link ShardIdleTimeAction}: a plain node-local lookup into {@link
 * ServerlessStoragePlugin#shardActivityRegistry()} -- no blob-store I/O, no thread-pool dispatch
 * (unlike {@link org.opensearch.serverless.storage.compaction.action.TransportCompactionTriggerAction},
 * which does real I/O and must never run on the transport thread), since a
 * {@link java.util.concurrent.ConcurrentHashMap} read and a {@link java.lang.ref.WeakReference}
 * dereference are both cheap enough to answer synchronously.
 *
 * <p>Requires routing to the specific node actually hosting the shard's writer engine: unlike
 * {@code TransportCompactionTriggerAction} (which operates purely against the shared object store
 * and so can run on whichever node receives the request), this action can only ever answer from
 * {@link ServerlessStoragePlugin#shardActivityRegistry()}'s own node-local state. A request for a
 * shard not hosted on the receiving node gets {@link ShardIdleTimeResponse#notTracked()}, not an
 * error -- the caller is expected to already know (e.g. from the cluster state's routing table)
 * which node to ask, the same way any other single-node transport action works.
 */
public class TransportShardIdleTimeAction extends HandledTransportAction<ShardIdleTimeRequest, ShardIdleTimeResponse> {

    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's {@link ServerlessStoragePlugin#shardActivityRegistry()}.
     */
    @Inject
    public TransportShardIdleTimeAction(TransportService transportService, ActionFilters actionFilters, ServerlessStoragePlugin plugin) {
        super(ShardIdleTimeAction.NAME, transportService, actionFilters, ShardIdleTimeRequest::new);
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard to query.
     * @param listener notified with the result immediately, since this lookup does no I/O.
     */
    @Override
    protected void doExecute(Task task, ShardIdleTimeRequest request, ActionListener<ShardIdleTimeResponse> listener) {
        Optional<Long> millisSinceLastActivity = plugin.shardActivityRegistry()
            .millisSinceLastActivity(request.indexUuid(), request.shardId());
        listener.onResponse(
            millisSinceLastActivity.<ShardIdleTimeResponse>map(ShardIdleTimeResponse::new).orElseGet(ShardIdleTimeResponse::notTracked)
        );
    }
}
