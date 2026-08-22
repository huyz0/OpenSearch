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
import org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * The actual work behind {@link WaitForGenerationAction}: looks up the receiving node's own
 * {@link ReaderShardActivityRegistry} for the requested shard and asynchronously waits via {@link
 * ReaderShardActivityRegistry#waitForGeneration} -- see that method's own javadoc, and {@link
 * org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine#waitForGeneration} it
 * ultimately delegates to, for the actual wait mechanism.
 *
 * <p>The initial lookup+dispatch is pushed onto {@link ThreadPool.Names#GENERIC}, not run on the
 * transport thread directly, since {@link
 * org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine#pollForNewerManifest}'s
 * first check does real object-store I/O -- same reasoning as {@code
 * TransportCompactionTriggerAction}'s own dispatch. Unlike that first hop, nothing here blocks
 * once dispatched: the wait itself is listener-based all the way down, so this action never pins a
 * {@code GENERIC} worker for the request's full {@code timeout}.
 */
public class TransportWaitForGenerationAction extends HandledTransportAction<WaitForGenerationRequest, WaitForGenerationResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves this node's {@link ServerlessStoragePlugin#readerShardActivityRegistry()}.
     * @param threadPool dispatches the initial lookup off the transport thread.
     */
    @Inject
    public TransportWaitForGenerationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(WaitForGenerationAction.NAME, transportService, actionFilters, WaitForGenerationRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard, the generation to wait for, and the timeout.
     * @param listener notified with the result once the asynchronous wait completes.
     */
    @Override
    protected void doExecute(Task task, WaitForGenerationRequest request, ActionListener<WaitForGenerationResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(
                () -> plugin.readerShardActivityRegistry()
                    .waitForGeneration(
                        request.indexUuid(),
                        request.shardId(),
                        request.minGeneration(),
                        request.timeout(),
                        ActionListener.wrap(
                            reached -> listener.onResponse(new WaitForGenerationResponse(reached.orElse(false), reached.isPresent())),
                            listener::onFailure
                        )
                    )
            );
    }
}
