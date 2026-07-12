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

import java.util.Optional;

/**
 * The actual work behind {@link WaitForGenerationAction}: looks up the receiving node's own
 * {@link ReaderShardActivityRegistry} for the requested shard and blocks on {@link
 * ReaderShardActivityRegistry#waitForGeneration} -- see that method's own javadoc, and {@link
 * org.opensearch.serverless.storage.readerengine.ObjectStoreReaderEngine#waitForGeneration} it
 * ultimately delegates to, for the actual wait mechanism.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * this can block for up to the request's own {@code timeout} (real time, not I/O-bound but still
 * a genuine blocking wait), which must never happen on a transport/network thread -- same
 * reasoning as {@code TransportCompactionTriggerAction}'s own dispatch.
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
     * @param threadPool dispatches the actual wait off the transport thread.
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
     * @param listener notified with the result once the wait (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, WaitForGenerationRequest request, ActionListener<WaitForGenerationResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                Optional<Boolean> reached = plugin.readerShardActivityRegistry()
                    .waitForGeneration(request.indexUuid(), request.shardId(), request.minGeneration(), request.timeout());
                listener.onResponse(new WaitForGenerationResponse(reached.orElse(false), reached.isPresent()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                listener.onFailure(e);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
