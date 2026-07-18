/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.security.RestrictingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Optional;

/**
 * The actual work behind {@link SnapshotPinAction}: reads the shard's current head via {@link
 * ShardStateStore#get}, then durably pins that exact (primaryTerm, generation) under the request's
 * {@code snapshotId} via {@link org.opensearch.serverless.storage.retention.DurablePinRegistry#addPin}
 * -- idempotent by that method's own contract, so a retried snapshot request is a safe no-op, not
 * a duplicate or an error.
 *
 * <p>Requires no routing to a specific data node: like {@link
 * org.opensearch.serverless.storage.compaction.action.TransportCompactionTriggerAction}, this
 * operates purely against the shared object store via {@link BlobContainer}s and the shard's own
 * CAS-guarded head, never against any node-local shard state, so whichever node receives this
 * request can execute it directly.
 *
 * <p>Dispatched onto {@link ThreadPool.Names#GENERIC}, not run on the transport thread directly:
 * reading the head and writing the pin are both real blob-store I/O.
 */
public class TransportSnapshotPinAction extends HandledTransportAction<SnapshotPinRequest, SnapshotPinResponse> {

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual pin attempt off the transport thread.
     */
    @Inject
    public TransportSnapshotPinAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(SnapshotPinAction.NAME, transportService, actionFilters, SnapshotPinRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard and snapshot to pin.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, SnapshotPinRequest request, ActionListener<SnapshotPinResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                // Credential scoping per tier (rfc-serverless-opensearch.md &sect;15): pinning
                // never deletes anything (removePin below is a CAS-based mutate, not a raw blob
                // delete), so this container is wrapped delete-denied.
                BlobContainer container = new RestrictingBlobContainer(
                    plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId()),
                    false
                );
                ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
                BlobContainerDurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);

                Optional<VersionedShardHead> head = shardStateStore.get(request.indexUuid(), request.shardId());
                if (head.isEmpty()) {
                    listener.onFailure(
                        new IllegalStateException(
                            "shard [" + request.indexUuid() + "/" + request.shardId() + "] has never published a manifest to snapshot"
                        )
                    );
                    return;
                }

                long primaryTerm = head.get().head().primaryTerm();
                long generation = head.get().head().latestManifestGeneration();
                PinRecord newPin = new PinRecord(request.snapshotId(), primaryTerm, generation);

                // Create-or-replace, not additive (see SnapshotPinAction's own javadoc): replacePin
                // adds the new pin and strips any older one under the same snapshotId within a
                // single atomic CAS mutation. Deliberately not a separate addPin call followed by a
                // read-then-remove loop -- that shape has a real race under two CONCURRENT calls
                // for the same snapshotId (e.g. a client retry): each call's independent removal
                // pass could observe and remove the OTHER call's just-added pin, leaving zero pins
                // for this snapshotId even though both calls reported success. See
                // DurablePinRegistry#replacePin's own javadoc for why this is safe under that race.
                pinRegistry.replacePin(request.indexUuid(), request.shardId(), newPin);

                listener.onResponse(new SnapshotPinResponse(primaryTerm, generation));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
