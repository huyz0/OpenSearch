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
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Optional;
import java.util.Set;

/**
 * The actual work behind {@link SnapshotRestoreAction}: finds the (primaryTerm, generation) {@link
 * SnapshotPinAction} pinned under the request's {@code snapshotId} and CASes the shard's head to
 * point at it directly (bypassing {@link ShardHead#withPublishedGeneration}'s forward-only
 * validation -- a restore is deliberately a rollback, not a publication).
 *
 * <p>Refuses to proceed if the shard's lease is currently held (see {@link
 * SnapshotRestoreAction}'s own javadoc for why) or if {@code snapshotId} names no pin on this
 * shard at all (nothing to restore to). Bounded CAS retry ({@link #MAX_RESTORE_ATTEMPTS}), same
 * shape as {@link org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor} and this
 * plugin's other CAS-retry call sites -- a version conflict here almost certainly means the lease
 * was just (re)acquired between the read and the write, which the next attempt's own lease check
 * will correctly catch and fail on, not spin forever against.
 *
 * <p>Same dispatch shape as {@link TransportSnapshotPinAction}: no specific-node routing needed,
 * dispatched onto {@link ThreadPool.Names#GENERIC}.
 */
public class TransportSnapshotRestoreAction extends HandledTransportAction<SnapshotRestoreRequest, SnapshotRestoreResponse> {

    /** Bounded CAS-retry budget, matching this plugin's other CAS-retry defaults. */
    private static final int MAX_RESTORE_ATTEMPTS = 5;

    private final ServerlessStoragePlugin plugin;
    private final ThreadPool threadPool;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual restore attempt off the transport thread.
     */
    @Inject
    public TransportSnapshotRestoreAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool
    ) {
        super(SnapshotRestoreAction.NAME, transportService, actionFilters, SnapshotRestoreRequest::new);
        this.plugin = plugin;
        this.threadPool = threadPool;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard and snapshot to restore from.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, SnapshotRestoreRequest request, ActionListener<SnapshotRestoreResponse> listener) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                BlobContainer container = plugin.blobContainerForDirectoryFactory(request.indexUuid(), request.shardId());
                ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
                BlobContainerDurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);

                Set<PinRecord> pins = pinRegistry.getPins(request.indexUuid(), request.shardId());
                PinRecord targetPin = null;
                for (PinRecord pin : pins) {
                    if (pin.pinId().equals(request.snapshotId()) && (targetPin == null || pin.generation() > targetPin.generation())) {
                        targetPin = pin;
                    }
                }
                if (targetPin == null) {
                    listener.onFailure(
                        new IllegalStateException(
                            "no snapshot ["
                                + request.snapshotId()
                                + "] pinned on shard ["
                                + request.indexUuid()
                                + "/"
                                + request.shardId()
                                + "] to restore from"
                        )
                    );
                    return;
                }

                for (int attempt = 0; attempt < MAX_RESTORE_ATTEMPTS; attempt++) {
                    Optional<VersionedShardHead> current = shardStateStore.get(request.indexUuid(), request.shardId());
                    if (current.isEmpty()) {
                        listener.onFailure(
                            new IllegalStateException(
                                "shard [" + request.indexUuid() + "/" + request.shardId() + "] has no head to restore"
                            )
                        );
                        return;
                    }
                    ShardHead currentHead = current.get().head();
                    if (currentHead.isLeaseHeldAt(System.currentTimeMillis())) {
                        listener.onFailure(
                            new IllegalStateException(
                                "shard ["
                                    + request.indexUuid()
                                    + "/"
                                    + request.shardId()
                                    + "] has an active writer lease; refusing to restore-in-place while a lease is "
                                    + "held -- release or wait for it to expire first"
                            )
                        );
                        return;
                    }

                    ShardHead restoredHead = new ShardHead(
                        targetPin.primaryTerm(),
                        currentHead.leaseHolderNodeId(),
                        currentHead.leaseExpiryMillis(),
                        targetPin.generation()
                    );
                    CasResult result = shardStateStore.compareAndSet(
                        request.indexUuid(),
                        request.shardId(),
                        Optional.of(current.get().version()),
                        restoredHead
                    );
                    if (result == CasResult.SUCCESS) {
                        listener.onResponse(new SnapshotRestoreResponse(targetPin.primaryTerm(), targetPin.generation()));
                        return;
                    }
                    // VERSION_CONFLICT: someone else moved the head between our read and write --
                    // loop back and re-check (including the lease check) against the new state.
                }
                listener.onFailure(
                    new IllegalStateException(
                        "shard ["
                            + request.indexUuid()
                            + "/"
                            + request.shardId()
                            + "] head kept changing out from under the restore attempt after "
                            + MAX_RESTORE_ATTEMPTS
                            + " retries"
                    )
                );
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
