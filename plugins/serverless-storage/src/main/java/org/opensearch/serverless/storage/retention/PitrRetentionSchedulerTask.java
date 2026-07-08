/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;

/**
 * Runs {@link PitrRetentionReconciler} for one shard on a fixed schedule -- the piece
 * rfc-serverless-opensearch.md &sect;16 Phase 4.6 flagged as still missing: {@code
 * PitrRetentionReconciler} existed and was correct, but nothing invoked it periodically.
 *
 * <p>Reconciliation is heavier than the directory-tier refresh {@code ObjectStoreWriterEngine}
 * already schedules (it enumerates and reads every manifest the shard has ever written, via
 * {@link BlobContainerManifestStore#listManifests}), and the PITR window moves far more slowly
 * than a directory entry's TTL, so this is a separate task with its own (much longer) interval
 * rather than folded into that existing refresh.
 *
 * <p>A failed reconciliation attempt is never worse than a stale one: pins already in place stay
 * in place, nothing is deleted as a side effect of a failed attempt, and the next scheduled tick
 * simply tries again -- so a failure here is logged-and-swallowed, never worth failing the shard's
 * engine over.
 */
public final class PitrRetentionSchedulerTask implements Closeable {

    private final String indexUuid;
    private final int shardId;
    private final BlobContainerManifestStore manifestStore;
    private final PitrRetentionReconciler reconciler;
    private final long windowMillis;
    private final Scheduler.Cancellable task;

    public PitrRetentionSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        String indexUuid,
        int shardId,
        BlobContainerManifestStore manifestStore,
        DurablePinRegistry pinRegistry,
        long windowMillis
    ) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be > 0, got " + windowMillis);
        }
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestStore = manifestStore;
        this.reconciler = new PitrRetentionReconciler(pinRegistry);
        this.windowMillis = windowMillis;
        this.task = threadPool.scheduleWithFixedDelay(this::reconcileSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void reconcileSafely() {
        try {
            reconciler.reconcile(indexUuid, shardId, manifestStore.listManifests(), System.currentTimeMillis(), windowMillis);
        } catch (IOException e) {
            // See class javadoc: swallow and let the next scheduled tick retry.
        }
    }

    @Override
    public void close() {
        task.cancel();
    }
}
