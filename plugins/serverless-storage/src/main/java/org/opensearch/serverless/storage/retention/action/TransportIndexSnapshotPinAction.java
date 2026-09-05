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
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore;
import org.opensearch.serverless.storage.retention.PinLedger;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The actual work behind {@link IndexSnapshotPinAction}: resolves the index's UUID and shard count
 * from cluster state, then pins each shard in turn (shard 0, then 1, ...) via {@link
 * SnapshotPinAction}, exactly the same per-shard call {@code POST .../_snapshot_pin} makes
 * directly. See {@link IndexSnapshotPinAction}'s own javadoc for the all-or-nothing rollback
 * contract this class implements.
 *
 * <p>Sequential rather than fanned out concurrently: keeps the rollback logic trivial (just "which
 * shards, in order, actually got pinned before the failure") and index shard counts are small
 * enough that the extra round trips are not a real cost for an operation this infrequent.
 *
 * <p>Reuses {@link SnapshotPinAction}/{@link SnapshotReleaseAction} via {@link NodeClient} rather
 * than duplicating their object-store logic -- both are themselves already dispatched onto {@link
 * org.opensearch.threadpool.ThreadPool.Names#GENERIC}, so this class needs no dispatch of its own.
 */
public class TransportIndexSnapshotPinAction extends HandledTransportAction<IndexSnapshotPinRequest, IndexSnapshotPinResponse> {

    private final ClusterService clusterService;
    private final NodeClient client;
    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the request's index name to a UUID and shard count.
     * @param client dispatches each shard's {@link SnapshotPinAction}/{@link SnapshotReleaseAction} call.
     * @param plugin resolves the index's shard 0 container, where the pin ledger is kept.
     */
    @Inject
    public TransportIndexSnapshotPinAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NodeClient client,
        ServerlessStoragePlugin plugin
    ) {
        super(IndexSnapshotPinAction.NAME, transportService, actionFilters, IndexSnapshotPinRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the index and snapshot to pin.
     * @param listener notified with the result once every shard is pinned, or with the first failure
     *                 once any already-pinned shards have been rolled back.
     */
    @Override
    protected void doExecute(Task task, IndexSnapshotPinRequest request, ActionListener<IndexSnapshotPinResponse> listener) {
        // Resolved through the descriptor supplier as well as cluster state, because a gated index has no
        // cluster state entry and metadata.index(name) answers null for one -- which reported
        // IndexNotFoundException for an index that exists, is serving traffic, and has manifests to pin.
        // The shard-level action underneath takes a uuid and a shard id and never consults cluster state,
        // so only this resolution had to learn about gating. Safe here because this runs on a transport or
        // GENERIC thread; AbsentIndexDescriptorSuppliers forbids a blocking descriptor read only on the
        // cluster state applier threads, where W4's deadlock lives.
        IndexMetadata indexMetadata = AbsentIndexDescriptorSuppliers.metadataOrDescriptor(
            clusterService.state().metadata(),
            request.indexName()
        );
        if (indexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(request.indexName()));
            return;
        }
        String indexUuid = indexMetadata.getIndexUUID();
        int numberOfShards = indexMetadata.getNumberOfShards();

        // The ledger is written BEFORE any pin, so it is a record of intent rather than of completion. That
        // direction is deliberate and it is the only one that helps: a coordinator that dies mid-fan-out
        // leaves pins behind, and a ledger written afterwards would not exist to say so. Written first, the
        // worst case is a ledger naming a pin that was never taken -- and release is idempotent per shard,
        // so acting on that costs nothing and clears it.
        try {
            new BlobContainerPinLedgerStore(plugin.blobContainerForDirectoryFactory(indexUuid, 0)).write(
                new PinLedger(request.snapshotId(), request.indexName(), indexUuid, numberOfShards, System.currentTimeMillis())
            );
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        // Phase one: every shard pinned with a short expiry. Phase two, below, makes them permanent once
        // all of them exist. An index-wide pin is N sequential shard pins, and until this the failure mode
        // of stopping in the middle was pins that hold generations against garbage collection forever --
        // rollback only runs when a shard *fails*, never when the coordinator itself goes away. Now a
        // half-finished pin lapses on its own, which is the direction to fail in for a mechanism whose only
        // job is to prevent deletion.
        long provisionalExpiry = System.currentTimeMillis() + UNCONFIRMED_PIN_TTL_MILLIS;
        pinShard(indexUuid, numberOfShards, 0, request.snapshotId(), provisionalExpiry, new ArrayList<>(), listener);
    }

    /**
     * How long an unconfirmed pin holds its generation.
     *
     * <p>Defined on {@link PinLedger} rather than here because {@code PinLedgerSweeper} needs the same
     * number to know how long a just-written ledger's pins may legitimately not exist yet -- see that
     * constant's own javadoc.
     */
    static final long UNCONFIRMED_PIN_TTL_MILLIS = PinLedger.UNCONFIRMED_PIN_TTL_MILLIS;

    private void pinShard(
        String indexUuid,
        int numberOfShards,
        int shardId,
        String snapshotId,
        long provisionalExpiry,
        List<Integer> pinnedSoFar,
        ActionListener<IndexSnapshotPinResponse> listener
    ) {
        if (shardId == numberOfShards) {
            confirmPins(indexUuid, numberOfShards, snapshotId, listener);
            return;
        }
        client.execute(
            SnapshotPinAction.INSTANCE,
            SnapshotPinRequest.expiring(indexUuid, shardId, snapshotId, provisionalExpiry),
            ActionListener.wrap(response -> {
                pinnedSoFar.add(shardId);
                pinShard(indexUuid, numberOfShards, shardId + 1, snapshotId, provisionalExpiry, pinnedSoFar, listener);
            }, failure -> rollBackAndFail(indexUuid, snapshotId, pinnedSoFar, failure, listener))
        );
    }

    /**
     * Phase two: every shard is pinned, so the pins become permanent.
     *
     * <p>Done here rather than by re-pinning, because the pinned generation must not change: a shard that
     * committed new data between the two phases would otherwise be re-pinned at a later generation, quietly
     * turning one point in time into two.
     *
     * <p><b>A failure here is not the safe direction, and used to be described as one.</b> This loop
     * re-stamps each shard's pin to {@code NEVER_EXPIRES} one shard at a time. If the object store fails at
     * shard 120 of 200, shards 0..119 are already permanent: they will not lapse, nothing will ever release
     * them, and garbage collection on those 120 shards is blocked at whatever generation was current --
     * while the caller has been told the pin did not take and therefore has no reason to release anything.
     * The pins that lapse (120..199) and the pins that do not (0..119) make the "snapshot" neither taken nor
     * absent. So a failure here runs the same compensating release the fan-out's own failure path runs.
     * Release is idempotent and by pin id, so it is correct over the confirmed and still-provisional shards
     * alike, and the caller still sees the original failure rather than the compensation's.
     */
    // Package-private rather than private so a test can drive the confirm phase directly: the failure this
    // guards against is the object store rejecting the 121st of 200 shards, which no amount of real wiring
    // produces on demand.
    void confirmPins(String indexUuid, int numberOfShards, String snapshotId, ActionListener<IndexSnapshotPinResponse> listener) {
        for (int shardId = 0; shardId < numberOfShards; shardId++) {
            try {
                confirmShardPin(indexUuid, shardId, snapshotId);
            } catch (Exception e) {
                // Every shard, not just the ones already confirmed: the unconfirmed ones would lapse on
                // their own, but releasing them now is a no-op-shaped extra call that removes any window in
                // which a retry of this whole request could see a stale provisional pin.
                List<Integer> everyShard = new ArrayList<>(numberOfShards);
                for (int i = 0; i < numberOfShards; i++) {
                    everyShard.add(i);
                }
                rollBackAndFail(indexUuid, snapshotId, everyShard, e, listener);
                return;
            }
        }
        listener.onResponse(new IndexSnapshotPinResponse(numberOfShards));
    }

    private void rollBackAndFail(
        String indexUuid,
        String snapshotId,
        List<Integer> pinnedSoFar,
        Exception failure,
        ActionListener<IndexSnapshotPinResponse> listener
    ) {
        if (pinnedSoFar.isEmpty()) {
            deleteLedgerBestEffort(indexUuid, snapshotId);
            listener.onFailure(failure);
            return;
        }
        AtomicInteger remaining = new AtomicInteger(pinnedSoFar.size());
        for (int shardId : pinnedSoFar) {
            releaseShardPin(indexUuid, shardId, snapshotId, ActionListener.wrap(ignored -> {
                if (remaining.decrementAndGet() == 0) {
                    deleteLedgerBestEffort(indexUuid, snapshotId);
                    listener.onFailure(failure);
                }
            }, releaseFailure -> {
                // The compensating release itself failed -- SnapshotReleaseAction is
                // idempotent, so a caller retrying the pin (or a manual release) will clean
                // this up; still surface the original pin failure, not this secondary one, so
                // the caller understands why the overall operation failed.
                if (remaining.decrementAndGet() == 0) {
                    deleteLedgerBestEffort(indexUuid, snapshotId);
                    listener.onFailure(failure);
                }
            }));
        }
    }

    /** One shard's confirm, as its own method so a test can make exactly one of them fail. */
    void confirmShardPin(String indexUuid, int shardId, String snapshotId) throws Exception {
        new BlobContainerDurablePinRegistry(plugin.blobContainerForDirectoryFactory(indexUuid, shardId)).confirmPin(
            indexUuid,
            shardId,
            snapshotId,
            PinRecord.NEVER_EXPIRES
        );
    }

    /** One shard's compensating release, as its own method so a test can observe which shards it covers. */
    void releaseShardPin(String indexUuid, int shardId, String snapshotId, ActionListener<SnapshotReleaseResponse> listener) {
        client.execute(SnapshotReleaseAction.INSTANCE, new SnapshotReleaseRequest(indexUuid, shardId, snapshotId), listener);
    }

    /**
     * Removes the ledger this request wrote before it started pinning, once the compensating release has
     * finished.
     *
     * <p>The ledger records intent, so leaving it behind after a rolled-back attempt leaves a permanent
     * record of a pin that does not exist -- and the sweeper that would otherwise clear it releases nothing
     * and is off by default, so "it will be tidied later" was not true. Deleted only after the releases
     * above, never before: the whole reason the ledger is written first is that it must outlive a failed
     * release, and deleting it while a release is still outstanding would recreate exactly the invisible
     * leak it exists to prevent. Failure here is swallowed -- the caller is being told about the original
     * failure, and a stale ledger is a bookkeeping cost, not a correctness one.
     */
    private void deleteLedgerBestEffort(String indexUuid, String snapshotId) {
        try {
            new BlobContainerPinLedgerStore(plugin.blobContainerForDirectoryFactory(indexUuid, 0)).delete(snapshotId);
        } catch (Exception ignored) {
            // See this method's own javadoc.
        }
    }
}
