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

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the request's index name to a UUID and shard count.
     * @param client dispatches each shard's {@link SnapshotPinAction}/{@link SnapshotReleaseAction} call.
     */
    @Inject
    public TransportIndexSnapshotPinAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NodeClient client
    ) {
        super(IndexSnapshotPinAction.NAME, transportService, actionFilters, IndexSnapshotPinRequest::new);
        this.clusterService = clusterService;
        this.client = client;
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
        pinShard(indexUuid, numberOfShards, 0, request.snapshotId(), new ArrayList<>(), listener);
    }

    private void pinShard(
        String indexUuid,
        int numberOfShards,
        int shardId,
        String snapshotId,
        List<Integer> pinnedSoFar,
        ActionListener<IndexSnapshotPinResponse> listener
    ) {
        if (shardId == numberOfShards) {
            listener.onResponse(new IndexSnapshotPinResponse(numberOfShards));
            return;
        }
        client.execute(SnapshotPinAction.INSTANCE, new SnapshotPinRequest(indexUuid, shardId, snapshotId), ActionListener.wrap(response -> {
            pinnedSoFar.add(shardId);
            pinShard(indexUuid, numberOfShards, shardId + 1, snapshotId, pinnedSoFar, listener);
        }, failure -> rollBackAndFail(indexUuid, snapshotId, pinnedSoFar, failure, listener)));
    }

    private void rollBackAndFail(
        String indexUuid,
        String snapshotId,
        List<Integer> pinnedSoFar,
        Exception failure,
        ActionListener<IndexSnapshotPinResponse> listener
    ) {
        if (pinnedSoFar.isEmpty()) {
            listener.onFailure(failure);
            return;
        }
        AtomicInteger remaining = new AtomicInteger(pinnedSoFar.size());
        for (int shardId : pinnedSoFar) {
            client.execute(
                SnapshotReleaseAction.INSTANCE,
                new SnapshotReleaseRequest(indexUuid, shardId, snapshotId),
                ActionListener.wrap(ignored -> {
                    if (remaining.decrementAndGet() == 0) {
                        listener.onFailure(failure);
                    }
                }, releaseFailure -> {
                    // The compensating release itself failed -- SnapshotReleaseAction is
                    // idempotent, so a caller retrying the pin (or a manual release) will clean
                    // this up; still surface the original pin failure, not this secondary one, so
                    // the caller understands why the overall operation failed.
                    if (remaining.decrementAndGet() == 0) {
                        listener.onFailure(failure);
                    }
                })
            );
        }
    }
}
