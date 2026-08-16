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
import org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore;
import org.opensearch.serverless.storage.retention.PinLedger;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * The actual work behind {@link IndexSnapshotReleaseAction}: resolves the index's UUID and shard
 * count from cluster state, then releases {@code snapshotId} from each shard in turn via {@link
 * SnapshotReleaseAction}. See {@link IndexSnapshotReleaseAction}'s own javadoc for why this needs
 * no rollback, unlike {@link TransportIndexSnapshotPinAction}.
 */
public class TransportIndexSnapshotReleaseAction extends HandledTransportAction<IndexSnapshotReleaseRequest, IndexSnapshotReleaseResponse> {

    private final ClusterService clusterService;
    private final NodeClient client;
    private final ServerlessStoragePlugin plugin;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param clusterService resolves the request's index name to a UUID and shard count.
     * @param client dispatches each shard's {@link SnapshotReleaseAction} call.
     */
    @Inject
    public TransportIndexSnapshotReleaseAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        NodeClient client,
        ServerlessStoragePlugin plugin
    ) {
        super(IndexSnapshotReleaseAction.NAME, transportService, actionFilters, IndexSnapshotReleaseRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.plugin = plugin;
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the index and snapshot to release.
     * @param listener notified with the result once every shard has released the pin, or with the
     *                 first failure encountered.
     */
    @Override
    protected void doExecute(Task task, IndexSnapshotReleaseRequest request, ActionListener<IndexSnapshotReleaseResponse> listener) {
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

        // The ledger decides what to release, not the index's current shard count. Those can differ -- a
        // resharded index has a different count than the one the pin was taken across, and walking the
        // current count would release the wrong set while reporting success. The ledger records the set as
        // it was.
        //
        // A missing ledger is not an error. It means either that this pin was never taken, or that a
        // previous release already finished and removed the record; releasing per shard is idempotent, so
        // falling back to the current shard count covers both, and covers pins taken before ledgers existed.
        final BlobContainerPinLedgerStore ledgerStore;
        final int shardsToRelease;
        try {
            ledgerStore = new BlobContainerPinLedgerStore(plugin.blobContainerForDirectoryFactory(indexUuid, 0));
            shardsToRelease = ledgerStore.read(request.snapshotId()).map(PinLedger::shardCount).orElse(indexMetadata.getNumberOfShards());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        releaseShard(indexUuid, shardsToRelease, 0, request.snapshotId(), ActionListener.wrap(response -> {
            // Deleted only after every pin it names is gone, which is what makes a failed release
            // retryable rather than an invisible leak: the record survives the failure and the next
            // attempt finds the same set. Core's shallow-copy delete orders it the same way and says so
            // in its own comment.
            try {
                ledgerStore.delete(request.snapshotId());
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(response);
        }, listener::onFailure));
    }

    private void releaseShard(
        String indexUuid,
        int numberOfShards,
        int shardId,
        String snapshotId,
        ActionListener<IndexSnapshotReleaseResponse> listener
    ) {
        if (shardId == numberOfShards) {
            listener.onResponse(new IndexSnapshotReleaseResponse(numberOfShards));
            return;
        }
        client.execute(
            SnapshotReleaseAction.INSTANCE,
            new SnapshotReleaseRequest(indexUuid, shardId, snapshotId),
            ActionListener.wrap(response -> releaseShard(indexUuid, numberOfShards, shardId + 1, snapshotId, listener), listener::onFailure)
        );
    }
}
