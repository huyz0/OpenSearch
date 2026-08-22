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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
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
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
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
 *
 * <p><b>Resolves the request's {@code indexUuid} against cluster metadata before touching the
 * object store</b> -- see {@link #requireRealShard}, which the other two retention transports in
 * this package share.
 */
public class TransportSnapshotPinAction extends HandledTransportAction<SnapshotPinRequest, SnapshotPinResponse> {

    private final ServerlessStoragePlugin plugin;
    private final String localNodeId;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    // A real Guice singleton (see this constructor's own @Inject), so doExecute can use this
    // concurrently across different in-flight requests -- IndexMetadataUuidIndex's own volatile-field
    // cache is documented safe under exactly that access pattern.
    private final IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link HandledTransportAction} to register this action.
     * @param actionFilters applied by {@link HandledTransportAction} around every request.
     * @param plugin resolves each request's shard {@link BlobContainer}.
     * @param threadPool dispatches the actual pin attempt off the transport thread.
     * @param clusterService resolves whether the requested {@code (indexUuid, shardId)} is a real shard.
     */
    @Inject
    public TransportSnapshotPinAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ServerlessStoragePlugin plugin,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        super(SnapshotPinAction.NAME, transportService, actionFilters, SnapshotPinRequest::new);
        // Recorded so a leftover pin can be traced to whoever took it. Read at construction because
        // TransportService knows the local node and this action has no cluster service of its own.
        this.localNodeId = transportService.getLocalNode() == null ? "" : transportService.getLocalNode().getId();
        this.plugin = plugin;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    /**
     * Resolves {@code indexUuid} to a real index in {@code metadata}, and {@code shardId} to a real
     * shard of it, or throws. Shared by all three retention transports in this package, which
     * already defer to this class for their common shape (see their own javadoc).
     *
     * <p>{@link ServerlessStoragePlugin#blobContainerForDirectoryFactory} builds a blob path out of
     * whatever string it is handed ({@code BlobPath.cleanPath().add(indexUuid)}), and the underlying
     * store resolves path segments without normalising them -- so an unvalidated uuid is a path the
     * caller chooses, not a shard the caller owns: {@code ".."} segments escape the repository base
     * on a filesystem repository, and on an object store a real-but-unrelated uuid simply reaches
     * another index's shards. Resolving through cluster metadata makes the check "this names a shard
     * that exists" rather than the far weaker "this string looks harmless". The same resolution
     * {@code TransportShardSplitAction#requireRealShard} performs, with the same error-message shape.
     *
     * <p>The shard bound accepts an id at or beyond {@code getNumberOfShards()} when the index's
     * split metadata knows it: an in-place split reserves child shard ids past the base range
     * <em>without</em> growing {@code number_of_shards} (see {@code
     * IndexMetadata#inSyncAllocationIds}), and a split child is a perfectly ordinary pin/restore
     * target. Rejecting on the base count alone would have made these refuse real shards.
     *
     * @throws IllegalArgumentException if {@code indexUuid} doesn't resolve to a real,
     *                                  currently-existing index, or {@code shardId} isn't one of its
     *                                  shards.
     */
    // Visible for testing: pure metadata resolution, exercisable without a plugin or a blob store.
    static void requireRealShard(IndexMetadataUuidIndex uuidIndex, Metadata metadata, String indexUuid, int shardId) {
        IndexMetadata indexMetadata = uuidIndex.findByUuid(metadata, indexUuid);
        if (indexMetadata == null) {
            throw new IllegalArgumentException("index [" + indexUuid + "] does not exist");
        }
        if (shardId >= indexMetadata.getNumberOfShards() && indexMetadata.getSplitShardsMetadata().getRangeOfShard(shardId) == null) {
            throw new IllegalArgumentException(
                "shard ["
                    + shardId
                    + "] is out of bounds for index ["
                    + indexMetadata.getIndex().getName()
                    + "]["
                    + indexUuid
                    + "], which has "
                    + indexMetadata.getNumberOfShards()
                    + " shard(s)"
            );
        }
    }

    /**
     * @param task the task tracking this request, unused.
     * @param request names the shard and snapshot to pin.
     * @param listener notified with the result once the attempt (dispatched off-thread) completes.
     */
    @Override
    protected void doExecute(Task task, SnapshotPinRequest request, ActionListener<SnapshotPinResponse> listener) {
        final String ownerId = localNodeId;
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                requireRealShard(uuidIndex, clusterService.state().metadata(), request.indexUuid(), request.shardId());

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
                // Stamped with who took it and when it lapses. An index-wide pin puts every shard down with a
                // short expiry and only makes them permanent once all of them exist, so a coordinator that
                // stops halfway leaves pins that lapse rather than pins that must be hunted down.
                PinRecord newPin = new PinRecord(request.snapshotId(), primaryTerm, generation, ownerId, request.expiresAtMillis());

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
