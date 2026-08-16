/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * The actual work behind {@link IndexDeepSnapshotAction}: resolve the index (gated or ordinary),
 * copy each shard in turn via {@link ShardDeepSnapshotAction}, then finalize once this call is
 * itself already running on the cluster manager -- {@link TransportClusterManagerNodeAction}'s own
 * base machinery is what guarantees that, forwarding the request here if the node that first
 * received it was not the cluster manager.
 *
 * <p>Sequential across shards, the same choice {@link
 * org.opensearch.serverless.storage.retention.action.TransportIndexSnapshotPinAction} makes and for
 * the same reason: it keeps "which shards actually finished" trivial to reason about, and this is an
 * infrequent, already-slow-relative-to-a-single-blob-write operation where the extra round trips are
 * not the cost that matters. True node-parallel dispatch -- {@link ShardDeepSnapshotAction} needs no
 * specific node, so nothing here stops it -- is a follow-on, not a correctness requirement.
 */
public class TransportIndexDeepSnapshotAction extends TransportClusterManagerNodeAction<
    IndexDeepSnapshotRequest,
    IndexDeepSnapshotResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndexDeepSnapshotAction.class);

    private final RepositoriesService repositoriesService;
    private final NodeClient client;

    /**
     * Creates the transport action.
     *
     * @param transportService used by {@link TransportClusterManagerNodeAction} to register this action.
     * @param clusterService resolves the request's index name and reads cluster metadata for {@code finalizeSnapshot}.
     * @param threadPool used by {@link TransportClusterManagerNodeAction}'s own base machinery.
     * @param actionFilters applied by {@link TransportClusterManagerNodeAction} around every request.
     * @param indexNameExpressionResolver required by {@link TransportClusterManagerNodeAction}'s constructor, unused here.
     * @param repositoriesService resolves the request's target {@link Repository} by name.
     * @param client dispatches each shard's {@link ShardDeepSnapshotAction} call.
     */
    @Inject
    public TransportIndexDeepSnapshotAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        RepositoriesService repositoriesService,
        NodeClient client
    ) {
        super(
            IndexDeepSnapshotAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndexDeepSnapshotRequest::new,
            indexNameExpressionResolver
        );
        this.repositoriesService = repositoriesService;
        this.client = client;
    }

    @Override
    protected String executor() {
        // The real work is dispatched by TransportShardDeepSnapshotAction (GENERIC) and by the
        // repository's own async finalizeSnapshot/getRepositoryData -- this method only chains
        // listeners between them, the same SAME-executor choice
        // TransportEnableWritePartitionRoutingAction makes for the equivalent reason.
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(IndexDeepSnapshotRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected IndexDeepSnapshotResponse read(StreamInput in) throws IOException {
        return new IndexDeepSnapshotResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        IndexDeepSnapshotRequest request,
        ClusterState state,
        ActionListener<IndexDeepSnapshotResponse> listener
    ) {
        // Resolved through the descriptor supplier as well as cluster state, the same gated-index
        // resolution item 1 of this round built for the shallow pin/restore/release trio --
        // metadata.index(name) alone answers null for a gated index, which reported
        // IndexNotFoundException for an index that exists, is serving traffic, and has manifests to
        // copy. Safe here: clusterManagerOperation runs on this action's own executor() (SAME,
        // dispatched off the transport thread by the outer transport layer already), not on the
        // cluster state applier thread AbsentIndexDescriptorSuppliers forbids a blocking read from.
        IndexMetadata indexMetadata = AbsentIndexDescriptorSuppliers.metadataOrDescriptor(state.metadata(), request.indexName());
        if (indexMetadata == null) {
            listener.onFailure(new IndexNotFoundException(request.indexName()));
            return;
        }

        Repository repository;
        try {
            repository = repositoriesService.repository(request.repositoryName());
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }

        CopyContext context = new CopyContext(
            new IndexId(request.indexName(), indexMetadata.getIndexUUID()),
            request.repositoryName(),
            new SnapshotId(request.snapshotName(), UUIDs.randomBase64UUID()),
            repository,
            indexMetadata.getNumberOfShards(),
            System.currentTimeMillis(),
            listener
        );

        copyShard(0, context, ShardGenerations.builder());
    }

    /**
     * Everything one {@link #clusterManagerOperation} call carries through the whole recursive
     * shard-copy fan-out and into {@link #finalizeSnapshot} unchanged -- bundled after the bug hunt
     * that followed round 006 found the previous shape (8-11 positional parameters, most invariant
     * across every recursive call) made adding any new piece of per-request state touch two method
     * signatures and every call site, and made the {@code copyShard}-to-{@code finalizeSnapshot} call
     * a long positional-argument list a reviewer had to diff by eye. {@code shardGenerations} is
     * deliberately not a field here: it is the one thing that is genuinely per-call state (a mutable
     * builder accumulated across the recursion), not per-request context, and keeping it a separate
     * parameter says so.
     *
     * <p>{@code numberOfShards} lives here rather than being re-derived from an {@code IndexMetadata}
     * field, because this context deliberately carries no {@code IndexMetadata} at all -- {@link
     * #finalizeSnapshot} re-resolves it fresh (see that method's own javadoc for why) rather than
     * reusing one captured here, so the shard count this loop bounds itself by is the only piece of
     * that original resolution this record still needs.
     */
    private record CopyContext(IndexId indexId, String repositoryName, SnapshotId snapshotId, Repository repository, int numberOfShards,
        long startTime, ActionListener<IndexDeepSnapshotResponse> listener) {
    }

    private void copyShard(int shardId, CopyContext context, ShardGenerations.Builder shardGenerations) {
        if (shardId == context.numberOfShards()) {
            finalizeSnapshot(context, shardGenerations.build());
            return;
        }
        client.execute(
            ShardDeepSnapshotAction.INSTANCE,
            new ShardDeepSnapshotRequest(
                context.indexId().getName(),
                context.indexId().getId(),
                shardId,
                context.repositoryName(),
                context.snapshotId().getName(),
                context.snapshotId().getUUID()
            ),
            ActionListener.wrap(response -> {
                shardGenerations.put(context.indexId(), shardId, response.shardGeneration());
                copyShard(shardId + 1, context, shardGenerations);
            }, context.listener()::onFailure)
        );
    }

    /**
     * The bookkeeping that turns copied files into a snapshot, mirroring {@code
     * DeepSnapshotOrchestrationIT}'s own manual call exactly, with one difference: this reads {@link
     * RepositoryData#getGenId} fresh right before calling rather than caching it from an earlier
     * step, since any real gap between resolving the repository and finishing every shard's copy is
     * exactly the window a concurrent repository write could move the generation in.
     *
     * <p><b>What this deliberately does not do</b>, stated rather than discovered later: it does not
     * replicate {@code SnapshotsService}'s in-memory tracking of concurrent in-progress snapshots
     * against the same repository, so two deep snapshots into the same repository at the same moment
     * race on {@code repositoryStateId} the way any two direct {@code Repository#finalizeSnapshot}
     * callers outside that service would -- one loses with a version-conflict-shaped failure and must
     * be retried. Every action under {@code retention.action} already scopes itself out of core's
     * real {@code _snapshot} machinery for the same reason (see {@link
     * org.opensearch.serverless.storage.retention.action.SnapshotPinAction}'s own javadoc); a single
     * shipped deep snapshot at a time per repository is round 006's own scope, not a limitation
     * introduced here.
     *
     * <p><b>The gated-index NPE this found.</b> {@code BlobStoreRepository#finalizeSnapshot} writes
     * each named index's own metadata into the repository, looking it up as {@code
     * clusterMetadata.index(name)} -- which answers null for a gated index, the same absence every
     * other consumer in this plugin has had to learn to route around. Resolving {@code indexMetadata}
     * through {@code AbsentIndexDescriptorSuppliers} folded into a copy of cluster metadata is what
     * makes that lookup answer instead of NPEing -- found by {@code IndexDeepSnapshotActionIT}'s gated
     * case, not reasoned in advance, the same "probe, do not reason" pattern this round's own
     * predecessors document repeatedly.
     *
     * <p><b>Why {@code indexMetadata} is resolved here rather than reused from {@link
     * #clusterManagerOperation}.</b> Found by round 2 of the bug hunt that landed this whole action: a
     * sequential multi-shard copy can run long enough for a mapping update or index deletion to land
     * mid-flight, and reusing the metadata object {@code clusterManagerOperation} resolved before that
     * loop started would silently snapshot stale mappings/settings -- or, for a deleted index, publish
     * metadata for something that no longer exists. Re-resolving fresh here, right before it is folded
     * into {@code clusterMetadata}, narrows that window to as small as this method can make it, the
     * same reasoning this method already applies to {@code repositoryData.getGenId()} one line below.
     */
    private void finalizeSnapshot(CopyContext context, ShardGenerations shardGenerations) {
        Repository repository = context.repository();
        SnapshotId snapshotId = context.snapshotId();
        int numberOfShards = context.numberOfShards();
        ActionListener<IndexDeepSnapshotResponse> listener = context.listener();
        IndexMetadata indexMetadata = AbsentIndexDescriptorSuppliers.metadataOrDescriptor(
            clusterService.state().metadata(),
            context.indexId().getName()
        );
        if (indexMetadata == null) {
            // The index was deleted while this copy was in flight -- every shard's bytes were
            // successfully copied, but there is nothing left to describe in the finalized snapshot's
            // own metadata, so this is reported as a failure rather than finalizing with stale data.
            listener.onFailure(new IndexNotFoundException(context.indexId().getName()));
            return;
        }
        repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
            // Metadata.builder(state) starts from the real cluster metadata rather than an empty one,
            // so a plain (non-gated) serverless index -- already present there -- is untouched, and
            // only the gated case gains the entry it was missing. indices(Map.of(...)) rather than
            // put(indexMetadata, false): put's identity short-circuit (indices.get(name) ==
            // indexMetadata) protects the ordinary-index case, since indexMetadata came straight out
            // of state.metadata() there, but for a gated index the resolved indexMetadata is a fresh
            // object every call, so the short-circuit never fires and put falls through to
            // publishDescriptorIfIncremental -- which unconditionally republishes the descriptor (and
            // appends a change-log entry) from metadata captured before the shard-copy loop ran,
            // silently clobbering a descriptor an operator may have legitimately changed since.
            // indices(Map) is a plain putAll with no publish hook either way, and does not bump the
            // metadata version: this Metadata is genuinely never published, only handed to one
            // repository call that reads it and discards it.
            Metadata clusterMetadata = Metadata.builder(clusterService.state().metadata())
                .indices(Map.of(indexMetadata.getIndex().getName(), indexMetadata))
                .build();
            long endTime = System.currentTimeMillis();
            SnapshotInfo snapshotInfo = new SnapshotInfo(
                snapshotId,
                List.of(context.indexId().getName()),
                Collections.emptyList(),
                context.startTime(),
                null,
                endTime,
                numberOfShards,
                Collections.emptyList(),
                false,
                Collections.emptyMap(),
                // Deliberately false: this is the deep, byte-copying tier, the opposite of what this
                // flag names for core's own remote-store shallow snapshot feature.
                false
            );
            repository.finalizeSnapshot(
                shardGenerations,
                repositoryData.getGenId(),
                clusterMetadata,
                snapshotInfo,
                Version.CURRENT,
                state -> state,
                ActionListener.wrap(
                    finalizedData -> listener.onResponse(
                        new IndexDeepSnapshotResponse(snapshotId.getName(), snapshotId.getUUID(), numberOfShards)
                    ),
                    listener::onFailure
                )
            );
        }, e -> {
            logger.warn("could not read repository data to finalize deep snapshot [{}]", snapshotId, e);
            listener.onFailure(e);
        }));
    }
}
