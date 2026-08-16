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

        String indexUuid = indexMetadata.getIndexUUID();
        int numberOfShards = indexMetadata.getNumberOfShards();
        IndexId indexId = new IndexId(request.indexName(), indexUuid);
        SnapshotId snapshotId = new SnapshotId(request.snapshotName(), UUIDs.randomBase64UUID());
        long startTime = System.currentTimeMillis();

        copyShard(
            0,
            numberOfShards,
            indexId,
            indexUuid,
            request.repositoryName(),
            snapshotId,
            repository,
            indexMetadata,
            ShardGenerations.builder(),
            startTime,
            listener
        );
    }

    private void copyShard(
        int shardId,
        int numberOfShards,
        IndexId indexId,
        String indexUuid,
        String repositoryName,
        SnapshotId snapshotId,
        Repository repository,
        IndexMetadata indexMetadata,
        ShardGenerations.Builder shardGenerations,
        long startTime,
        ActionListener<IndexDeepSnapshotResponse> listener
    ) {
        if (shardId == numberOfShards) {
            finalizeSnapshot(indexId, numberOfShards, snapshotId, repository, indexMetadata, shardGenerations.build(), startTime, listener);
            return;
        }
        client.execute(
            ShardDeepSnapshotAction.INSTANCE,
            new ShardDeepSnapshotRequest(indexId.getName(), indexUuid, shardId, repositoryName, snapshotId.getName(), snapshotId.getUUID()),
            ActionListener.wrap(response -> {
                shardGenerations.put(indexId, shardId, response.shardGeneration());
                copyShard(
                    shardId + 1,
                    numberOfShards,
                    indexId,
                    indexUuid,
                    repositoryName,
                    snapshotId,
                    repository,
                    indexMetadata,
                    shardGenerations,
                    startTime,
                    listener
                );
            }, listener::onFailure)
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
     * other consumer in this plugin has had to learn to route around. Passing {@code
     * indexMetadata} (already resolved through {@code AbsentIndexDescriptorSuppliers} in {@link
     * #clusterManagerOperation}) folded into a copy of cluster metadata is what makes that lookup
     * answer instead of NPEing -- found by {@code IndexDeepSnapshotActionIT}'s gated case, not
     * reasoned in advance, the same "probe, do not reason" pattern this round's own predecessors
     * document repeatedly.
     */
    private void finalizeSnapshot(
        IndexId indexId,
        int numberOfShards,
        SnapshotId snapshotId,
        Repository repository,
        IndexMetadata indexMetadata,
        ShardGenerations shardGenerations,
        long startTime,
        ActionListener<IndexDeepSnapshotResponse> listener
    ) {
        repository.getRepositoryData(ActionListener.wrap(repositoryData -> {
            // Metadata.builder(state) starts from the real cluster metadata rather than an empty
            // one, so a plain (non-gated) serverless index -- already present there -- is
            // untouched, and only the gated case gains the entry it was missing. put(..., false)
            // does not bump the metadata version: this Metadata is never published, only handed to
            // one repository call that reads it and discards it.
            Metadata clusterMetadata = Metadata.builder(clusterService.state().metadata()).put(indexMetadata, false).build();
            long endTime = System.currentTimeMillis();
            SnapshotInfo snapshotInfo = new SnapshotInfo(
                snapshotId,
                List.of(indexId.getName()),
                Collections.emptyList(),
                startTime,
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
