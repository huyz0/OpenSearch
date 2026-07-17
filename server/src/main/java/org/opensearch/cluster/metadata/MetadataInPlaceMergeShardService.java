/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceMergeShardClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterManagerTask;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Service responsible for applying in-place shard merge requests to cluster state -- the reverse of
 * {@link MetadataInPlaceSplitShardService}. A merge reverses an earlier split by reviving the single
 * parent shard and, once it has recovered, retiring that split's children.
 *
 * <p>Structurally symmetric to split: this service performs only <em>phase 1</em>, marking the merge
 * pending ({@link SplitShardsMetadata.Builder#startMergeChildrenToParent(int)}) and reviving the
 * parent as one more UNASSIGNED primary, while KEEPING the children live and serving. The children are
 * only actually removed -- or the pending merge rolled back if the parent fails to recover -- by the
 * asynchronous commit/cancel driver {@link MetadataInPlaceMergeShardCommitService}, exactly as split's
 * children are committed/cancelled by {@link MetadataInPlaceSplitShardCommitService}. Nothing is
 * destroyed until the revived parent reaches STARTED, so a failed revival is a lossless rollback.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MetadataInPlaceMergeShardService {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceMergeShardService.class);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final ClusterManagerTaskThrottler.ThrottlingKey mergeShardTaskKey;

    public MetadataInPlaceMergeShardService(final ClusterService clusterService, final AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.mergeShardTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTask.IN_PLACE_MERGE_SHARD, true);
    }

    /**
     * Submits a cluster state update task to merge an earlier split's children back into their parent.
     */
    public void merge(final InPlaceMergeShardClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
            "in-place-merge-shard ["
                + request.getParentShardId()
                + "] of index ["
                + request.getIndex()
                + "], cause ["
                + request.cause()
                + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return mergeShardTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyMergeShardRequest(currentState, request, allocationService::reroute);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "[{}] of index [{}] failed to merge in place",
                            request.getParentShardId(),
                            request.getIndex()
                        ),
                        e
                    );
                    super.onFailure(source, e);
                }
            }
        );
    }

    /**
     * Applies phase 1 of a shard merge request to the given cluster state: marks the merge pending in the
     * index's {@link SplitShardsMetadata} via
     * {@link SplitShardsMetadata.Builder#startMergeChildrenToParent(int)}, KEEPS the children's
     * {@link IndexRoutingTable} entries live, revives a single UNASSIGNED primary for the parent
     * (recovering via {@link RecoverySource.InPlaceMergeShardRecoverySource}), and triggers a reroute --
     * the mirror image of {@link MetadataInPlaceSplitShardService#applySplitShardRequest}, which likewise
     * only initiates the operation and leaves commit/cancel to its async driver.
     */
    static ClusterState applyMergeShardRequest(
        ClusterState currentState,
        InPlaceMergeShardClusterStateUpdateRequest request,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable
    ) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(request.getIndex());
        if (curIndexMetadata == null) {
            throw new IllegalArgumentException("Index [" + request.getIndex() + "] not found");
        }

        if (curIndexMetadata.getNumberOfVirtualShards() != -1) {
            throw new IllegalArgumentException(
                "In-place shard merge is not supported on index [" + request.getIndex() + "] with virtual shards enabled"
            );
        }

        if (currentState.nodes().getMinNodeVersion().equals(currentState.nodes().getMaxNodeVersion()) == false
            || currentState.nodes().getMinNodeVersion().before(Version.V_3_7_0)) {
            throw new IllegalArgumentException(
                "In-place shard merge requires all nodes to be on the same version, at or above " + Version.V_3_7_0
            );
        }

        int parentShardId = request.getParentShardId();
        SplitShardsMetadata splitShardsMetadata = curIndexMetadata.getSplitShardsMetadata();

        // The direct children this parent's own split produced (captured against the current
        // metadata, before the merge de-commits it below, so the routing retirement further down
        // still knows which entries to retire). Each ShardRange carries its own child shard id
        // alongside the hash range: the ids drive the routing retirement below, and the whole
        // ShardRange list rides on the revived parent's InPlaceMergeShardRecoverySource so the
        // engine hook can still find each child's blob container and hash range after this update
        // has erased them from SplitShardsMetadata (see that recovery source's own javadoc).
        ShardRange[] childRanges = splitShardsMetadata.getChildShardsOfParent(parentShardId);
        Set<Integer> childShardIds = splitShardsMetadata.getChildShardIdsOfParent(parentShardId);

        // Mark the merge as *pending* -- phase 1 of the two-phase in-place merge. This validates every
        // split-level precondition (parent really is a split parent, the split is not still in progress,
        // and no child has itself been split further) and throws a clean IllegalArgumentException -- not a
        // raw assertion -- for any violation, so those precise errors take priority over the per-child
        // liveness check below. Crucially it does NOT yet remove the children: they stay recorded in
        // SplitShardsMetadata, keep their routing entries, and keep serving their hash ranges. The children
        // are only actually torn down once the revived parent reaches STARTED, at which point
        // MetadataInPlaceMergeShardCommitService finalizes the merge (mergeChildrenBackToParent). If the
        // revived parent instead exhausts its allocation retries, that same commit service cancels the
        // pending merge and the children -- never destroyed -- remain fully live: a lossless rollback.
        SplitShardsMetadata.Builder mergeMetadataBuilder = new SplitShardsMetadata.Builder(splitShardsMetadata);
        mergeMetadataBuilder.startMergeChildrenToParent(parentShardId);
        SplitShardsMetadata updatedSplitShardsMetadata = mergeMetadataBuilder.build();

        // Every child being merged must be a live, started, non-relocating primary before its data
        // can be folded back into the revived parent -- checked only now that the split-level
        // preconditions above have confirmed this is a genuine, committed, mergeable split.
        for (int childShardId : childShardIds) {
            IndexShardRoutingTable childShardTable = currentState.routingTable()
                .index(curIndexMetadata.getIndex().getName())
                .shard(childShardId);
            if (childShardTable == null) {
                continue;
            }
            ShardRouting childPrimary = childShardTable.primaryShard();
            if (childPrimary.relocating()) {
                throw new IllegalArgumentException(
                    "Cannot merge child shard ["
                        + childShardId
                        + "] of ["
                        + parentShardId
                        + "] on index ["
                        + request.getIndex()
                        + "] because it is currently relocating"
                );
            }
            if (childPrimary.started() == false) {
                throw new IllegalArgumentException(
                    "Cannot merge child shard ["
                        + childShardId
                        + "] of ["
                        + parentShardId
                        + "] on index ["
                        + request.getIndex()
                        + "] because its primary shard is not started, current state: "
                        + childPrimary.state()
                );
            }
        }

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata)
            .splitShardsMetadata(updatedSplitShardsMetadata);
        // Reset the revived parent's in-sync allocation set to empty, mirroring how the split service
        // seeds each new child's set empty. The parent was a live, started primary before it was
        // split and retired, so its in-sync set still carries that stale pre-split primary's
        // allocation id. Left in place, IndexMetadataUpdater#updateInSyncAllocations would see a
        // brand-new primary (recovering via InPlaceMergeShardRecoverySource) whose fresh allocation
        // id is absent from a non-empty in-sync set, mistake it for a forced *stale-primary*
        // allocation, and assert -- an AssertionError on the cluster-manager thread, which (being an
        // Error, not an Exception) is not caught by the cluster-state task machinery and hangs the
        // request rather than failing it. The revived parent is a genuinely fresh local copy the
        // engine materializes by folding both children together, not a continuation of the stale
        // pre-split primary, so an empty in-sync set (repopulated when the revived primary starts) is
        // correct, not merely a workaround.
        indexMetadataBuilder.putInSyncAllocationIds(parentShardId, java.util.Collections.emptySet());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);

        // Rebuild the IndexRoutingTable KEEPING every child's routing entry live (they keep serving their
        // hash ranges throughout the pending merge, mirroring how split keeps the parent's entry live
        // until its children start), and add a single UNASSIGNED primary for the parent recovering via
        // InPlaceMergeShardRecoverySource. The children are retired only later, atomically with the
        // metadata commit, by MetadataInPlaceMergeShardCommitService#applyCommit -- the mirror of how
        // split's own commit service retires the parent. Leaving numberOfShards and the children's
        // inSyncAllocationIds/primaryTerm map entries untouched -- IndexMetadata.Builder#build preserves
        // those extra entries verbatim.
        Index index = curIndexMetadata.getIndex();
        IndexRoutingTable currentIndexRoutingTable = currentState.routingTable().index(index.getName());
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
            indexRoutingTableBuilder.addIndexShard(shardTable);
        }

        ShardId parentShard = new ShardId(index, parentShardId);
        indexRoutingTableBuilder.addShard(
            ShardRouting.newUnassigned(
                parentShard,
                true,
                new RecoverySource.InPlaceMergeShardRecoverySource(java.util.Arrays.asList(childRanges)),
                new UnassignedInfo(
                    UnassignedInfo.Reason.INDEX_CREATED,
                    "primary of parent shard revived by in-place merge of shard [" + parentShardId + "]"
                )
            )
        );
        int numberOfReplicas = curIndexMetadata.getNumberOfReplicas();
        for (int replica = 0; replica < numberOfReplicas; replica++) {
            indexRoutingTableBuilder.addShard(
                ShardRouting.newUnassigned(
                    parentShard,
                    false,
                    RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(
                        UnassignedInfo.Reason.INDEX_CREATED,
                        "replica of parent shard revived by in-place merge of shard [" + parentShardId + "]"
                    )
                )
            );
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        routingTableBuilder.add(indexRoutingTableBuilder);

        ClusterState updatedState = ClusterState.builder(currentState)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder.build())
            .build();
        return rerouteRoutingTable.apply(updatedState, "shard [" + parentShardId + "] of index [" + request.getIndex() + "] merged");
    }
}
