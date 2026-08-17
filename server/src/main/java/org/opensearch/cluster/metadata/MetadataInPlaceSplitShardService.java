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
import org.opensearch.action.admin.indices.split.InPlaceSplitShardClusterStateUpdateRequest;
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
import org.opensearch.index.seqno.SequenceNumbers;

import java.util.Collections;
import java.util.function.BiFunction;

/**
 * Service responsible for applying in-place shard split requests to cluster state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MetadataInPlaceSplitShardService {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceSplitShardService.class);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final ClusterManagerTaskThrottler.ThrottlingKey splitShardTaskKey;

    public MetadataInPlaceSplitShardService(final ClusterService clusterService, final AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.splitShardTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTask.IN_PLACE_SPLIT_SHARD, true);
    }

    /**
     * Submits a cluster state update task to split a shard in-place.
     */
    public void split(final InPlaceSplitShardClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
            "in-place-split-shard [" + request.getShardId() + "] of index [" + request.getIndex() + "], cause [" + request.cause() + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return splitShardTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applySplitShardRequest(currentState, request, allocationService::reroute);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "[{}] of index [{}] failed to split online",
                            request.getShardId(),
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
     * Applies a shard split request to the given cluster state. Updates the split metadata
     * in the index metadata and triggers a reroute so that child shards get allocated.
     */
    static ClusterState applySplitShardRequest(
        ClusterState currentState,
        InPlaceSplitShardClusterStateUpdateRequest request,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable
    ) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(request.getIndex());
        if (curIndexMetadata == null) {
            throw new IllegalArgumentException("Index [" + request.getIndex() + "] not found");
        }

        if (curIndexMetadata.getNumberOfVirtualShards() != -1) {
            throw new IllegalArgumentException(
                "In-place shard split is not supported on index [" + request.getIndex() + "] with virtual shards enabled"
            );
        }

        // An index whose placement is computed publishes no routing entry, and in-place resharding is
        // built on manipulating one: it adds child primaries, retires parent entries and drives the
        // commit off what the routing table reports. With nothing published, split reaches
        // shardRoutingTable and throws IndexNotFoundException, merge dereferences a null index routing
        // table, and both commit services read the absence as "still in progress" and stay pending
        // forever. Rejecting up front turns three bad failure modes into one clear sentence.
        //
        // Making resharding work under computed placement is a redesign rather than a fix, because the
        // children would have to be placed by the same function and the operation would have to reach
        // agreement without publishing anything. That is not attempted here.
        // Phase C4b of core-pluggability-refactor-plan.md: currentState.routingTable().shouldPublishRouting(...)
        // replaces AbsentIndexRoutingSuppliers.shouldPublishRouting(...) here -- same predicate, discovered
        // through the resolver attached to this state's own routing table.
        if (currentState.routingTable().shouldPublishRouting(curIndexMetadata) == false) {
            throw new IllegalArgumentException(
                "In-place shard split is not supported on index ["
                    + request.getIndex()
                    + "] because its shard placement is computed rather than published"
            );
        }

        // An index with index.routing_partition_size > 1 spreads each routing value across a
        // partitionOffset-shifted band of the hash space (see OperationRouting#generateShardId). The
        // in-place split read-path filter (InPlaceSplitPartitionFilter, used by the plugin's
        // InPlaceSplitFilteringDirectoryReader) hashes effectiveRouting only and cannot reproduce that
        // per-document partition offset, so a child would bucket partitioned documents by the wrong
        // hash. This is a hard, structural limitation of the filter -- reject the split up front rather
        // than silently corrupt search/GET visibility.
        if (curIndexMetadata.isRoutingPartitionedIndex()) {
            throw new IllegalArgumentException(
                "In-place shard split is not supported on index [" + request.getIndex() + "] with index.routing_partition_size > 1"
            );
        }

        if (currentState.nodes().getMinNodeVersion().equals(currentState.nodes().getMaxNodeVersion()) == false
            || currentState.nodes().getMinNodeVersion().before(Version.V_3_7_0)) {
            throw new IllegalArgumentException(
                "In-place shard split requires all nodes to be on the same version, at or above " + Version.V_3_7_0
            );
        }

        int shardId = request.getShardId();
        SplitShardsMetadata splitShardsMetadata = curIndexMetadata.getSplitShardsMetadata();

        if (splitShardsMetadata.getInProgressSplitShardIds().contains(shardId)) {
            throw new IllegalArgumentException("Splitting of shard [" + shardId + "] is already in progress");
        }

        if (splitShardsMetadata.isSplitParent(shardId)) {
            throw new IllegalArgumentException("Shard [" + shardId + "] has already been split.");
        }

        // A shard that is currently a live child of an in-progress merge is pending destruction --
        // the merge's own commit step (SplitShardsMetadata.Builder#mergeChildrenBackToParent) requires
        // every direct child to still be an active, unsplit leaf, and will fail forever (with no
        // automatic cancellation, since the merge's revived parent may allocate successfully) if this
        // shard splits out from under it. Reject up front rather than wedge the merge permanently.
        if (splitShardsMetadata.isChildOfInProgressMerge(shardId)) {
            throw new IllegalArgumentException("Cannot split shard [" + shardId + "] because it is a child of an in-progress merge");
        }

        ShardRouting primaryShard = currentState.routingTable()
            .shardRoutingTable(curIndexMetadata.getIndex().getName(), shardId)
            .primaryShard();
        if (primaryShard.relocating()) {
            throw new IllegalArgumentException(
                "Cannot split shard [" + shardId + "] on index [" + request.getIndex() + "] because it is currently relocating"
            );
        }
        if (primaryShard.started() == false) {
            throw new IllegalArgumentException(
                "Cannot split shard ["
                    + shardId
                    + "] on index ["
                    + request.getIndex()
                    + "] because the primary shard is not started, current state: "
                    + primaryShard.state()
            );
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata);

        SplitShardsMetadata.Builder splitMetadataBuilder = new SplitShardsMetadata.Builder(splitShardsMetadata);
        splitMetadataBuilder.splitShard(shardId, request.getSplitInto());
        SplitShardsMetadata updatedSplitShardsMetadata = splitMetadataBuilder.build();
        indexMetadataBuilder.splitShardsMetadata(updatedSplitShardsMetadata);

        // Give each reserved child shard ID a real, UNASSIGNED ShardRouting in the same cluster-state-update
        // task that records the split in SplitShardsMetadata, mirroring how classic resize (split/shrink/clone)
        // commits its new index's IndexMetadata and IndexRoutingTable together in one step
        // (MetadataCreateIndexService#clusterStateCreateIndex) rather than growing the routing table
        // incrementally across multiple cluster-state updates. The child primary recovers via
        // InPlaceSplitShardRecoverySource (an existing, previously-unused recovery source shaped after
        // LocalShardsRecoverySource); replicas recover from that primary the ordinary way, via PEER.
        Index index = curIndexMetadata.getIndex();
        IndexRoutingTable existingRoutingTable = currentState.routingTable().index(index.getName());
        if (existingRoutingTable == null) {
            throw new IllegalStateException(
                "cannot split shards of index [" + index.getName() + "]: it has no routing table to split from"
            );
        }
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable existingShardTable : existingRoutingTable) {
            indexRoutingTableBuilder.addIndexShard(existingShardTable);
        }
        int numberOfReplicas = curIndexMetadata.getNumberOfReplicas();
        for (ShardRange childRange : updatedSplitShardsMetadata.getChildShardsOfParent(shardId)) {
            ShardId childShardId = new ShardId(index, childRange.shardId());
            // A child's shard id is >= curIndexMetadata.getNumberOfShards() by construction (SplitShardsMetadata
            // reserves child ids beyond the original shard count, deliberately without growing numberOfShards --
            // see IndexMetadata#inSyncAllocationIds's own javadoc for why). Seed both maps explicitly here so
            // IndexMetadata.Builder#build's per-shard validation (which only requires entries for shards
            // < numberOfShards, but preserves anything else verbatim) has real, present entries for the child
            // from the moment it's reserved, rather than relying on a later IndexMetadataUpdater update to add
            // them lazily.
            indexMetadataBuilder.putInSyncAllocationIds(childRange.shardId(), Collections.emptySet());
            indexMetadataBuilder.primaryTerm(childRange.shardId(), SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            indexRoutingTableBuilder.addShard(
                ShardRouting.newUnassigned(
                    childShardId,
                    true,
                    RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE,
                    new UnassignedInfo(
                        UnassignedInfo.Reason.INDEX_CREATED,
                        "primary of child shard created by in-place split of shard [" + shardId + "]"
                    )
                )
            );
            for (int replica = 0; replica < numberOfReplicas; replica++) {
                indexRoutingTableBuilder.addShard(
                    ShardRouting.newUnassigned(
                        childShardId,
                        false,
                        RecoverySource.PeerRecoverySource.INSTANCE,
                        new UnassignedInfo(
                            UnassignedInfo.Reason.INDEX_CREATED,
                            "replica of child shard created by in-place split of shard [" + shardId + "]"
                        )
                    )
                );
            }
        }
        routingTableBuilder.add(indexRoutingTableBuilder);

        RoutingTable routingTable = routingTableBuilder.build();
        metadataBuilder.put(indexMetadataBuilder);

        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTable).build();
        return rerouteRoutingTable.apply(updatedState, "shard [" + shardId + "] of index [" + request.getIndex() + "] split");
    }
}
