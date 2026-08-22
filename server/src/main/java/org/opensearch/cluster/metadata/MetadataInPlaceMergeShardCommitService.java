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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.Set;

/**
 * The commit/cancel driver for the two-phase in-place shard <em>merge</em> -- the exact mirror of
 * {@link MetadataInPlaceSplitShardCommitService} for split. {@link MetadataInPlaceMergeShardService}
 * performs only phase 1: it marks a merge <em>pending</em> ({@code inProgressMergeParentShardIds}) and
 * revives the parent as an UNASSIGNED primary recovering via
 * {@link org.opensearch.cluster.routing.RecoverySource.InPlaceMergeShardRecoverySource}, while leaving
 * the children fully live, routed, and serving their hash ranges. This service watches cluster state
 * and drives each pending merge to one of two terminal outcomes:
 *
 * <ul>
 *   <li><b>commit</b> -- once the revived parent's primary reaches {@code STARTED}, the merge is
 *       finalized: the children are removed from {@link SplitShardsMetadata} via
 *       {@link SplitShardsMetadata.Builder#mergeChildrenBackToParent(int)} and their routing entries are
 *       retired, in the same cluster-state update. This is the only point at which anything is destroyed,
 *       and it happens only after the parent has confirmed it holds all the data. (Retiring the children
 *       atomically with the parent becoming the sole owner of the range mirrors split's own commit, which
 *       retires the parent atomically with its children taking over -- so a range is owned by exactly one
 *       search-visible shard across the transition.)</li>
 *   <li><b>cancel (automatic rollback)</b> -- if the revived parent instead exhausts the same
 *       allocation-retry budget {@link MaxRetryAllocationDecider} uses to permanently give up on a shard
 *       ({@code UnassignedInfo#getNumFailedAllocations() >= index.allocation.max_retries}), the pending
 *       merge is cancelled via {@link SplitShardsMetadata.Builder#cancelMerge(int)} and the parent's
 *       revived (and now permanently unassigned) routing entry is removed. Because phase 1 never removed
 *       the children, they are still active, routed, and range-owning -- so the shard range is served by
 *       them exactly as it was before the merge was attempted. This is a genuine, lossless rollback, not
 *       merely a logged failure.</li>
 * </ul>
 *
 * <p>Cluster-manager-only, modeled on {@link MetadataInPlaceSplitShardCommitService}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MetadataInPlaceMergeShardCommitService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceMergeShardCommitService.class);

    private final ClusterService clusterService;

    public MetadataInPlaceMergeShardCommitService(Settings settings, ClusterService clusterService) {
        this.clusterService = clusterService;
        if (DiscoveryNode.isClusterManagerNode(settings)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeClusterManager() == false) {
            return;
        }
        ClusterState state = event.state();
        for (IndexMetadata indexMetadata : state.metadata()) {
            SplitShardsMetadata splitShardsMetadata = indexMetadata.getSplitShardsMetadata();
            for (Integer parentShardId : splitShardsMetadata.getInProgressMergeParentShardIds()) {
                MergeCompletionState completionState = evaluateMergeCompletion(state, indexMetadata, parentShardId);
                if (completionState == MergeCompletionState.READY_TO_COMMIT) {
                    submitCommit(indexMetadata.getIndex().getName(), parentShardId);
                } else if (completionState == MergeCompletionState.SHOULD_CANCEL) {
                    submitCancel(indexMetadata.getIndex().getName(), parentShardId);
                }
            }
        }
    }

    enum MergeCompletionState {
        STILL_IN_PROGRESS,
        READY_TO_COMMIT,
        SHOULD_CANCEL
    }

    /**
     * Classifies a pending merge from the revived parent primary's routing state: STARTED means the parent
     * holds the merged data and the children can be retired (commit); an unassigned parent that has failed
     * allocation at least {@code index.allocation.max_retries} times will never recover (cancel/rollback);
     * anything else is still in flight.
     */
    static MergeCompletionState evaluateMergeCompletion(ClusterState state, IndexMetadata indexMetadata, int parentShardId) {
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex().getName());
        if (indexRoutingTable == null) {
            return MergeCompletionState.STILL_IN_PROGRESS;
        }
        IndexShardRoutingTable parentShardTable = indexRoutingTable.shard(parentShardId);
        if (parentShardTable == null) {
            return MergeCompletionState.STILL_IN_PROGRESS;
        }
        ShardRouting parentPrimary = parentShardTable.primaryShard();
        if (parentPrimary == null) {
            return MergeCompletionState.STILL_IN_PROGRESS;
        }
        if (parentPrimary.started()) {
            return MergeCompletionState.READY_TO_COMMIT;
        }
        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(indexMetadata.getSettings());
        if (parentPrimary.unassigned()
            && parentPrimary.unassignedInfo() != null
            && parentPrimary.unassignedInfo().getNumFailedAllocations() >= maxRetries) {
            return MergeCompletionState.SHOULD_CANCEL;
        }
        return MergeCompletionState.STILL_IN_PROGRESS;
    }

    private void submitCommit(String indexName, int parentShardId) {
        clusterService.submitStateUpdateTask(
            "commit in-place merge of shard [" + parentShardId + "] of index [" + indexName + "]",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyCommit(currentState, indexName, parentShardId);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to commit in-place merge of shard [{}] of index [{}]",
                            parentShardId,
                            indexName
                        ),
                        e
                    );
                }
            }
        );
    }

    private void submitCancel(String indexName, int parentShardId) {
        clusterService.submitStateUpdateTask(
            "cancel in-place merge of shard ["
                + parentShardId
                + "] of index ["
                + indexName
                + "] (revived parent allocation retries exhausted)",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyCancel(currentState, indexName, parentShardId);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to cancel in-place merge of shard [{}] of index [{}]",
                            parentShardId,
                            indexName
                        ),
                        e
                    );
                }
            }
        );
    }

    /**
     * Commits a pending merge whose revived parent primary has reached {@code STARTED}: removes the
     * children from {@link SplitShardsMetadata} via
     * {@link SplitShardsMetadata.Builder#mergeChildrenBackToParent(int)} (which also clears the pending
     * marker) and retires their routing entries, in the very same cluster-state update -- so the range is
     * owned by exactly one search-visible shard (the parent now, the children before) across the boundary.
     * Re-validates the completion condition against the state actually being applied to, since cluster
     * state may have advanced between {@link #clusterChanged} firing and this task executing.
     */
    static ClusterState applyCommit(ClusterState currentState, String indexName, int parentShardId) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(indexName);
        if (curIndexMetadata == null || curIndexMetadata.getSplitShardsMetadata().isMergeOfShardInProgress(parentShardId) == false) {
            return currentState;
        }
        if (evaluateMergeCompletion(currentState, curIndexMetadata, parentShardId) != MergeCompletionState.READY_TO_COMMIT) {
            return currentState;
        }

        Set<Integer> childIds = curIndexMetadata.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId);
        SplitShardsMetadata.Builder mergeMetadataBuilder = new SplitShardsMetadata.Builder(curIndexMetadata.getSplitShardsMetadata());
        mergeMetadataBuilder.mergeChildrenBackToParent(parentShardId);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata)
            .splitShardsMetadata(mergeMetadataBuilder.build());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);

        Index index = curIndexMetadata.getIndex();
        IndexRoutingTable currentIndexRoutingTable = currentState.routingTable().index(indexName);
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
            if (childIds.contains(shardTable.shardId().id()) == false) {
                indexRoutingTableBuilder.addIndexShard(shardTable);
            }
        }
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        routingTableBuilder.add(indexRoutingTableBuilder);

        logger.info(
            "in-place merge of shard [{}] of index [{}] committed: revived parent is STARTED, children {} retired",
            parentShardId,
            indexName,
            childIds
        );
        return ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).build();
    }

    /**
     * Rolls back a pending merge whose revived parent primary permanently exhausted its allocation-retry
     * budget: drops the pending marker via {@link SplitShardsMetadata.Builder#cancelMerge(int)} and removes
     * the parent's (permanently unassigned) revived routing entry. The children were never removed, so they
     * remain active, routed, and serving their hash ranges -- the range is fully servable through them
     * again, exactly as before the merge was attempted.
     *
     * <p>Re-validates the cancel condition against the state actually being applied to (not just the state
     * that triggered the listener), symmetrically with {@link #applyCommit}. Cluster state may have advanced
     * between {@link #clusterChanged} firing and this task executing: e.g. an operator's {@code retry_failed}
     * reroute could have allocated the revived parent and driven it to {@code STARTED} (now {@code
     * READY_TO_COMMIT}) after this cancel was queued but before it ran. In that case we no-op and let the
     * queued commit task retire the children instead of discarding a parent that already holds the data.
     */
    static ClusterState applyCancel(ClusterState currentState, String indexName, int parentShardId) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(indexName);
        if (curIndexMetadata == null || curIndexMetadata.getSplitShardsMetadata().isMergeOfShardInProgress(parentShardId) == false) {
            return currentState;
        }
        if (evaluateMergeCompletion(currentState, curIndexMetadata, parentShardId) != MergeCompletionState.SHOULD_CANCEL) {
            return currentState;
        }

        SplitShardsMetadata.Builder mergeMetadataBuilder = new SplitShardsMetadata.Builder(curIndexMetadata.getSplitShardsMetadata());
        Set<Integer> childIds = curIndexMetadata.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId);
        mergeMetadataBuilder.cancelMerge(parentShardId);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata)
            .splitShardsMetadata(mergeMetadataBuilder.build());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);

        Index index = curIndexMetadata.getIndex();
        IndexRoutingTable currentIndexRoutingTable = currentState.routingTable().index(indexName);
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
            // Drop only the revived parent's routing entry; every child's entry stays live and serving.
            if (shardTable.shardId().id() != parentShardId) {
                indexRoutingTableBuilder.addIndexShard(shardTable);
            }
        }
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        routingTableBuilder.add(indexRoutingTableBuilder);

        logger.warn(
            "In-place merge of shard [{}] of index [{}] rolled back automatically: the revived parent primary exhausted its "
                + "allocation-retry budget and could not recover. The children {} were never removed, so they remain live and "
                + "continue to serve the shard's hash range -- no data is lost and no operator action is required.",
            parentShardId,
            indexName,
            childIds
        );
        return ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).build();
    }
}
