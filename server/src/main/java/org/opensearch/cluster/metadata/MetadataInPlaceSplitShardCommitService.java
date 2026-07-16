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
 * Closes the gap left by {@link MetadataInPlaceSplitShardService}: nothing in production code ever
 * finalizes or aborts an in-progress in-place split once it's been recorded in {@link SplitShardsMetadata}.
 * This is a cluster-manager-only {@link ClusterStateListener}, modeled on the same pattern
 * {@code org.opensearch.persistent.PersistentTasksClusterService} uses for "watch cluster state, react by
 * submitting a follow-up cluster-state-update task" -- there is no more specific "shard reached STARTED,
 * now do X on the cluster-manager" hook anywhere in this codebase to reuse instead (verified: neither
 * {@code AllocationService} nor {@code GatewayAllocator} expose one).
 *
 * <p>On every cluster state change, for every in-progress split recorded in {@link SplitShardsMetadata}:
 * <ul>
 *   <li>if every reserved child shard's primary {@link ShardRouting} has reached
 *       {@code ShardRoutingState.STARTED}, the split is committed via
 *       {@link SplitShardsMetadata.Builder#updateSplitMetadataForChildShards}.</li>
 *   <li>if any reserved child shard's primary has exhausted the same allocation-retry budget
 *       {@link MaxRetryAllocationDecider} itself uses to permanently give up on a shard
 *       ({@code UnassignedInfo#getNumFailedAllocations() >= SETTING_ALLOCATION_MAX_RETRY}), the split is
 *       cancelled via {@link SplitShardsMetadata.Builder#cancelSplit}, and the abandoned children's
 *       {@link ShardRouting} entries are removed from the routing table so their reserved shard IDs can be
 *       reused by a future split ({@link SplitShardsMetadata.Builder#splitShard}'s hole-reuse logic depends
 *       on the ID not still being referenced by a live routing entry).</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MetadataInPlaceSplitShardCommitService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceSplitShardCommitService.class);

    private final ClusterService clusterService;

    public MetadataInPlaceSplitShardCommitService(Settings settings, ClusterService clusterService) {
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
            for (Integer parentShardId : splitShardsMetadata.getInProgressSplitShardIds()) {
                SplitCompletionState completionState = evaluateSplitCompletion(state, indexMetadata, parentShardId);
                if (completionState == SplitCompletionState.READY_TO_COMMIT) {
                    submitCommit(indexMetadata.getIndex().getName(), parentShardId);
                } else if (completionState == SplitCompletionState.SHOULD_CANCEL) {
                    submitCancel(indexMetadata.getIndex().getName(), parentShardId);
                }
            }
        }
    }

    private enum SplitCompletionState {
        STILL_IN_PROGRESS,
        READY_TO_COMMIT,
        SHOULD_CANCEL
    }

    private static SplitCompletionState evaluateSplitCompletion(ClusterState state, IndexMetadata indexMetadata, int parentShardId) {
        ShardRange[] childRanges = indexMetadata.getSplitShardsMetadata().getChildShardsOfParent(parentShardId);
        IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex().getName());
        if (indexRoutingTable == null) {
            return SplitCompletionState.STILL_IN_PROGRESS;
        }

        int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(indexMetadata.getSettings());
        boolean allStarted = true;
        for (ShardRange childRange : childRanges) {
            IndexShardRoutingTable childShardTable = indexRoutingTable.shard(childRange.shardId());
            if (childShardTable == null) {
                return SplitCompletionState.STILL_IN_PROGRESS;
            }
            ShardRouting childPrimary = childShardTable.primaryShard();
            if (childPrimary.unassigned()
                && childPrimary.unassignedInfo() != null
                && childPrimary.unassignedInfo().getNumFailedAllocations() >= maxRetries) {
                return SplitCompletionState.SHOULD_CANCEL;
            }
            if (childPrimary.started() == false) {
                allStarted = false;
            }
        }
        return allStarted ? SplitCompletionState.READY_TO_COMMIT : SplitCompletionState.STILL_IN_PROGRESS;
    }

    private void submitCommit(String indexName, int parentShardId) {
        clusterService.submitStateUpdateTask(
            "commit in-place split of shard [" + parentShardId + "] of index [" + indexName + "]",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyCommit(currentState, indexName, parentShardId);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to commit in-place split of shard [{}] of index [{}]",
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
            "cancel in-place split of shard [" + parentShardId + "] of index [" + indexName + "] (child allocation retries exhausted)",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applyCancel(currentState, indexName, parentShardId);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage(
                            "failed to cancel in-place split of shard [{}] of index [{}]",
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
     * Commits an in-progress split whose child shards have all reached {@code STARTED}: promotes the
     * reserved child shard IDs to active in {@link SplitShardsMetadata}, and, in the very same
     * cluster-state update, removes the parent shard's own {@link ShardRouting} entries (primary and
     * every replica) from the routing table. Re-validates the completion condition against the state
     * actually being applied to (not just the state that triggered the listener), since cluster state
     * may have advanced between {@link #clusterChanged} firing and this task executing.
     *
     * <p><b>Why the parent is retired here, atomically with the metadata commit</b> --
     * dynamic-partitioning-plan.md's Part 3 "read-path correctness during the split transition window"
     * question, resolved as option (a) there: a splitting child's manifest, until a physical bundle
     * rewrite happens (not yet built -- see {@code PartitionRewriteSchedulerTask}'s equivalent for
     * this plugin's older split mechanism, no counterpart exists yet for in-place split), references
     * the *same* underlying bundle files the parent's manifest does. If both parent and children were
     * ever simultaneously {@code STARTED} and therefore both search-visible, a search fanning out to
     * every active shard in the routing table (core's ordinary behavior, unmodified by this feature)
     * would double-count every document: once via the parent's full pre-split view, once via
     * whichever child's hash range it falls into. Removing the parent's routing entry in the same
     * cluster-state update the children's `SplitShardsMetadata` promotion lands in (not a
     * separate, later step) closes that window at the only point where it can be closed atomically --
     * a partition is owned by exactly one visible shard at a time, even across the split boundary,
     * matching the plan's own "DynamoDB-like" resolution. This intentionally shuts down the parent's
     * {@code IndexShard} on whatever node(s) hosted it (ordinary core behavior when a routing entry
     * disappears) -- safe, since the parent's own manifest bytes are still referenced (and protected
     * from GC by the pin {@code ShardCloner.clone} placed during each child's {@code
     * Engine#recoverFromInPlaceSplit}) by the children that have just taken over its range.
     */
    static ClusterState applyCommit(ClusterState currentState, String indexName, int parentShardId) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(indexName);
        if (curIndexMetadata == null || curIndexMetadata.getSplitShardsMetadata().isSplitOfShardInProgress(parentShardId) == false) {
            return currentState;
        }
        if (evaluateSplitCompletion(currentState, curIndexMetadata, parentShardId) != SplitCompletionState.READY_TO_COMMIT) {
            return currentState;
        }

        Set<Integer> childIds = curIndexMetadata.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId);
        SplitShardsMetadata.Builder splitMetadataBuilder = new SplitShardsMetadata.Builder(curIndexMetadata.getSplitShardsMetadata());
        splitMetadataBuilder.updateSplitMetadataForChildShards(parentShardId, childIds);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata)
            .splitShardsMetadata(splitMetadataBuilder.build());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata()).put(indexMetadataBuilder);

        Index index = curIndexMetadata.getIndex();
        IndexRoutingTable currentIndexRoutingTable = currentState.routingTable().index(indexName);
        IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
        for (IndexShardRoutingTable shardTable : currentIndexRoutingTable) {
            if (shardTable.shardId().id() != parentShardId) {
                indexRoutingTableBuilder.addIndexShard(shardTable);
            }
        }
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        routingTableBuilder.add(indexRoutingTableBuilder);

        return ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).build();
    }

    /**
     * Cancels an in-progress split after a child shard permanently exhausted its allocation-retry budget:
     * frees the reserved child shard IDs in {@link SplitShardsMetadata} and removes their now-abandoned
     * {@link ShardRouting} entries from the routing table, so a later split of the same parent doesn't
     * collide with routing entries this attempt already created.
     */
    static ClusterState applyCancel(ClusterState currentState, String indexName, int parentShardId) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(indexName);
        if (curIndexMetadata == null || curIndexMetadata.getSplitShardsMetadata().isSplitOfShardInProgress(parentShardId) == false) {
            return currentState;
        }

        Set<Integer> childIds = curIndexMetadata.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId);
        SplitShardsMetadata.Builder splitMetadataBuilder = new SplitShardsMetadata.Builder(curIndexMetadata.getSplitShardsMetadata());
        splitMetadataBuilder.cancelSplit(parentShardId);

        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata)
            .splitShardsMetadata(splitMetadataBuilder.build());
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

        return ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTableBuilder.build()).build();
    }
}
