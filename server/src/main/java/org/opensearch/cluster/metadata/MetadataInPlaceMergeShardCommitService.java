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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Surfaces the one failure mode {@link MetadataInPlaceMergeShardService} cannot itself recover from:
 * a revived parent primary that never manages to recover.
 *
 * <p>An in-place merge de-commits the split synchronously and revives the parent as a single
 * {@code UNASSIGNED} primary recovering via {@link RecoverySource.InPlaceMergeShardRecoverySource}.
 * If that primary then exhausts its allocation-retry budget (the same
 * {@code UnassignedInfo#getNumFailedAllocations() >= }{@link MaxRetryAllocationDecider}'s
 * {@code SETTING_ALLOCATION_MAX_RETRY} threshold split's own commit service uses to give up on a
 * child), the shard is left permanently red. Split has a symmetric rollback for this exact case
 * ({@link MetadataInPlaceSplitShardCommitService}'s {@code SHOULD_CANCEL} path), but a merge cannot
 * reuse that shape: split watches a persistent {@code inProgressSplitShardIds} marker that survives
 * across the recovery window, whereas a merge erases all trace of the children from
 * {@link SplitShardsMetadata} in the very same cluster-state update that revives the parent, so there
 * is no "merge in progress" state left to drive an automatic re-split from.
 *
 * <p><b>Scope, stated honestly.</b> A full automatic rollback (re-establish the split's metadata and
 * re-create the children's routing so their still-present data becomes reachable again) is possible
 * in principle -- the children's blob containers persist until GC, and the revived parent's recovery
 * source still carries their {@link ShardRange}s -- but it would require a new persistent
 * "merge pending" concept in cluster state (a new field, wire-format and XContent gating, and a
 * commit/cancel driver to restore from a pre-merge snapshot). That is deferred as future work. This
 * service lands the narrower, honest fix in the meantime: detect the stuck-parent condition and make
 * it <em>loud and operator-actionable</em> -- a distinct WARN naming the parent, the retired
 * children, and the fact that their data is still recoverable from their containers -- rather than
 * leaving a silently-red shard with no explanation of what happened or what can be done about it.
 *
 * <p>Cluster-manager-only, modeled on {@link MetadataInPlaceSplitShardCommitService}. The warning is
 * de-duplicated per stuck parent so it fires once, not on every subsequent cluster-state change,
 * and is re-armed if the same parent later recovers (or the index goes away) and gets stuck again.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MetadataInPlaceMergeShardCommitService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceMergeShardCommitService.class);

    private final ClusterService clusterService;

    /** Parents already warned about, so the actionable WARN fires once per stuck parent, not per cluster-state change. */
    private final Set<ShardId> warnedStuckParents = new HashSet<>();

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
        List<StuckMergeParent> stuck = findStuckMergeParents(event.state());

        // Re-arm any parent that is no longer stuck (recovered, or its index was deleted), so a future
        // stuck merge of the same shard id warns again rather than being suppressed forever.
        Set<ShardId> stillStuck = new HashSet<>();
        for (StuckMergeParent parent : stuck) {
            stillStuck.add(parent.parentShardId());
        }
        warnedStuckParents.retainAll(stillStuck);

        for (StuckMergeParent parent : stuck) {
            if (warnedStuckParents.add(parent.parentShardId())) {
                logger.warn(
                    "In-place merge of shard [{}] of index [{}] cannot complete: the revived parent primary has exhausted its "
                        + "allocation-retry budget and is permanently unassigned. Automatic rollback is not implemented, so the "
                        + "shard will stay red until an operator intervenes (e.g. address the allocation failure and retry via the "
                        + "cluster reroute API's retry_failed flag). The merged-away children {} are no longer in cluster state, but "
                        + "their underlying data has not been garbage-collected and remains recoverable from their blob containers.",
                    parent.parentShardId().id(),
                    parent.parentShardId().getIndexName(),
                    parent.retiredChildShardIds()
                );
            }
        }
    }

    /**
     * A revived in-place-merge parent primary that has permanently failed to recover: it is still
     * {@code UNASSIGNED}, still carries an {@link RecoverySource.InPlaceMergeShardRecoverySource}, and
     * has failed allocation at least {@code index.allocation.max_retries} times.
     */
    static List<StuckMergeParent> findStuckMergeParents(ClusterState state) {
        List<StuckMergeParent> stuck = new ArrayList<>();
        for (IndexMetadata indexMetadata : state.metadata()) {
            IndexRoutingTable indexRoutingTable = state.routingTable().index(indexMetadata.getIndex().getName());
            if (indexRoutingTable == null) {
                continue;
            }
            int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(indexMetadata.getSettings());
            for (IndexShardRoutingTable shardTable : indexRoutingTable) {
                ShardRouting primary = shardTable.primaryShard();
                if (primary == null || primary.unassigned() == false) {
                    continue;
                }
                if ((primary.recoverySource() instanceof RecoverySource.InPlaceMergeShardRecoverySource) == false) {
                    continue;
                }
                if (primary.unassignedInfo() == null || primary.unassignedInfo().getNumFailedAllocations() < maxRetries) {
                    continue;
                }
                RecoverySource.InPlaceMergeShardRecoverySource mergeSource = (RecoverySource.InPlaceMergeShardRecoverySource) primary
                    .recoverySource();
                List<Integer> childShardIds = new ArrayList<>();
                for (ShardRange child : mergeSource.children()) {
                    childShardIds.add(child.shardId());
                }
                stuck.add(new StuckMergeParent(primary.shardId(), childShardIds));
            }
        }
        return stuck;
    }

    /** A revived merge parent that exhausted its allocation retries, and the child shard ids it retired. */
    static final class StuckMergeParent {
        private final ShardId parentShardId;
        private final List<Integer> retiredChildShardIds;

        StuckMergeParent(ShardId parentShardId, List<Integer> retiredChildShardIds) {
            this.parentShardId = parentShardId;
            this.retiredChildShardIds = retiredChildShardIds;
        }

        ShardId parentShardId() {
            return parentShardId;
        }

        List<Integer> retiredChildShardIds() {
            return retiredChildShardIds;
        }
    }
}
