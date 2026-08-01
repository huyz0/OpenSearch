/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ComputedPlacementMembership;
import org.opensearch.cluster.routing.ComputedPlacementMembershipService;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds an {@link IndexRoutingTable} from {@link RendezvousShardPlacement} rather than from allocator
 * output.
 *
 * <p>C4. This is the piece that takes the allocator out of the path. Nothing here consults
 * {@code AllocationService}, no decider runs, and no global pass over other indices happens: the entry
 * for one index is a function of that index's descriptor and the node list.
 *
 * <h2>The recovery source, and why it is set explicitly</h2>
 *
 * The obvious way to write this is {@code RoutingTable.Builder#addAsRecovery}, and it would be wrong in
 * a way that does not fail. That helper picks its recovery source by looking at
 * {@code inSyncAllocationIds}: non-empty means {@code ExistingStoreRecoverySource}, empty means
 * {@code EmptyStoreRecoverySource}. Under computed placement those ids are absent by design, so the
 * helper silently chooses "empty store" and the index comes back <b>blank</b> -- live data replaced by
 * nothing, with no error anywhere.
 *
 * <p>A5 hit exactly this once already, through a different route: {@code CancelAllocationCommand} clears
 * the ids during eviction, so reactivating a scaled-to-zero index resurrected it empty. It was caught by
 * a test rather than by review. This class therefore never infers a recovery source; it states one.
 *
 * <h2>Shards are built already started</h2>
 *
 * A computed shard has nowhere to be allocated <em>to</em>: the answer is already known. So each shard is
 * walked through unassigned, then initializing on its computed node, then started, without an allocator
 * observing any of it. That is deliberate and it is the reason the transition observers had to be
 * audited: everything that watches INITIALIZING becoming STARTED is allocator-internal except
 * {@code SnapshotsService} and {@code IndexService}.
 */
public final class ComputedRoutingTable {

    /**
     * Why the shard is where it is. {@link UnassignedInfo.Reason#INDEX_CREATED} would be a lie for an
     * index that already exists, and the reason is surfaced in the allocation explain API, so it is worth
     * being honest about even though nothing branches on it here.
     */
    private static final String PLACEMENT_MESSAGE = "placement computed by rendezvous hash";

    private ComputedRoutingTable() {}

    /**
     * Eligible nodes for placement, in a fixed order.
     *
     * <p>C2. Two coordinators computing against different node lists produce different placement, so
     * this is a correctness input rather than a convenience. Sorted by node id because the underlying
     * {@code DiscoveryNodes} iteration order is not contractual, and although
     * {@link RendezvousShardPlacement} is order-independent by construction, depending on that here would
     * make this method's contract weaker than it needs to be.
     *
     * <p>Data nodes only. A cluster-manager-only node holds no shards, and including one would place a
     * fraction of every index nowhere.
     */
    public static List<String> eligibleNodes(ClusterState state) {
        // The published membership when there is one, because placement has to be stable in time as well
        // as across coordinators. Reading DiscoveryNodes directly means a node that is merely restarting
        // drops out, its shards move to nodes holding none of their data, and those recover empty while
        // looking healthy. C13 hit exactly that, and it is silent.
        ComputedPlacementMembership membership = ComputedPlacementMembershipService.get(state);
        if (membership.isEmpty() == false) {
            return membership.nodeIds();
        }
        // Nothing published yet, which is the window between a cluster forming and the maintainer's
        // first update. Fall back to the live view so a fresh cluster can place shards at all; it is
        // only unsafe once there is data to lose, and by then the membership exists.
        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (node.isDataNode()) {
                nodeIds.add(node.getId());
            }
        }
        nodeIds.sort(null);
        return nodeIds;
    }

    /**
     * The routing entry for one index, with every shard started on its computed node.
     *
     * <p>Returns an index whose shards are all unassigned when the cluster has no data nodes, rather than
     * failing: that is a transient state during startup, and an exception there would turn a slow start
     * into a broken one.
     */
    public static IndexRoutingTable build(IndexMetadata indexMetadata, List<String> eligibleNodeIds, int candidateCount) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        String indexUuid = indexMetadata.getIndexUUID();

        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            IndexShardRoutingTable.Builder shardBuilder = new IndexShardRoutingTable.Builder(shard);
            List<String> candidates = RendezvousShardPlacement.candidates(eligibleNodeIds, indexUuid, shardId, candidateCount);

            shardBuilder.addShard(primary(shard, candidates.isEmpty() ? null : candidates.get(0)));

            // Search-only replicas take the remaining candidates. They read from object storage, so they
            // recover from the store rather than from a peer, and there is no primary to pull from
            // anyway under computed placement.
            int searchReplicas = Math.min(indexMetadata.getNumberOfSearchOnlyReplicas(), Math.max(candidates.size() - 1, 0));
            for (int replica = 0; replica < searchReplicas; replica++) {
                shardBuilder.addShard(searchReplica(shard, candidates.get(replica + 1)));
            }

            builder.addIndexShard(shardBuilder.build());
        }
        return builder.build();
    }

    public static IndexRoutingTable build(IndexMetadata indexMetadata, ClusterState state) {
        return build(indexMetadata, eligibleNodes(state), RendezvousShardPlacement.DEFAULT_CANDIDATE_COUNT);
    }

    /**
     * The writable copy.
     *
     * <p>{@code ExistingStoreRecoverySource} is stated rather than inferred. See the class javadoc: the
     * inference path picks "empty store" when {@code inSyncAllocationIds} is absent, which under computed
     * placement is always, and the result is a live index recovering as blank.
     */
    private static ShardRouting primary(ShardId shard, String nodeId) {
        ShardRouting routing = ShardRouting.newUnassigned(
            shard,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, PLACEMENT_MESSAGE)
        );
        return nodeId == null ? routing : started(routing, nodeId);
    }

    /** A search-only copy, recovering from the object store like the primary does. */
    private static ShardRouting searchReplica(ShardId shard, String nodeId) {
        ShardRouting routing = ShardRouting.newUnassigned(
            shard,
            false,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, PLACEMENT_MESSAGE)
        );
        return started(routing, nodeId);
    }

    /**
     * Walks a shard to STARTED on its computed node.
     *
     * <p><b>The allocation id is derived, not generated, and that was a defect until T39 drove a write
     * through here.</b> This method used to pass null and let {@link ShardRouting#initialize} mint a fresh
     * id. That is invisible while every node answers its own reads: a coordinator resolves placement, gets
     * an id, and never compares it with anyone.
     *
     * <p>A write compares them. The coordinator puts the id it computed into the request, and
     * {@code TransportReplicationAction.AsyncPrimaryAction} checks it against the id the shard on the data
     * node actually has. Two nodes minting independently random ids for the same computed shard disagree
     * every time, and the write fails with "expected allocation id [x] but found [y]" -- a hard failure
     * whose message points at a stale routing table rather than at a random number.
     *
     * <p>{@link ComputedShardRouting#allocationId} is the derivation, and it already existed for the data
     * node's half of this. Its javadoc gives the second reason: an entry rebuilt on every applied cluster
     * state carrying a new id makes {@code IndexShard.updateShardState} reject the update and
     * {@code removeShards} tear the shard down, so a computed shard would be destroyed and rebuilt
     * continuously. The same reasoning that lets placement be computed rather than agreed applies to
     * identity.
     */
    private static ShardRouting started(ShardRouting routing, String nodeId) {
        String allocationId = ComputedShardRouting.allocationId(routing.shardId().getIndex().getUUID(), routing.id(), nodeId);
        return routing.initialize(nodeId, allocationId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();
    }
}
