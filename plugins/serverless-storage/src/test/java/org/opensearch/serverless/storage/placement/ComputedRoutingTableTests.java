/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ComputedPlacementMembership;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * C4. The routing entry built from the placement function rather than from the allocator.
 *
 * <p>The centrepiece is {@link #testPrimaryNeverRecoversFromAnEmptyStore}. The natural way to write this
 * production code is {@code RoutingTable.Builder#addAsRecovery}, which infers its recovery source from
 * {@code inSyncAllocationIds} and therefore picks "empty store" whenever they are absent -- which under
 * computed placement is always. The index then comes back blank, with no error anywhere. A5 hit exactly
 * that once, through a different route, and it was a test that caught it rather than review.
 */
public class ComputedRoutingTableTests extends OpenSearchTestCase {

    /**
     * The trap. Live data replaced by nothing is the worst failure this area can produce, and it is
     * silent, so it gets an assertion of its own rather than being implied by a round-trip test.
     */
    public void testPrimaryNeverRecoversFromAnEmptyStore() {
        // Asserted on unassigned shards, because core clears recoverySource once a shard is STARTED --
        // a started shard has already recovered and has nothing left to recover from. Unassigned is
        // therefore the only state where the field is live, and it is exactly the state a shard is in
        // when a node restart makes it recover for real. Asserting on a started shard would have read
        // null and passed for the wrong reason.
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 5, 0), List.of(), 3);

        for (IndexShardRoutingTable shard : routing) {
            ShardRouting primary = shard.primaryShard();
            assertNotNull(primary);
            assertEquals(ShardRoutingState.UNASSIGNED, primary.state());
            assertEquals(
                "a computed primary must recover from its existing store, never from an empty one",
                RecoverySource.Type.EXISTING_STORE,
                primary.recoverySource().getType()
            );
            assertNotSame(RecoverySource.EmptyStoreRecoverySource.INSTANCE, primary.recoverySource());
        }
    }

    /** Started shards have already recovered, so core drops the field. Pinned so the above reads clearly. */
    public void testStartedShardsCarryNoRecoverySource() {
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 4, 0), nodes(4), 3);

        for (IndexShardRoutingTable shard : routing) {
            assertEquals(ShardRoutingState.STARTED, shard.primaryShard().state());
            assertNull(shard.primaryShard().recoverySource());
        }
    }

    public void testSearchReplicasAreSearchOnlyAndStarted() {
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 3, 2), nodes(6), 3);

        int replicas = 0;
        for (IndexShardRoutingTable shard : routing) {
            for (ShardRouting replica : shard.replicaShards()) {
                assertTrue("replicas here are search-only", replica.isSearchOnly());
                assertEquals(ShardRoutingState.STARTED, replica.state());
                replicas++;
            }
        }
        assertEquals("3 shards x 2 search replicas", 6, replicas);
    }

    /** Every shard is placed and started without the allocator ever running. */
    public void testEveryShardIsStartedOnAComputedNode() {
        List<String> nodeIds = nodes(5);
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 8, 0), nodeIds, 3);

        assertEquals(8, routing.shards().size());
        for (IndexShardRoutingTable shard : routing) {
            ShardRouting primary = shard.primaryShard();
            assertEquals(ShardRoutingState.STARTED, primary.state());
            assertTrue("assigned to a node outside the cluster", nodeIds.contains(primary.currentNodeId()));
            assertNotNull("a started shard needs an allocation id", primary.allocationId());
        }
    }

    /** And the placement it lands on is the placement the function says, not something adjacent to it. */
    public void testPlacementMatchesTheFunction() {
        List<String> nodeIds = nodes(6);
        IndexMetadata metadata = index("idx", 10, 0);
        IndexRoutingTable routing = ComputedRoutingTable.build(metadata, nodeIds, 3);

        for (IndexShardRoutingTable shard : routing) {
            String expected = RendezvousShardPlacement.primaryCandidate(nodeIds, metadata.getIndexUUID(), shard.shardId().id());
            assertEquals("shard " + shard.shardId().id(), expected, shard.primaryShard().currentNodeId());
        }
    }

    public void testPrimaryAndSearchReplicasLandOnDistinctNodes() {
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 6, 2), nodes(8), 3);

        for (IndexShardRoutingTable shard : routing) {
            Set<String> holders = new HashSet<>();
            for (ShardRouting routingEntry : shard) {
                assertTrue("a shard must not be placed twice on one node", holders.add(routingEntry.currentNodeId()));
            }
        }
    }

    /**
     * Startup has a window with no data nodes yet. Failing there would turn a slow start into a broken
     * one, so the entry is produced with unassigned shards instead.
     */
    public void testNoDataNodesYieldsUnassignedRatherThanFailing() {
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 3, 0), List.of(), 3);

        assertEquals(3, routing.shards().size());
        for (IndexShardRoutingTable shard : routing) {
            assertEquals(ShardRoutingState.UNASSIGNED, shard.primaryShard().state());
            assertNull(shard.primaryShard().currentNodeId());
        }
    }

    /** More requested search replicas than candidates must not invent placements. */
    public void testSearchReplicasAreCappedByAvailableCandidates() {
        IndexRoutingTable routing = ComputedRoutingTable.build(index("idx", 2, 5), nodes(3), 3);

        for (IndexShardRoutingTable shard : routing) {
            // 3 candidates, one taken by the primary, so at most two search replicas regardless of the
            // five the index asks for.
            assertTrue("got " + shard.replicaShards().size() + " replicas", shard.replicaShards().size() <= 2);
        }
    }

    public void testBuildIsDeterministic() {
        IndexMetadata metadata = index("idx", 12, 1);
        List<String> nodeIds = nodes(7);

        IndexRoutingTable first = ComputedRoutingTable.build(metadata, nodeIds, 3);
        IndexRoutingTable second = ComputedRoutingTable.build(metadata, nodeIds, 3);

        for (int shardId = 0; shardId < metadata.getNumberOfShards(); shardId++) {
            assertEquals(first.shard(shardId).primaryShard().currentNodeId(), second.shard(shardId).primaryShard().currentNodeId());
        }
    }

    /**
     * C2. A cluster-manager-only node holds no shards, so including it would place a fraction of every
     * index nowhere.
     */
    public void testOnlyDataNodesAreEligible() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(node("data-1", DiscoveryNodeRole.DATA_ROLE))
                    .add(node("data-2", DiscoveryNodeRole.DATA_ROLE))
                    .add(node("cm-1", DiscoveryNodeRole.CLUSTER_MANAGER_ROLE))
                    .build()
            )
            .build();

        assertEquals(List.of("data-1", "data-2"), ComputedRoutingTable.eligibleNodes(state));
    }

    /** Sorted, so two coordinators cannot compute against differently-ordered lists. */
    public void testEligibleNodesAreSorted() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(node("zulu", DiscoveryNodeRole.DATA_ROLE))
                    .add(node("alpha", DiscoveryNodeRole.DATA_ROLE))
                    .add(node("mike", DiscoveryNodeRole.DATA_ROLE))
                    .build()
            )
            .build();

        assertEquals(List.of("alpha", "mike", "zulu"), ComputedRoutingTable.eligibleNodes(state));
    }

    /**
     * C2, the property C13 needed and nothing asserted. A node leaving must not change placement.
     *
     * <p>Without this, a restarting node drops out of the live view, its shards move to nodes holding
     * none of their data, and those recover empty while looking healthy. The membership is published and
     * never shrinks precisely so that a node being briefly away is not a placement event.
     */
    public void testPlacementDoesNotChangeWhenANodeLeaves() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("data-1", "data-2", "data-3"), 1L);

        ClusterState whole = withMembership(membership, "data-1", "data-2", "data-3");
        ClusterState missingOne = withMembership(membership, "data-1", "data-3");

        assertEquals(
            "a node being away must not change the eligible set, or its shards move off their data",
            ComputedRoutingTable.eligibleNodes(whole),
            ComputedRoutingTable.eligibleNodes(missingOne)
        );
        assertEquals(List.of("data-1", "data-2", "data-3"), ComputedRoutingTable.eligibleNodes(missingOne));
    }

    /** And the membership wins over the live view even when the live view has more nodes. */
    public void testPublishedMembershipWinsOverTheLiveView() {
        ComputedPlacementMembership membership = ComputedPlacementMembership.of(List.of("data-1"), 3L);

        ClusterState state = withMembership(membership, "data-1", "data-2");

        assertEquals(
            "an unpublished newcomer must not shift placement until the membership says so",
            List.of("data-1"),
            ComputedRoutingTable.eligibleNodes(state)
        );
    }

    /**
     * Before anything is published there is nothing to be stable about, so the live view is used. That
     * window is only unsafe once there is data to lose, and by then the maintainer has published.
     */
    public void testFallsBackToTheLiveViewWhenNothingIsPublished() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(node("data-2", DiscoveryNodeRole.DATA_ROLE))
                    .add(node("data-1", DiscoveryNodeRole.DATA_ROLE))
                    .build()
            )
            .build();

        assertEquals(List.of("data-1", "data-2"), ComputedRoutingTable.eligibleNodes(state));
    }

    private static ClusterState withMembership(ComputedPlacementMembership membership, String... liveNodeIds) {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (String nodeId : liveNodeIds) {
            nodes.add(node(nodeId, DiscoveryNodeRole.DATA_ROLE));
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes.build())
            .metadata(Metadata.builder().putCustom(ComputedPlacementMembership.TYPE, membership).build())
            .build();
    }

    // ---------------------------------------------------------------- helpers

    private static IndexMetadata index(String name, int shards, int searchReplicas) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .numberOfSearchReplicas(searchReplicas)
            .build();
    }

    private static List<String> nodes(int count) {
        List<String> nodeIds = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            nodeIds.add("node-" + i);
        }
        return nodeIds;
    }

    private static DiscoveryNode node(String id, DiscoveryNodeRole role) {
        return new DiscoveryNode(
            id,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + Math.abs(id.hashCode() % 1000)),
            java.util.Map.of(),
            Set.of(role),
            Version.CURRENT
        );
    }
}
