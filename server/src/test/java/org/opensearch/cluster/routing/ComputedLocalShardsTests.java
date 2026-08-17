/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * C18. Whether the data node's shard list includes computed shards.
 *
 * <p>{@code RoutingNodes.localRoutingNode} is what every phase of {@code IndicesClusterStateService}
 * uses to decide which shards this node should open. It loops the published routing table, so an index
 * that publishes no entry is invisible to shard creation on every node in the cluster: the index is
 * routable and has nothing to route to.
 *
 * <p>The hook is on this helper and not on the {@link RoutingNodes} constructor. The constructor is what
 * {@code AllocationService} and {@code ClusterState#getRoutingNodes} build, and putting computed shards
 * there would hand these indices back to the allocator. The test below asserts that separation directly,
 * because it is the property the rest of the area depends on and it is easy to lose by moving one line.
 */
public class ComputedLocalShardsTests extends OpenSearchTestCase {

    private static final String COMPUTED = "computed-idx";
    private static final String PUBLISHED = "classic-idx";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    public void testLocalRoutingNodeIncludesComputedShards() {
        ClusterState state = state(2);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedLocalShardsTests::roundRobin);

        RoutingNode node = RoutingNodes.localRoutingNode(state, "node-1");

        assertNotNull(node);
        assertEquals("node-1 owns the even shard", 1, node.size());
        assertEquals(COMPUTED, node.iterator().next().index().getName());
    }

    /** Every shard must land on exactly one node, or a shard is either orphaned or opened twice. */
    public void testEveryComputedShardIsOwnedByExactlyOneNode() {
        ClusterState state = state(4);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedLocalShardsTests::roundRobin);

        int total = 0;
        for (String nodeId : List.of("node-1", "node-2")) {
            total += RoutingNodes.localRoutingNode(state, nodeId).size();
        }

        assertEquals(4, total);
    }

    /** Without the registration this is exactly what it always was. */
    public void testWithoutTheRegistrationNothingIsAdded() {
        ClusterState state = state(2);

        assertEquals(0, RoutingNodes.localRoutingNode(state, "node-1").size());
    }

    /**
     * The load-bearing separation. The allocator builds RoutingNodes through the constructor, so computed
     * shards must not appear there: an index the allocator can see is an index the allocator will try to
     * place, which is the whole thing this area exists to avoid.
     */
    public void testTheAllocatorsViewDoesNotSeeComputedShards() {
        ClusterState state = state(2);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedLocalShardsTests::roundRobin);

        RoutingNodes allocatorView = new RoutingNodes(state, false);

        assertEquals("the allocator must be handed nothing to allocate", 0, allocatorView.node("node-1").size());
        assertEquals(0, allocatorView.node("node-2").size());
    }

    /** A published index is unaffected, and the two sources add rather than replace. */
    public void testPublishedShardsAreStillThere() {
        ClusterState state = stateWithPublished();
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedLocalShardsTests::roundRobin);

        RoutingNode node = RoutingNodes.localRoutingNode(state, "node-1");

        Set<String> indices = new java.util.HashSet<>();
        for (ShardRouting shard : node) {
            indices.add(shard.index().getName());
        }
        assertEquals(Set.of(COMPUTED, PUBLISHED), indices);
    }

    /** A throwing implementation must not stop a node from applying cluster state. */
    public void testAThrowingImplementationReadsAsEmpty() {
        ClusterState state = state(2);
        AbsentIndexRoutingSuppliers.registerLocalShards((s, nodeId) -> { throw new IllegalStateException("plugin bug"); });

        assertEquals(0, RoutingNodes.localRoutingNode(state, "node-1").size());
    }

    // ---------------------------------------------------------------- helpers

    /** Shard n to node (n % 2) + 1, computed indices only. */
    private static List<ShardRouting> roundRobin(ClusterState state, String nodeId) {
        List<ShardRouting> mine = new ArrayList<>();
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (indexMetadata.getIndex().getName().startsWith("computed-") == false) {
                continue;
            }
            for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                String owner = "node-" + (shardId % 2 + 1);
                if (owner.equals(nodeId)) {
                    mine.add(started(new ShardId(indexMetadata.getIndex(), shardId), nodeId));
                }
            }
        }
        return mine;
    }

    private static ClusterState state(int shards) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index(COMPUTED, shards), false).build())
            .routingTable(routingTableWithBridge())
            .nodes(DiscoveryNodes.builder().add(node("node-1")).add(node("node-2")).localNodeId("node-1").build())
            .build();
    }

    private static ClusterState stateWithPublished() {
        IndexMetadata published = index(PUBLISHED, 1);
        IndexRoutingTable.Builder routing = IndexRoutingTable.builder(published.getIndex());
        ShardId shard = new ShardId(published.getIndex(), 0);
        routing.addIndexShard(new IndexShardRoutingTable.Builder(shard).addShard(started(shard, "node-1")).build());

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(index(COMPUTED, 2), false).put(published, false).build())
            .routingTable(RoutingTable.builder(routingTableWithBridge()).add(routing.build()).build())
            .nodes(DiscoveryNodes.builder().add(node("node-1")).add(node("node-2")).localNodeId("node-1").build())
            .build();
    }

    /**
     * A real (non-EMPTY_ROUTING_TABLE) instance with the SupplierBackedIndexRoutingResolver bridge attached
     * -- attachIndexRoutingResolver is deliberately a no-op on the shared EMPTY_ROUTING_TABLE singleton (see
     * its own javadoc), so resolving via the new SPI (Phase C4b of core-pluggability-refactor-plan.md, which
     * RoutingNodes#localRoutingNode now goes through for localShardsFor) needs an explicit one here, same as
     * production code gets from a real cluster state. Harmless for tests that register nothing on
     * AbsentIndexRoutingSuppliers -- the bridge still answers empty with nothing registered underneath it.
     */
    private static RoutingTable routingTableWithBridge() {
        RoutingTable routingTable = RoutingTable.builder().build();
        routingTable.attachIndexRoutingResolver(new SupplierBackedIndexRoutingResolver());
        return routingTable;
    }

    private static IndexMetadata index(String name, int shards) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-000000000")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    private static ShardRouting started(ShardId shard, String nodeId) {
        return ShardRouting.newUnassigned(
            shard,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "computed")
        ).initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();
    }

    private static DiscoveryNode node(String id) {
        return new DiscoveryNode(
            id,
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300 + Math.abs(id.hashCode() % 1000)),
            Map.of(),
            Set.of(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }
}
