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
import org.opensearch.cluster.metadata.IndexCatalogRegistry;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.SupplierBackedIndexCatalog;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

/**
 * C7 and C8. Whether request routing actually resolves through a computed entry.
 *
 * <p>This is the assumption the whole area rests on and it had not been checked. C4 builds an
 * {@link IndexRoutingTable} and C3 installs a hook that supplies it, but nothing verified that
 * {@link OperationRouting} -- the code every search and write goes through -- consumes an entry that never
 * appeared in the published routing table. If it did not, the design would be sound and useless.
 *
 * <p>Search (C7) and write (C8) are both covered here because both reach the same resolution path.
 * What differs is what they do with the answer: search may use any of the K candidates, while a write
 * uses the primary as a hint that {@code ShardHead} CAS then confirms or rejects. The routing layer does
 * not know the difference, which is the point -- correctness for writes lives in the lease, not here.
 */
public class ComputedRoutingResolutionTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-idx";

    @After
    public void clearSupplier() {
        AbsentIndexRoutingSuppliers.register(null);
        IndexCatalogRegistry.register(null);
    }

    /**
     * The load-bearing test. The index is in metadata, absent from the routing table, and the supplier
     * computes its entry. Resolution must find shards.
     */
    public void testSearchResolvesThroughASuppliedEntry() {
        ClusterState state = stateWithoutRouting(4);
        AbsentIndexRoutingSuppliers.register((clusterState, indexMetadata) -> allStartedOn(indexMetadata, "node-1"));

        GroupShardsIterator<ShardIterator> groups = operationRouting().searchShards(state, new String[] { INDEX }, null, null);

        assertEquals("every shard should resolve", 4, groups.size());
        for (ShardIterator iterator : groups) {
            assertEquals("a supplied shard must be routable", 1, iterator.size());
            assertEquals("node-1", iterator.nextOrNull().currentNodeId());
        }
    }

    /** Without the supplier this is Phase A's pessimistic answer, which is what the hook improves on. */
    public void testWithoutASupplierResolutionFindsNoShards() {
        ClusterState state = stateWithoutRouting(4);

        GroupShardsIterator<ShardIterator> groups = operationRouting().searchShards(state, new String[] { INDEX }, null, null);

        assertEquals(4, groups.size());
        for (ShardIterator iterator : groups) {
            assertEquals("Phase A reports no shard available", 0, iterator.size());
        }
    }

    /**
     * C8. A write resolves to a single primary, which is the hint {@code ShardHead} CAS then confirms.
     * The routing layer offering the wrong node is a retry, not a correctness failure, and that
     * separation is what lets placement be computed rather than agreed.
     */
    public void testWriteResolvesToASinglePrimary() {
        ClusterState state = stateWithoutRouting(3);
        AbsentIndexRoutingSuppliers.register((clusterState, indexMetadata) -> allStartedOn(indexMetadata, "node-2"));

        ShardIterator iterator = operationRouting().indexShards(state, INDEX, "some-doc-id", null);

        assertNotNull(iterator);
        ShardRouting primary = iterator.nextOrNull();
        assertNotNull(primary);
        assertTrue(primary.primary());
        assertEquals("node-2", primary.currentNodeId());
    }

    /** A supplier that declines leaves the index exactly as Phase A left it, index by index. */
    public void testDecliningOneIndexDoesNotAffectResolutionOfIt() {
        ClusterState state = stateWithoutRouting(2);
        AbsentIndexRoutingSuppliers.register((clusterState, indexMetadata) -> null);

        GroupShardsIterator<ShardIterator> groups = operationRouting().searchShards(state, new String[] { INDEX }, null, null);

        assertEquals(2, groups.size());
        for (ShardIterator iterator : groups) {
            assertEquals(0, iterator.size());
        }
    }

    // ---------------------------------------------------------------- helpers

    private static OperationRouting operationRouting() {
        return new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    /** An index present in metadata with no routing entry: the state Phase A made legal. */
    private static ClusterState stateWithoutRouting(int shards) {
        IndexMetadata metadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();

        // The node-scoped catalog a real node gets from ClusterPlugin#getIndexCatalog(), registered rather
        // than attached to this state: resolution goes through IndexCatalogRegistry now, so a hand-built
        // state needs nothing special, only the registration a real node performs at startup. Cleared in
        // this class's @After, since it is node-scoped and would otherwise outlive these tests.
        IndexCatalogRegistry.register(new SupplierBackedIndexCatalog());
        // Harmless for tests that register nothing on AbsentIndexRoutingSuppliers -- the catalog still
        // answers empty with nothing registered underneath it.
        RoutingTable routingTable = RoutingTable.builder().build();
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(node("node-1")).add(node("node-2")).localNodeId("node-1").build())
            .build();
    }

    private static IndexRoutingTable allStartedOn(IndexMetadata indexMetadata, String nodeId) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            ShardRouting routing = ShardRouting.newUnassigned(
                shard,
                true,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "computed")
            ).initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted();
            builder.addIndexShard(new IndexShardRoutingTable.Builder(shard).addShard(routing).build());
        }
        return builder.build();
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
