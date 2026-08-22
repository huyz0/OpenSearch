/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterStateHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.Map;
import java.util.Set;

/**
 * C17. Whether counting active shards sees a computed entry, the way resolution does.
 *
 * <p>C12 found that skipping routing publication is necessary and not sufficient. An index whose
 * placement is computed has no published routing entry, so anything that counts active shards by reading
 * the routing table counts zero: the create API waits for shards that will never be published and the
 * call never returns. Resolution had been hooked; counting had not.
 *
 * <p>Two counters matter, and the second is the more dangerous of the pair. {@link ActiveShardCount}
 * is what index creation waits on, so getting it wrong hangs. {@link ClusterStateHealth} skipped an
 * index with no routing entry entirely, which does not hang: it removes the index from every counter and
 * reports GREEN. A cluster reporting green for indices with no shards anywhere would have made every
 * later integration test pass without exercising anything, so the false green had to go before C13 could
 * mean anything.
 */
public class ComputedActiveShardCountingTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-idx";

    @After
    public void clearSupplier() {
        AbsentIndexRoutingSuppliers.register(null);
    }

    /**
     * The blocker, directly. Creation waits on this returning true and it can only do so by consulting
     * the supplier, because the routing table has nothing to count.
     */
    public void testCountingSeesSuppliedShards() {
        ClusterState state = stateWithoutRouting(3);
        AbsentIndexRoutingSuppliers.register((s, meta) -> allStartedOn(meta, "node-1"));

        assertTrue("a computed index's shards must count as active", ActiveShardCount.DEFAULT.enoughShardsActive(state, INDEX));
        assertTrue(ActiveShardCount.ALL.enoughShardsActive(state, INDEX));
        assertTrue(ActiveShardCount.ONE.enoughShardsActive(state, INDEX));
    }

    /** A computed entry whose shards are not yet placed must still read as not ready, not as ready. */
    public void testCountingIsNotSatisfiedByUnassignedSuppliedShards() {
        ClusterState state = stateWithoutRouting(3);
        AbsentIndexRoutingSuppliers.register((s, meta) -> allUnassigned(meta));

        assertFalse(ActiveShardCount.ONE.enoughShardsActive(state, INDEX));
    }

    /**
     * An open index with neither a published nor a computed entry is a misconfiguration: the opt-out was
     * registered and the supplier declined. It must read as not-ready rather than throw, because this is
     * evaluated inside a cluster state applier thread, where an exception fails the listener rather than
     * the request that is waiting on it.
     */
    public void testADecliningSupplierReadsAsNotReadyRatherThanThrowing() {
        ClusterState state = stateWithoutRouting(1);
        AbsentIndexRoutingSuppliers.register((s, meta) -> null);

        assertFalse(ActiveShardCount.ONE.enoughShardsActive(state, INDEX));
        assertFalse(ActiveShardCount.ALL.enoughShardsActive(state, INDEX));
    }

    /** A published index is unaffected: the supplier is consulted only where there is nothing published. */
    public void testAPublishedIndexIsCountedFromItsPublishedEntry() {
        ClusterState state = stateWithPublishedRouting(2, "node-1");
        AbsentIndexRoutingSuppliers.register((s, meta) -> allUnassigned(meta));

        assertTrue("the supplier must not override a published entry", ActiveShardCount.ALL.enoughShardsActive(state, INDEX));
    }

    /**
     * Finding 8 of the C17 audit. Before this, an index with no routing entry was skipped by health,
     * so a cluster with nothing placed reported GREEN at 100 percent.
     */
    public void testHealthCountsSuppliedShardsRatherThanSkippingTheIndex() {
        ClusterState state = stateWithoutRouting(4);
        AbsentIndexRoutingSuppliers.register((s, meta) -> allStartedOn(meta, "node-1"));

        ClusterStateHealth health = new ClusterStateHealth(state, new String[] { INDEX });

        assertEquals(4, health.getActiveShards());
        assertEquals(4, health.getActivePrimaryShards());
        assertEquals(ClusterHealthStatus.GREEN, health.getStatus());
        assertEquals(1, health.getIndices().size());
    }

    /** The half that proves the counting is real: unplaced computed shards must read RED, not GREEN. */
    public void testHealthIsRedWhenComputedShardsAreUnassigned() {
        ClusterState state = stateWithoutRouting(4);
        AbsentIndexRoutingSuppliers.register((s, meta) -> allUnassigned(meta));

        ClusterStateHealth health = new ClusterStateHealth(state, new String[] { INDEX });

        assertEquals("an index with nothing placed is not green", ClusterHealthStatus.RED, health.getStatus());
        assertEquals(0, health.getActiveShards());
        assertEquals(4, health.getUnassignedShards());
        assertEquals(0.0d, health.getActiveShardsPercent(), 0.0d);
    }

    /**
     * The percentage is a division over the published routing table, and a cluster whose indices are all
     * computed has an empty one. Zero over zero is NaN, which serializes into the health response.
     */
    public void testActiveShardsPercentIsNotNaNWhenNothingIsPublished() {
        // A SERVICE_UNAVAILABLE global block forces the non-green branch, which is the one that divides.
        ClusterState state = ClusterState.builder(stateWithoutRouting(1))
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_ALL).build())
            .build();

        ClusterStateHealth health = new ClusterStateHealth(state, new String[0]);

        assertFalse("percent must not be NaN", Double.isNaN(health.getActiveShardsPercent()));
    }

    /**
     * Two absences that must stay distinguishable. An index with no routing entry may be computed and
     * resolves to null; an index that has an entry without this shard is a caller asking for a shard
     * that does not exist, and collapsing that into null turns a hard error into a retry until timeout.
     */
    public void testResolveShardKeepsShardNotFoundDistinctFromIndexAbsent() {
        ClusterState computed = stateWithoutRouting(1);
        assertNull(
            "an index with no entry resolves to null so the supplier can answer",
            computed.resolveShard(new ShardId(computed.metadata().index(INDEX).getIndex(), 0))
        );

        ClusterState published = stateWithPublishedRouting(1, "node-1");
        ShardId missing = new ShardId(published.metadata().index(INDEX).getIndex(), 7);
        expectThrows(ShardNotFoundException.class, () -> published.resolveShard(missing));
    }

    // ---------------------------------------------------------------- helpers

    private static ClusterState stateWithoutRouting(int shards) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata(shards), false).build())
            .routingTable(routingTableWithBridge())
            .nodes(DiscoveryNodes.builder().add(node("node-1")).add(node("node-2")).localNodeId("node-1").build())
            .build();
    }

    private static ClusterState stateWithPublishedRouting(int shards, String nodeId) {
        IndexMetadata metadata = indexMetadata(shards);
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .routingTable(RoutingTable.builder(routingTableWithBridge()).add(allStartedOn(metadata, nodeId)).build())
            .nodes(DiscoveryNodes.builder().add(node("node-1")).add(node("node-2")).localNodeId("node-1").build())
            .build();
    }

    /**
     * A real (non-EMPTY_ROUTING_TABLE) instance with the SupplierBackedIndexRoutingResolver bridge attached
     * -- attachIndexRoutingResolver is deliberately a no-op on the shared EMPTY_ROUTING_TABLE singleton (see
     * its own javadoc), so resolving via the SPI ClusterState#getIndexRoutingTable/ActiveShardCount/
     * ClusterStateHealth already consult (Phase C4a of core-pluggability-refactor-plan.md) needs an explicit
     * one here, same as production code gets from a real cluster state. This was a real, pre-existing gap:
     * confirmed via git stash that testCountingSeesSuppliedShards/testHealthCountsSuppliedShardsRatherThanSkippingTheIndex/
     * testHealthIsRedWhenComputedShardsAreUnassigned were already failing before any of this session's C4b
     * work, because AbsentIndexRoutingSuppliers.register(...) alone was never bridged into the resolver
     * ActiveShardCount/ClusterStateHealth have consulted since C4a.
     */
    private static RoutingTable routingTableWithBridge() {
        RoutingTable routingTable = RoutingTable.builder().build();
        routingTable.attachIndexRoutingResolver(new SupplierBackedIndexRoutingResolver());
        return routingTable;
    }

    private static IndexMetadata indexMetadata(int shards) {
        return IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    private static IndexRoutingTable allStartedOn(IndexMetadata indexMetadata, String nodeId) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            ShardRouting routing = unassignedPrimary(shard).initialize(nodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                .moveToStarted();
            builder.addIndexShard(new IndexShardRoutingTable.Builder(shard).addShard(routing).build());
        }
        return builder.build();
    }

    private static IndexRoutingTable allUnassigned(IndexMetadata indexMetadata) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            builder.addIndexShard(new IndexShardRoutingTable.Builder(shard).addShard(unassignedPrimary(shard)).build());
        }
        return builder.build();
    }

    private static ShardRouting unassignedPrimary(ShardId shard) {
        return ShardRouting.newUnassigned(
            shard,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "computed")
        );
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
