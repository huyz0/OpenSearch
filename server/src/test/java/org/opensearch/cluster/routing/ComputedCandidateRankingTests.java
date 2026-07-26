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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * C15. Whether adaptive replica selection ranks the candidates a placement function computed.
 *
 * <p>The area produces K candidates per shard and C7 established that {@code OperationRouting} consumes
 * a computed entry unmodified. What nothing had checked is the next step: that something then *chooses*
 * among the K rather than taking them in whatever order the entry lists them. A hash spreads load evenly
 * across shards and knows nothing about which node is currently slow, so without ranking the K are three
 * equally blind guesses.
 *
 * <p>The answer turns out to need no production change, and the reason is worth recording. ARS is
 * applied by {@code OperationRouting#shardRoutings} to an {@link IndexShardRoutingTable}, and a computed
 * entry is an {@code IndexShardRoutingTable}. The ranking does not know or care where the table came
 * from. That is the same property that made C3 and C5 small: keeping the existing type rather than
 * introducing a parallel one means existing behaviour composes for free.
 *
 * <p>So this suite is a characterisation test rather than a change. It matters because "ARS applies to
 * computed candidates" was an assumption the area rested on, and this is the difference between assuming
 * it and knowing it.
 */
public class ComputedCandidateRankingTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-idx";

    @After
    public void clearSupplier() {
        AbsentIndexRoutingSuppliers.register(null);
    }

    /**
     * The load-bearing assertion. Three computed candidates, and the node with the worst measured
     * response time must not be chosen first.
     */
    public void testAdaptiveSelectionRanksComputedCandidates() {
        ClusterState state = stateWithoutRouting();
        AbsentIndexRoutingSuppliers.register((s, meta) -> threeCandidates(meta));

        ResponseCollectorService collector = new ResponseCollectorService(clusterService(state));
        // node-3 is an order of magnitude slower than the other two on every measure.
        collector.addNodeStatistics("node-1", 1, 100_000_000L, 10_000_000L);
        collector.addNodeStatistics("node-2", 1, 100_000_000L, 10_000_000L);
        collector.addNodeStatistics("node-3", 100, 5_000_000_000L, 500_000_000L);

        GroupShardsIterator<ShardIterator> groups = operationRouting().searchShards(
            state,
            new String[] { INDEX },
            null,
            null,
            collector,
            new HashMap<>(),
            null
        );

        assertEquals(1, groups.size());
        ShardIterator iterator = groups.iterator().next();
        assertEquals("all three computed candidates must be routable", 3, iterator.size());

        List<String> order = new ArrayList<>();
        ShardRouting next;
        while ((next = iterator.nextOrNull()) != null) {
            order.add(next.currentNodeId());
        }
        assertEquals("the slowest node must be ranked last, or nothing is choosing among the candidates", "node-3", order.get(2));
    }

    /**
     * The control. Without statistics there is nothing to rank on, and all three candidates must still
     * be offered: ranking that silently dropped candidates would look like success here.
     */
    public void testAllCandidatesRemainReachableWithoutStatistics() {
        ClusterState state = stateWithoutRouting();
        AbsentIndexRoutingSuppliers.register((s, meta) -> threeCandidates(meta));

        GroupShardsIterator<ShardIterator> groups = operationRouting().searchShards(
            state,
            new String[] { INDEX },
            null,
            null,
            new ResponseCollectorService(clusterService(state)),
            new HashMap<>(),
            null
        );

        assertEquals(3, groups.iterator().next().size());
    }

    // ---------------------------------------------------------------- helpers

    private static org.opensearch.cluster.service.ClusterService clusterService(ClusterState state) {
        org.opensearch.cluster.service.ClusterService clusterService = org.mockito.Mockito.mock(
            org.opensearch.cluster.service.ClusterService.class
        );
        org.mockito.Mockito.when(clusterService.getClusterSettings())
            .thenReturn(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        org.mockito.Mockito.when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static OperationRouting operationRouting() {
        return new OperationRouting(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    /** One shard, three started copies: the K candidates a placement function would produce. */
    private static IndexRoutingTable threeCandidates(IndexMetadata indexMetadata) {
        ShardId shard = new ShardId(indexMetadata.getIndex(), 0);
        IndexShardRoutingTable.Builder shardTable = new IndexShardRoutingTable.Builder(shard);
        shardTable.addShard(ComputedShardRouting.started(shard, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE));
        for (String replica : List.of("node-2", "node-3")) {
            shardTable.addShard(
                ShardRouting.newUnassigned(
                    shard,
                    false,
                    RecoverySource.PeerRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.CLUSTER_RECOVERED, "computed candidate")
                ).initialize(replica, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE).moveToStarted()
            );
        }
        return IndexRoutingTable.builder(indexMetadata.getIndex()).addIndexShard(shardTable.build()).build();
    }

    private static ClusterState stateWithoutRouting() {
        IndexMetadata metadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(2)
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(metadata, false).build())
            .nodes(DiscoveryNodes.builder().add(node("node-1")).add(node("node-2")).add(node("node-3")).localNodeId("node-1").build())
            .build();
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
