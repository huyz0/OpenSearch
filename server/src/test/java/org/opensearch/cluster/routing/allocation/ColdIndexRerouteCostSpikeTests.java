/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;

/**
 * What a quiescent tenant costs the allocator, in the shape it has today versus the shape Phase A
 * proposes.
 *
 * <p>The A4 spike established that scale-to-zero does not remove an index from the routing table. It
 * evicts the shards and holds them down with a decider that says no, so they stay present and
 * `UNASSIGNED` and are re-evaluated on every reroute. Nobody had measured what that costs, and the
 * whole case for making cold indices routing-absent rests on that number.
 *
 * <p>Three shapes are compared at a fixed active set:
 * <ul>
 *   <li><b>none</b> -- the active indices alone, as a baseline.</li>
 *   <li><b>held down</b> -- cold indices present in the routing table with unassignable shards. This
 *       is today's scale-to-zero. `index.routing.allocation.enable: none` stands in for
 *       {@code SuspendedShardAllocationDecider}: same shape, no plugin needed.</li>
 *   <li><b>absent</b> -- cold indices in metadata with no routing entry at all. This is what Phase A
 *       is for.</li>
 * </ul>
 *
 * <p>A spike, not an assertion of a threshold. It logs a table; the numbers go in
 * {@code benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md}. Run with assertions disabled to get figures
 * that mean anything -- Gradle enables {@code -ea}, production does not, and S6 measured that
 * difference at roughly 2x.
 */
public class ColdIndexRerouteCostSpikeTests extends OpenSearchAllocationTestCase {

    private static final int ACTIVE_INDICES = 500;
    private static final int[] COLD_TIERS = { 0, 2_000, 10_000 };
    private static final int NODES = 20;
    private static final int REPLICAS = 1;

    public void testRerouteCostAgainstColdIndexCount() {
        logger.info("--- cold index reroute cost: {} active indices, {} nodes ---", ACTIVE_INDICES, NODES);
        logger.info("coldShape | coldIndices | steadyRerouteMs | routingNodesBuildMs | totalShardsInRouting");

        for (int cold : COLD_TIERS) {
            measure("held-down", cold, true);
            measure("absent", cold, false);
        }
    }

    private void measure(String shape, int coldIndices, boolean coldInRoutingTable) {
        AllocationService service = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
                .build()
        );

        ClusterState state = buildState(coldIndices, coldInRoutingTable);

        // Drive the active shards to STARTED. The cold ones never start: either they are unassignable
        // or they are not in the routing table to begin with.
        state = service.reroute(state, "spike");
        int guard = 0;
        while (state.getRoutingNodes().shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false) {
            state = startInitializingShardsAndReroute(service, state);
            if (++guard > 200) {
                fail("cluster did not settle");
            }
        }

        long steadyStart = System.nanoTime();
        ClusterState steady = service.reroute(state, "spike-steady");
        long steadyMs = (System.nanoTime() - steadyStart) / 1_000_000;

        long rnStart = System.nanoTime();
        RoutingNodes rn = new RoutingNodes(steady, false);
        long rnMs = (System.nanoTime() - rnStart) / 1_000_000;

        int shardsInRouting = 0;
        for (var indexRouting : steady.routingTable().indicesRouting().values()) {
            shardsInRouting += indexRouting.shardsWithState(ShardRoutingState.UNASSIGNED).size() + indexRouting.shardsWithState(
                ShardRoutingState.STARTED
            ).size();
        }

        logger.info("{} | {} | {} | {} | {}", shape, coldIndices, steadyMs, rnMs, shardsInRouting);

        assertNotNull(steady);
        assertNotNull(rn);
    }

    private ClusterState buildState(int coldIndices, boolean coldInRoutingTable) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();

        for (int i = 0; i < ACTIVE_INDICES; i++) {
            IndexMetadata indexMetadata = IndexMetadata.builder("active-" + i)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(REPLICAS)
                .build();
            metadata.put(indexMetadata, false);
            routingTable.addAsNew(indexMetadata);
        }

        for (int i = 0; i < coldIndices; i++) {
            // allocation.enable=none stands in for the suspended-shard decider: the shard is in the
            // routing table, unassigned, and no node will ever be allowed to take it.
            IndexMetadata indexMetadata = IndexMetadata.builder("cold-" + i)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put("index.routing.allocation.enable", "none")
                )
                .numberOfShards(1)
                .numberOfReplicas(REPLICAS)
                .build();
            metadata.put(indexMetadata, false);
            if (coldInRoutingTable) {
                routingTable.addAsNew(indexMetadata);
            }
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < NODES; i++) {
            nodes.add(newNode("node-" + i));
        }

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable.build()).nodes(nodes).build();
    }
}
