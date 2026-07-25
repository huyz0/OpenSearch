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
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;

/**
 * Spike S6, not a correctness test: measures how the cluster-manager's reroute cost scales with
 * the number of indices and shards it has to consider, to answer whether the binding limit on a
 * scalable-metadata design is the allocator's global view rather than metadata residency.
 *
 * <p>Context: a series of spikes established that per-index metadata residency can be reduced
 * from ~3,668 B to ~289 B by holding a compact routing descriptor instead of full
 * {@code IndexMetadata} (see {@code benchmarks/CLUSTERSTATE_METADATA_SPIKE_FINDINGS.md}). That
 * shifts the question to what the cluster-manager itself can carry, since
 * {@code AllocationService.getMutableRoutingNodes()} rebuilds {@code RoutingNodes} from the whole
 * routing table on every reroute and {@code BalancedShardsAllocator} scans every shard and every
 * index. This measures that directly instead of asserting it.
 *
 * <p>This is a measurement harness, not an assertion of behavior. Run explicitly:
 *
 * <pre>{@code
 * ./gradlew :server:test --tests "*AllocationCeilingSpikeTests*" -Dtests.jvm.argline="-da -dsa"
 * }</pre>
 *
 * <p><b>Pass {@code -da -dsa}.</b> Gradle test runs enable assertions ({@code -ea -esa}, set by
 * {@code OpenSearchTestBasePlugin}) but production {@code jvm.options} does not, and the assertion
 * paths here are expensive: {@code RoutingNodes.assertShardStats} makes two full shard passes
 * allocating a {@code HashSet} per {@code ShardId}, and {@code RoutingNode#invariant} makes three
 * stream-and-collect passes per node. Leaving assertions on roughly <em>doubles</em> the measured
 * cost, so figures taken with them enabled do not represent production.
 */
public class AllocationCeilingSpikeTests extends OpenSearchAllocationTestCase {

    /**
     * Index-count tiers to measure. Shard count is indices x shardsPerIndex x (1 + replicas).
     *
     * <p>A 50,000-index tier was measured once and exceeds the 20-minute suite timeout, because
     * driving 100,000 shards to STARTED takes several rounds of a cold reroute that is itself
     * superlinear. Its cold-reroute figure (53.7 s with assertions enabled) is recorded in
     * {@code benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md}; it is excluded here so this harness
     * stays repeatable.
     */
    private static final int[] INDEX_TIERS = { 1_000, 5_000, 20_000 };
    private static final int SHARDS_PER_INDEX = 1;
    private static final int REPLICAS = 1;
    private static final int NODES = 20;

    public void testRerouteCostByIndexCount() {
        logger.info("--- allocation ceiling spike: {} nodes, {} shard(s)/index, {} replica(s) ---", NODES, SHARDS_PER_INDEX, REPLICAS);
        logger.info("indices | totalShards | buildStateMs | firstRerouteMs | steadyStateRerouteMs | routingNodesBuildMs");

        for (int indexCount : INDEX_TIERS) {
            long buildStart = System.nanoTime();
            ClusterState state = buildState(indexCount);
            long buildMs = (System.nanoTime() - buildStart) / 1_000_000;

            AllocationService service = createAllocationService(
                Settings.builder()
                    // Remove the throttle so we measure the allocator's own cost rather than how
                    // many shards it is permitted to start per round.
                    .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE)
                    .put("cluster.routing.allocation.node_initial_primaries_recoveries", Integer.MAX_VALUE)
                    .build()
            );

            // Cold allocation: everything starts UNASSIGNED, so this is the worst case and not
            // what a running cluster pays per change.
            long rerouteStart = System.nanoTime();
            ClusterState rerouted = service.reroute(state, "spike");
            long rerouteMs = (System.nanoTime() - rerouteStart) / 1_000_000;

            // Drive the shards to STARTED so the next reroute is the steady-state case: nothing to
            // allocate or move, only the unconditional rebuild-and-scan work. This is the number
            // that governs an already-running cluster's per-cluster-state-change cost.
            rerouted = startInitializingShardsAndReroute(service, rerouted);
            while (rerouted.getRoutingNodes()
                .shardsWithState(org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING)
                .isEmpty() == false) {
                rerouted = startInitializingShardsAndReroute(service, rerouted);
            }

            long steadyStart = System.nanoTime();
            ClusterState steady = service.reroute(rerouted, "spike-steady");
            long steadyMs = (System.nanoTime() - steadyStart) / 1_000_000;

            // Isolate the unconditional O(total shards) RoutingNodes rebuild that every reroute --
            // and, per ClusterState.getRoutingNodes(), every data node applying a state -- pays.
            long rnStart = System.nanoTime();
            org.opensearch.cluster.routing.RoutingNodes rn = new org.opensearch.cluster.routing.RoutingNodes(steady, false);
            long rnMs = (System.nanoTime() - rnStart) / 1_000_000;

            int totalShards = indexCount * SHARDS_PER_INDEX * (1 + REPLICAS);
            logger.info("{} | {} | {} | {} | {} | {}", indexCount, totalShards, buildMs, rerouteMs, steadyMs, rnMs);

            // Keep results reachable so nothing is collected mid-measurement.
            assertNotNull(rerouted);
            assertNotNull(steady);
            assertNotNull(rn);
        }
    }

    private ClusterState buildState(int indexCount) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        for (int i = 0; i < indexCount; i++) {
            IndexMetadata indexMetadata = IndexMetadata.builder("index-" + i)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(SHARDS_PER_INDEX)
                .numberOfReplicas(REPLICAS)
                .build();
            metadata.put(indexMetadata, false);
            routingTable.addAsNew(indexMetadata);
        }

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        for (int i = 0; i < NODES; i++) {
            nodes.add(newNode("node-" + i));
        }

        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable.build()).nodes(nodes).build();
    }

    @SuppressWarnings("unused")
    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}
