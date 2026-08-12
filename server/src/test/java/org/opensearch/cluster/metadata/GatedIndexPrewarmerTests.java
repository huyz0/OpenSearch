/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.support.ServerlessAffinityRouting;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class GatedIndexPrewarmerTests extends OpenSearchTestCase {

    @After
    public void cleanup() {
        DescriptorPrefetch.register(null);
    }

    private static DiscoveryNode dataNode(String id) {
        return new DiscoveryNode(
            id,
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    /**
     * Picks an index name whose affinity node, under the given node set, is exactly
     * {@code wantNodeId} -- so the test drives real affinity decisions rather than guessing
     * whether a fixed literal name happens to land where the test wants it to.
     *
     * <p>{@code allNodeIds} must be every node id {@link ServerlessAffinityRouting#getAffinityNode}
     * would see in the real {@code DiscoveryNodes} the test builds -- including the cluster-manager,
     * since the production code iterates every node with no role filtering. Passing a narrower set
     * here than the real one would compute a different winner than the code under test actually sees.
     */
    private static String nameWithAffinityFor(String wantNodeId, List<String> allNodeIds) {
        for (int i = 0; i < 10_000; i++) {
            String candidate = "gated-affinity-probe-" + i;
            if (wantNodeId.equals(ServerlessAffinityRouting.getAffinityNodeId(candidate, allNodeIds))) {
                return candidate;
            }
        }
        throw new AssertionError("could not find a probe name with affinity for " + wantNodeId + " in 10,000 tries");
    }

    public void testOnlyIndicesWithAffinityForTheLocalNodeArePrefetched() {
        AtomicReference<Collection<String>> prefetched = new AtomicReference<>();
        DescriptorPrefetch.register((names, listener) -> {
            prefetched.set(names);
            listener.onResponse(null);
        });

        DiscoveryNode master = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode oldDataNode = dataNode("data1");
        DiscoveryNode newDataNode = dataNode("data2");

        // The real candidate set the production code sees: every node in the resulting
        // DiscoveryNodes, master included -- getAffinityNode applies no role filtering.
        List<String> allNodeIds = List.of("master", "data1", "data2");
        // One name this node owns, one name some other node owns -- proves the filter is
        // selective, not "everything happens to hash here" or "nothing was actually filtered."
        String ownedByNewNode = nameWithAffinityFor("data2", allNodeIds);
        String ownedBySomeoneElse = nameWithAffinityFor("data1", allNodeIds);

        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(master).add(oldDataNode).clusterManagerNodeId("master").localNodeId("data2"))
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(master)
                    .add(oldDataNode)
                    .add(newDataNode)
                    .clusterManagerNodeId("master")
                    .localNodeId("data2")
            )
            .build();

        GatedIndexPrewarmer prewarmer = new GatedIndexPrewarmer(() -> List.of(ownedByNewNode, ownedBySomeoneElse));
        ClusterChangedEvent event = new ClusterChangedEvent("test-source", currentState, previousState);

        prewarmer.applyClusterState(event);

        assertNotNull("this node has affinity for one of the two names, so it must prefetch something", prefetched.get());
        assertEquals(List.of(ownedByNewNode), new ArrayList<>(prefetched.get()));
    }

    public void testNodeWithNoAffinityShareDoesNotPrefetch() {
        AtomicReference<Collection<String>> prefetched = new AtomicReference<>();
        DescriptorPrefetch.register((names, listener) -> {
            prefetched.set(names);
            listener.onResponse(null);
        });

        DiscoveryNode master = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode oldDataNode = dataNode("data1");
        DiscoveryNode newDataNode = dataNode("data2");

        List<String> allNodeIds = List.of("master", "data1", "data2");
        String ownedBySomeoneElse = nameWithAffinityFor("data1", allNodeIds);

        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(master).add(oldDataNode).clusterManagerNodeId("master").localNodeId("data2"))
            .build();

        ClusterState currentState = ClusterState.builder(new ClusterName("test"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(master)
                    .add(oldDataNode)
                    .add(newDataNode)
                    .clusterManagerNodeId("master")
                    .localNodeId("data2")
            )
            .build();

        // Only names owned by some other node -- the local node ("data2") must prefetch nothing.
        GatedIndexPrewarmer prewarmer = new GatedIndexPrewarmer(() -> List.of(ownedBySomeoneElse));
        ClusterChangedEvent event = new ClusterChangedEvent("test-source", currentState, previousState);

        prewarmer.applyClusterState(event);

        assertNull("a node with no affinity share of the changed set must not call the prefetcher at all", prefetched.get());
    }

    public void testNoDataNodeAddedDoesNotTriggerPrefetch() {
        AtomicReference<Collection<String>> prefetched = new AtomicReference<>();
        DescriptorPrefetch.register((names, listener) -> {
            prefetched.set(names);
            listener.onResponse(null);
        });

        GatedIndexPrewarmer prewarmer = new GatedIndexPrewarmer(() -> List.of("gated-index-1"));

        DiscoveryNode master = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode onlyDataNode = dataNode("data1");

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(master).add(onlyDataNode).clusterManagerNodeId("master").localNodeId("data1"))
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("test-source", state, state);

        prewarmer.applyClusterState(event);

        assertNull("prewarmer must not trigger DescriptorPrefetch when node topology has not changed", prefetched.get());
    }
}
