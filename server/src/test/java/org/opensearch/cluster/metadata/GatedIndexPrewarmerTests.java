/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class GatedIndexPrewarmerTests extends OpenSearchTestCase {

    @After
    public void cleanup() {
        DescriptorPrefetch.register(null);
    }

    public void testDataNodeJoinTriggersSpeculativePrefetch() {
        AtomicBoolean prefetched = new AtomicBoolean(false);
        DescriptorPrefetch.register((names, listener) -> {
            prefetched.set(true);
            listener.onResponse(null);
        });

        GatedIndexPrewarmer prewarmer = new GatedIndexPrewarmer(() -> List.of("gated-index-1", "gated-index-2"));

        DiscoveryNode master = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode oldDataNode = new DiscoveryNode(
            "data1",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        ClusterState previousState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(master).add(oldDataNode).clusterManagerNodeId("master"))
            .build();

        DiscoveryNode newDataNode = new DiscoveryNode(
            "data2",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        ClusterState currentState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(master).add(oldDataNode).add(newDataNode).clusterManagerNodeId("master"))
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("test-source", currentState, previousState);

        prewarmer.applyClusterState(event);

        assertTrue("prewarmer must trigger DescriptorPrefetch when a new data node joins", prefetched.get());
    }

    public void testNoDataNodeAddedDoesNotTriggerPrefetch() {
        AtomicBoolean prefetched = new AtomicBoolean(false);
        DescriptorPrefetch.register((names, listener) -> {
            prefetched.set(true);
            listener.onResponse(null);
        });

        GatedIndexPrewarmer prewarmer = new GatedIndexPrewarmer(() -> List.of("gated-index-1"));

        DiscoveryNode master = new DiscoveryNode("master", buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode dataNode = new DiscoveryNode(
            "data1",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );

        ClusterState state = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(master).add(dataNode).clusterManagerNodeId("master"))
            .build();

        ClusterChangedEvent event = new ClusterChangedEvent("test-source", state, state);

        prewarmer.applyClusterState(event);

        assertFalse("prewarmer must not trigger DescriptorPrefetch when node topology has not changed", prefetched.get());
    }
}
