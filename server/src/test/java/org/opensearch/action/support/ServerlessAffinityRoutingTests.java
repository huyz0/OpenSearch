/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;

public class ServerlessAffinityRoutingTests extends OpenSearchTestCase {

    public void testGetAffinityNodeIdDeterministic() {
        List<String> nodeIds = List.of("node-a", "node-b", "node-c");
        String affinity1 = ServerlessAffinityRouting.getAffinityNodeId("test-index-1", nodeIds);
        String affinity2 = ServerlessAffinityRouting.getAffinityNodeId("test-index-1", nodeIds);
        assertNotNull(affinity1);
        assertEquals(affinity1, affinity2);
    }

    public void testGetAffinityNodeIdOrderIndependentInput() {
        List<String> ordered = List.of("node-a", "node-b", "node-c");
        List<String> reverse = List.of("node-c", "node-b", "node-a");
        String res1 = ServerlessAffinityRouting.getAffinityNodeId("my-index", ordered);
        String res2 = ServerlessAffinityRouting.getAffinityNodeId("my-index", reverse);
        assertEquals(res1, res2);
    }

    public void testGetAffinityNodeIdNullAndEmptyHandling() {
        assertNull(ServerlessAffinityRouting.getAffinityNodeId(null, List.of("node-1")));
        assertNull(ServerlessAffinityRouting.getAffinityNodeId("idx", null));
        assertNull(ServerlessAffinityRouting.getAffinityNodeId("idx", Collections.emptyList()));
        assertNull(ServerlessAffinityRouting.getAffinityNode(null, (DiscoveryNodes) null));
    }

    public void testGetAffinityNodeFromDiscoveryNodes() {
        DiscoveryNode nodeA = new DiscoveryNode(
            "node-a",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            org.opensearch.Version.CURRENT
        );
        DiscoveryNode nodeB = new DiscoveryNode(
            "node-b",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            org.opensearch.Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(nodeA).add(nodeB).build();

        DiscoveryNode affinityNode = ServerlessAffinityRouting.getAffinityNode("logs-2026-08", nodes);
        assertNotNull(affinityNode);
        assertTrue(affinityNode.equals(nodeA) || affinityNode.equals(nodeB));
    }
}
