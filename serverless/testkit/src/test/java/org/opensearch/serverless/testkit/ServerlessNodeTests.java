/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Phase 1 acceptance for {@code rfc-serverless-shell.md}: the production {@link ServerlessNode} boots,
 * starts, contains no control plane and shuts down cleanly.
 *
 * <p>S0 proved this shape was possible using disposable spike code. These tests assert it of the real
 * class, which is a different claim — the promotion from spike to production already surfaced one
 * difference (the spike leaned on {@code DefaultRecoverySettings}, which lives in {@code test/framework}
 * and production code cannot see).
 */
public class ServerlessNodeTests extends OpenSearchTestCase {

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-test")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    public void testNodeBootsStartsAndStopsCleanly() throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings("phase1-a"));
        try {
            assertFalse("a freshly constructed node must not report started", node.isStarted());
            node.start();
            assertTrue("start() did not take effect", node.isStarted());

            // The data plane is present and usable.
            assertNotNull(node.indicesService());
            assertNotNull(node.searchService());
            assertNotNull(node.clusterService());

            // The node holds a node-local view naming itself, and no elected manager (§10.5).
            assertEquals(node.localNode().getId(), node.clusterService().state().nodes().getLocalNodeId());
            assertNull(
                "a serverless node must never believe a cluster-manager was elected",
                node.clusterService().state().nodes().getClusterManagerNodeId()
            );
        } finally {
            node.close();
        }
        assertFalse("close() did not clear started", node.isStarted());
    }

    public void testProductionNodeContainsNoControlPlane() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("phase1-b"))) {
            node.start();
            // Scanned from the node itself rather than from named services: the transport and HTTP
            // layers added in phase 1 are reachable only through its fields, and a check that covers
            // less than the node does would quietly stop being the assertion it claims to be.
            final ControlPlaneAbsence.Result r = ControlPlaneAbsence.scan(ControlPlaneAbsence.CONTROL_PLANE, node);
            assertEquals("a control plane was constructed: " + r.found, java.util.Set.of(), r.found);
            assertTrue("the object-graph walk was vacuous: only " + r.visited + " objects visited", r.visited > 1000);
            logger.info("phase 1: production ServerlessNode walked {} objects, no control plane", r.visited);
        }
    }

    public void testTwoNodesCoexistWithIndependentViews() throws Exception {
        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("phase1-c1"));
            ServerlessNode b = new ServerlessNode(nodeSettings("phase1-c2"))
        ) {
            a.start();
            b.start();
            assertNotEquals("two nodes must not share an identity", a.localNode().getId(), b.localNode().getId());
            assertEquals(a.localNode().getId(), a.clusterService().state().nodes().getLocalNodeId());
            assertEquals(b.localNode().getId(), b.clusterService().state().nodes().getLocalNodeId());
            // Neither node knows the other exists. Membership is derived, not published (§10.1).
            assertEquals(1, a.clusterService().state().nodes().getSize());
            assertEquals(1, b.clusterService().state().nodes().getSize());
        }
    }
}
