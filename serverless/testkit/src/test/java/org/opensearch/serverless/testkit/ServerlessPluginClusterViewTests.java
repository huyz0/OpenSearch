/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Plugins see a cluster they can work with: a cluster manager named in the node's view, and cluster health and
 * cluster state answered through their client.
 */
public class ServerlessPluginClusterViewTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    /** Any plugin: the view changes only on a node that hosts one. */
    public static final class AnyPlugin extends Plugin {}

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-cluster-view")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(extra)
            .build();
    }

    private MetadataPlane plane() throws java.io.IOException {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), System::currentTimeMillis, 30_000L);
    }

    private static void reapply(ServerlessNode node) throws Exception {
        node.applyLocalView(node.projector().project(List.of(), List.of()), () -> 10_000L);
    }

    /** Every node names the same cluster manager, the live member with the lowest id, and only it leads. */
    public void testNodesAgreeOnOneClusterManager() throws Exception {
        final MetadataPlane plane = plane();
        try (
            ServerlessNode a = new ServerlessNode(nodeSettings("view-a", Settings.EMPTY), List.of(new AnyPlugin()));
            ServerlessNode b = new ServerlessNode(nodeSettings("view-b", Settings.EMPTY), List.of(new AnyPlugin()))
        ) {
            a.startup().start();
            b.startup().start();
            a.setMetadataPlane(plane);
            b.setMetadataPlane(plane);
            plane.membership().refresh();
            reapply(a);
            reapply(b);
            final String expected = a.localNode().getId().compareTo(b.localNode().getId()) < 0
                ? a.localNode().getId()
                : b.localNode().getId();
            assertEquals(expected, a.clusterService().state().nodes().getClusterManagerNodeId());
            assertEquals(expected, b.clusterService().state().nodes().getClusterManagerNodeId());
            assertNotEquals(
                "exactly one node leads",
                a.clusterService().state().nodes().isLocalNodeElectedClusterManager(),
                b.clusterService().state().nodes().isLocalNodeElectedClusterManager()
            );
        }
    }

    /** A node without plugins, or with the switch off, names no cluster manager: its own code never asks. */
    public void testNoClusterManagerWithoutPluginsOrWithTheSwitchOff() throws Exception {
        try (
            ServerlessNode bare = new ServerlessNode(nodeSettings("view-bare", Settings.EMPTY));
            ServerlessNode off = new ServerlessNode(
                nodeSettings("view-off", Settings.builder().put("serverless.plugin_cluster_view.enabled", false).build()),
                List.of(new AnyPlugin())
            )
        ) {
            for (ServerlessNode node : List.of(bare, off)) {
                node.startup().start();
                node.setMetadataPlane(plane());
                reapply(node);
                assertNull(node.clusterService().state().nodes().getClusterManagerNodeId());
            }
        }
    }

    /** Health is green for an index the metadata plane describes, and red after the timeout for one it does not. */
    public void testHealthThroughThePluginClient() throws Exception {
        final MetadataPlane plane = plane();
        plane.createIndex(new IndexDescriptor(".plugin_config", "uuid-view-health-0", 2, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("view-health", Settings.EMPTY), List.of(new AnyPlugin()))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final ClusterHealthResponse health = node.services()
                .client()
                .admin()
                .cluster()
                .health(new ClusterHealthRequest(".plugin_config").waitForYellowStatus())
                .actionGet();
            assertEquals(ClusterHealthStatus.GREEN, health.getStatus());
            assertEquals(2, health.getActiveShards());
            assertFalse(health.isTimedOut());

            final ClusterHealthResponse missing = node.services()
                .client()
                .admin()
                .cluster()
                .health(new ClusterHealthRequest("absent").timeout(TimeValue.timeValueMillis(300)))
                .actionGet();
            assertEquals(ClusterHealthStatus.RED, missing.getStatus());
            assertTrue(missing.isTimedOut());
        }
    }

    /** State names the cluster manager, and carries the metadata of an index this node has not opened. */
    public void testStateThroughThePluginClient() throws Exception {
        final MetadataPlane plane = plane();
        plane.createIndex(new IndexDescriptor("tenant-a", "uuid-view-state-0", 1, MAPPING, null));
        final IndexDescriptor held = new IndexDescriptor("tenant-b", "uuid-view-state-1", 1, MAPPING, null);
        plane.createIndex(held);
        try (ServerlessNode node = new ServerlessNode(nodeSettings("view-state", Settings.EMPTY), List.of(new AnyPlugin()))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            // tenant-b is in this node's view; tenant-a is not.
            node.applyLocalView(node.projector().project(List.of(held), List.of()), () -> 10_000L);
            final var state = node.services()
                .client()
                .admin()
                .cluster()
                .state(new ClusterStateRequest().indices("tenant-a"))
                .actionGet()
                .getState();
            assertEquals(node.localNode().getId(), state.nodes().getClusterManagerNodeId());
            assertNotNull("the named index must be described", state.metadata().index("tenant-a"));
            assertEquals(1, state.metadata().index("tenant-a").getNumberOfShards());
            assertNull("an index the request did not name is left out, as core filters it", state.metadata().index("tenant-b"));
        }
    }
}
