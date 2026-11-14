/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;

/**
 * Node info, node stats, indices stats and refresh through the plugin client: what a metrics exporter reads on
 * every scrape, and what plugins call after writing their own indices.
 */
public class ServerlessPluginNodeAnswersTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    /** Registers the exporter's per-index switch, as the Prometheus exporter does. */
    public static final class ExporterLikePlugin extends Plugin {
        static final Setting<Boolean> INDICES = Setting.boolSetting(
            "prometheus.indices",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(INDICES);
        }
    }

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-answers")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessBootstrap.STORE_PATH, createTempDir().toString())
            .put(extra)
            .build();
    }

    /** This node answers for itself with real numbers, and only the indices it has open. */
    public void testNodeAndIndexStatisticsForThisNode() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("answers", Settings.EMPTY))) {
            final Client client = boot.node().services().client();
            client.admin().indices().create(new CreateIndexRequest("metrics-a").mapping(MAPPING)).actionGet();
            client.index(new IndexRequest("metrics-a").id("1").source("{\"msg\":\"x\"}", XContentType.JSON)).actionGet();

            final NodesInfoResponse info = client.admin().cluster().nodesInfo(new NodesInfoRequest("_local").clear()).actionGet();
            assertEquals(1, info.getNodes().size());
            assertEquals(boot.node().localNode().getId(), info.getNodes().get(0).getNode().getId());

            final NodesStatsResponse stats = client.admin().cluster().nodesStats(new NodesStatsRequest("_local").all()).actionGet();
            assertFalse(stats.hasFailures());
            final NodeStats node = stats.getNodes().get(0);
            assertTrue("the heap is measured", node.getJvm().getMem().getHeapUsed().getBytes() > 0);
            assertNotNull("the OS is measured", node.getOs());
            assertNotNull("the data paths are measured", node.getFs());
            assertNotNull("the thread pools are measured", node.getThreadPool());
            assertEquals("the write is counted", 1, node.getIndices().getIndexing().getTotal().getIndexCount());

            final IndicesStatsResponse indexStats = client.admin()
                .indices()
                .stats(new IndicesStatsRequest().indices("metrics-*"))
                .actionGet();
            assertEquals(1, indexStats.getShards().length);
            assertEquals(1, indexStats.getIndex("metrics-a").getTotal().getIndexing().getTotal().getIndexCount());
            final IndicesStatsResponse none = client.admin().indices().stats(new IndicesStatsRequest().indices("other-*")).actionGet();
            assertEquals("an index this node has not opened reports nothing", 0, none.getShards().length);
        }
    }

    /** A node the request names that is not this one is reported as failed, with where to ask instead. */
    public void testAnotherNodeIsNotAnsweredFor() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("answers-unknown", Settings.EMPTY))) {
            final NodesStatsResponse stats = boot.node()
                .services()
                .client()
                .admin()
                .cluster()
                .nodesStats(new NodesStatsRequest("no-such-node"))
                .actionGet();
            assertTrue("a node that is not in the view is not answered for", stats.getNodes().isEmpty());
        }
    }

    /** A plugin's refresh makes its own write searchable. */
    public void testRefreshMakesAWriteVisible() throws Exception {
        try (ServerlessBootstrap boot = ServerlessBootstrap.start(nodeSettings("answers-refresh", Settings.EMPTY))) {
            final Client client = boot.node().services().client();
            client.admin().indices().create(new CreateIndexRequest(".plugin_jobs").mapping(MAPPING)).actionGet();
            client.index(new IndexRequest(".plugin_jobs").id("1").source("{\"msg\":\"x\"}", XContentType.JSON)).actionGet();
            final RefreshResponse refreshed = client.admin().indices().refresh(new RefreshRequest(".plugin_jobs")).actionGet();
            assertEquals(1, refreshed.getTotalShards());
            assertEquals(1, refreshed.getSuccessfulShards());
            assertEquals(1L, client.search(new SearchRequest(".plugin_jobs")).actionGet().getHits().getTotalHits().value());
        }
    }

    /** With the switch off, the client refuses them by name, as before. */
    public void testTheSwitchRefusesThem() throws Exception {
        try (
            ServerlessBootstrap boot = ServerlessBootstrap.start(
                nodeSettings("answers-off", Settings.builder().put("serverless.plugin_client.node_answers.enabled", false).build())
            )
        ) {
            final Exception refused = expectThrows(
                Exception.class,
                () -> boot.node().services().client().admin().cluster().nodesStats(new NodesStatsRequest("_local")).actionGet()
            );
            assertTrue(refused.toString(), org.opensearch.ExceptionsHelper.stackTrace(refused).contains("does not implement action"));
        }
    }

    /** The exporter's per-index series are off unless an operator turns them on. */
    public void testPerIndexExportIsOffByDefault() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("exporter", Settings.EMPTY), List.of(new ExporterLikePlugin()))) {
            assertFalse(ExporterLikePlugin.INDICES.get(node.settings()));
        }
        try (
            ServerlessNode node = new ServerlessNode(
                nodeSettings("exporter-on", Settings.builder().put("prometheus.indices", true).build()),
                List.of(new ExporterLikePlugin())
            )
        ) {
            assertTrue("an operator's choice stands", ExporterLikePlugin.INDICES.get(node.settings()));
        }
    }
}
