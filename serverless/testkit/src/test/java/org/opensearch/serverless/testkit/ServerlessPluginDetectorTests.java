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
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a detector plugin asks of the client on its own indices: field mappings, update and delete by query, writes through
 * its result alias, the plugins this node runs, and its own names resolved.
 */
public class ServerlessPluginDetectorTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"ts\":{\"type\":\"date\"},"
        + "\"msg\":{\"type\":\"keyword\",\"fields\":{\"words\":{\"type\":\"text\"}}},\"n\":{\"type\":\"long\"}}}";

    private MetadataPlane plane;
    private final AtomicLong clock = new AtomicLong(1_000L);

    /** Takes shard 0 of the index here, as the reconcile loop would once a write asked for it. */
    private void take(ServerlessNode node, String index) throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want(index, 0);
        loop.tick(clock.get());
    }

    private ServerlessNode started(String name) throws Exception {
        return started(name, Settings.EMPTY);
    }

    private ServerlessNode started(String name, Settings extra) throws Exception {
        final Settings settings = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-detector")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(extra)
            .build();
        final ServerlessNode node = new ServerlessNode(settings, List.of(new ServerlessPluginIndicesViewTests.ProbePlugin()));
        node.startup().start();
        plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, 30_000L);
        node.setMetadataPlane(plane);
        return node;
    }

    /** Leaf fields and multi-fields, each answered with its full name. */
    public void testFieldMappings() throws Exception {
        try (ServerlessNode node = started("fields")) {
            final Client client = node.services().client();
            client.admin().indices().create(new CreateIndexRequest(".probe-plugin-config").mapping(MAPPING)).actionGet();
            final GetFieldMappingsResponse response = client.admin()
                .indices()
                .getFieldMappings(new GetFieldMappingsRequest().indices(".probe-plugin-config").fields("ts", "msg.words", "absent"))
                .actionGet();
            final var fields = response.mappings().get(".probe-plugin-config");
            assertNotNull("the index is answered: " + response.mappings(), fields);
            assertEquals("ts", fields.get("ts").fullName());
            assertEquals("date", ((java.util.Map<?, ?>) fields.get("ts").sourceAsMap().get("ts")).get("type"));
            assertEquals("msg.words", fields.get("msg.words").fullName());
            assertNull("a field the mapping lacks is left out", fields.get("absent"));
            assertTrue(
                "the index carries its creation date, which index management ages it by",
                node.clusterService().state().metadata().index(".probe-plugin-config").getCreationDate() > 0
            );
        }
    }

    /** Update by query changes every matched document; delete by query removes only the matches. Refreshed is enough. */
    public void testUpdateAndDeleteByQuery() throws Exception {
        try (ServerlessNode node = started("by-query")) {
            final Client client = node.services().client();
            client.admin().indices().create(new CreateIndexRequest(".probe-plugin-tasks").mapping(MAPPING)).actionGet();
            take(node, ".probe-plugin-tasks");
            for (int i = 0; i < 3; i++) {
                client.index(
                    new IndexRequest(".probe-plugin-tasks").id("t" + i)
                        .source("{\"msg\":\"" + (i == 0 ? "old" : "new") + "\",\"n\":" + i + "}", XContentType.JSON)
                        .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                ).actionGet();
            }
            final BulkByScrollResponse updated = client.execute(
                UpdateByQueryAction.INSTANCE,
                new UpdateByQueryRequest(".probe-plugin-tasks").setQuery(QueryBuilders.termQuery("msg", "new")).setRefresh(true)
            ).actionGet();
            assertEquals(2, updated.getStatus().getTotal());
            assertEquals(2, updated.getStatus().getUpdated() + updated.getStatus().getNoops());
            assertTrue(updated.getBulkFailures().isEmpty());

            final BulkByScrollResponse deleted = client.execute(
                DeleteByQueryAction.INSTANCE,
                new DeleteByQueryRequest(".probe-plugin-tasks").setQuery(QueryBuilders.termQuery("msg", "old")).setRefresh(true)
            ).actionGet();
            assertEquals(1, deleted.getDeleted());
            assertFalse(client.get(new GetRequest(".probe-plugin-tasks", "t0")).actionGet().isExists());
            assertTrue(client.get(new GetRequest(".probe-plugin-tasks", "t1")).actionGet().isExists());
        }
    }

    /** A write to the plugin's result alias lands in the one index the alias names. */
    public void testWriteThroughPluginAliasLandsInItsIndex() throws Exception {
        try (ServerlessNode node = started("alias-write")) {
            final Client client = node.services().client();
            client.admin()
                .indices()
                .create(new CreateIndexRequest(".probe-plugin-results-1").mapping(MAPPING).alias(new Alias(".probe-plugin-results")))
                .actionGet();
            take(node, ".probe-plugin-results-1");
            final IndexResponse written = client.index(
                new IndexRequest(".probe-plugin-results").id("r1").source("{\"msg\":\"r\"}", XContentType.JSON)
            ).actionGet();
            assertEquals(".probe-plugin-results-1", written.getIndex());
            assertTrue(client.get(new GetRequest(".probe-plugin-results-1", "r1")).actionGet().isExists());
        }
    }

    /** Nodes info answers the plugins section when asked for it; plugins given on the classpath are not listed there. */
    public void testNodesInfoListsPlugins() throws Exception {
        try (ServerlessNode node = started("plugins-info")) {
            final NodesInfoResponse info = node.services()
                .client()
                .admin()
                .cluster()
                .nodesInfo(new NodesInfoRequest("_local").clear().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName()))
                .actionGet();
            final PluginsAndModules plugins = info.getNodes().get(0).getInfo(PluginsAndModules.class);
            assertNotNull("plugins are reported", plugins);
            assertNotNull(plugins.getPluginInfos());
        }
    }

    /** Plugin names resolve against the view; tenant names, aliases and trailing wildcards against the metadata plane. */
    public void testResolverAnswersPluginAndTenantNames() throws Exception {
        try (ServerlessNode node = started("resolver", Settings.EMPTY)) {
            final Client client = node.services().client();
            client.admin()
                .indices()
                .create(new CreateIndexRequest(".probe-plugin-results-1").mapping(MAPPING).alias(new Alias(".probe-plugin-results")))
                .actionGet();
            client.admin().indices().create(new CreateIndexRequest("tenant-logs").mapping(MAPPING)).actionGet();
            client.admin().indices().create(new CreateIndexRequest("tenant-metrics").mapping(MAPPING)).actionGet();
            client.admin()
                .indices()
                .aliases(
                    new org.opensearch.action.admin.indices.alias.IndicesAliasesRequest().addAliasAction(
                        org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions.add()
                            .index("tenant-metrics")
                            .alias("tenant-current")
                    )
                )
                .actionGet();
            final IndexNameExpressionResolver resolver = node.services().indexNameExpressionResolver();
            final var state = node.clusterService().state();
            final IndicesOptions strict = IndicesOptions.strictExpandOpen();
            assertArrayEquals(
                new String[] { ".probe-plugin-results-1" },
                resolver.concreteIndexNames(state, strict, ".probe-plugin-results")
            );
            assertArrayEquals(new String[] { "tenant-logs" }, resolver.concreteIndexNames(state, strict, "tenant-logs"));
            assertArrayEquals(new String[] { "tenant-metrics" }, resolver.concreteIndexNames(state, strict, "tenant-current"));
            assertEquals(
                java.util.Set.of("tenant-logs", "tenant-metrics"),
                java.util.Set.of(resolver.concreteIndexNames(state, strict, "tenant-*"))
            );
            assertEquals("the uuid is the plane's", 1, resolver.concreteIndices(state, strict, "tenant-logs").length);
            assertTrue(resolver.hasIndexAbstraction("tenant-logs", state));
            assertFalse(resolver.hasIndexAbstraction("tenant-absent", state));
            expectThrows(
                org.opensearch.index.IndexNotFoundException.class,
                () -> resolver.concreteIndexNames(state, strict, "tenant-absent")
            );
            assertEquals(0, resolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), "tenant-absent").length);
            expectThrows(UnsupportedOperationException.class, () -> resolver.concreteIndexNames(state, strict, "_all"));
            expectThrows(UnsupportedOperationException.class, () -> resolver.concreteIndexNames(state, strict, "t*-logs"));
        }
    }

    /** With the switch off, a tenant name is refused as before. */
    public void testNameResolutionSwitchRefusesTenantNames() throws Exception {
        try (
            ServerlessNode node = started(
                "resolver-off",
                Settings.builder().put("serverless.plugin_name_resolution.enabled", false).build()
            )
        ) {
            node.services().client().admin().indices().create(new CreateIndexRequest("tenant-logs").mapping(MAPPING)).actionGet();
            final IndexNameExpressionResolver resolver = node.services().indexNameExpressionResolver();
            expectThrows(
                UnsupportedOperationException.class,
                () -> resolver.concreteIndexNames(node.clusterService().state(), IndicesOptions.strictExpandOpen(), "tenant-logs")
            );
        }
    }
}
