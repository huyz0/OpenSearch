/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The plugins' own indices and aliases in every node's view: loaded at start, updated at once by a plugin's own changes,
 * followed on other nodes, each shard with one owner every node agrees on.
 */
public class ServerlessPluginIndicesViewTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}";

    /** A plugin whose indices all start with {@code .probe-plugin}. */
    public static final class ProbePlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new SystemIndexDescriptor(".probe-plugin*", "a plugin's own records"));
        }
    }

    private Settings nodeSettings(String name, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-indices")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.plugin_indices_view.refresh_interval", "100ms")
            .put(extra)
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, 30_000L);
    }

    private ServerlessNode started(String name, MetadataPlane plane, Settings extra) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name, extra), List.of(new ProbePlugin()));
        node.startup().start();
        node.setMetadataPlane(plane);
        return node;
    }

    private static String owner(ClusterState view, String index) {
        final ShardRouting primary = view.routingTable().index(index).shard(0).primaryShard();
        assertTrue(index + " has an owner: " + primary, primary.started());
        return primary.currentNodeId();
    }

    /** Loaded at start on every node, ordinary indices left out, and one owner both nodes agree on. */
    public void testLoadedAtStartWithOneOwner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor(".probe-plugin-config", "uuid-config-0000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("tenant-logs", "uuid-tenant-0000000", 1, MAPPING, null));
        try (ServerlessNode a = started("view-a", plane, Settings.EMPTY); ServerlessNode b = started("view-b", plane, Settings.EMPTY)) {
            for (ServerlessNode node : List.of(a, b)) {
                final ClusterState view = node.clusterService().state();
                assertNotNull(node + " holds the plugin index", view.metadata().index(".probe-plugin-config"));
                assertNull("and not the tenants' indices", view.metadata().index("tenant-logs"));
            }
            // The two nodes share one membership here; a refresh makes it name both, as each node's own would.
            plane.membership().refresh();
            // b started second: each node chooses again once it sees the other among the members.
            assertBusy(
                () -> assertEquals(
                    "both nodes name the same owner: " + a.clusterService().state().nodes() + " / " + b.clusterService().state().nodes(),
                    owner(a.clusterService().state(), ".probe-plugin-config"),
                    owner(b.clusterService().state(), ".probe-plugin-config")
                ),
                30,
                TimeUnit.SECONDS
            );
        }
    }

    /** A plugin's own create, alias and delete are in its node's view when the call returns, and reach the other node. */
    public void testChangesAppearHereAtOnceAndElsewhereSoon() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        try (ServerlessNode a = started("change-a", plane, Settings.EMPTY); ServerlessNode b = started("change-b", plane, Settings.EMPTY)) {
            a.services()
                .client()
                .admin()
                .indices()
                .create(new CreateIndexRequest(".probe-plugin-history-1").mapping(MAPPING).alias(new Alias(".probe-plugin-history")))
                .actionGet();
            final ClusterState here = a.clusterService().state();
            assertNotNull(here.metadata().index(".probe-plugin-history-1"));
            assertTrue("the alias is in the view", here.metadata().hasAlias(".probe-plugin-history"));
            assertEquals(
                ".probe-plugin-history-1",
                here.metadata().getIndicesLookup().get(".probe-plugin-history").getIndices().get(0).getIndex().getName()
            );

            assertBusy(() -> {
                final ClusterState there = b.clusterService().state();
                assertNotNull("the other node follows", there.metadata().index(".probe-plugin-history-1"));
                assertTrue(there.metadata().hasAlias(".probe-plugin-history"));
            }, 30, TimeUnit.SECONDS);

            a.services().client().admin().indices().delete(new DeleteIndexRequest(".probe-plugin-history-1")).actionGet();
            assertNull(a.clusterService().state().metadata().index(".probe-plugin-history-1"));
            assertBusy(
                () -> assertNull("a delete is followed too", b.clusterService().state().metadata().index(".probe-plugin-history-1")),
                30,
                TimeUnit.SECONDS
            );
        }
    }

    /** A search selecting shards searches those, and a pattern matching nothing answers empty, as core answers a plugin. */
    public void testShardSelectionAndAnEmptyPattern() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        try (ServerlessNode a = started("search-a", plane, Settings.EMPTY)) {
            final var client = a.services().client();
            client.admin().indices().create(new CreateIndexRequest(".probe-plugin-jobs").mapping(MAPPING)).actionGet();
            final BackgroundReconciler loop = new BackgroundReconciler(a, plane);
            loop.want(".probe-plugin-jobs", 0);
            loop.tick(clock.get());
            client.index(
                new IndexRequest(".probe-plugin-jobs").id("1")
                    .source("{\"msg\":\"x\"}", org.opensearch.common.xcontent.XContentType.JSON)
                    .setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE)
            ).actionGet();

            final SearchResponse selected = client.search(new SearchRequest(".probe-plugin-jobs").preference("_shards:0|_only_local"))
                .actionGet();
            assertEquals(1, selected.getHits().getTotalHits().value());

            final SearchResponse none = client.search(new SearchRequest(".probe-plugin-comments*")).actionGet();
            assertEquals(0, none.getHits().getTotalHits().value());
            assertEquals(0, none.getTotalShards());
        }
    }

    /** With the switch off, the view holds only what the node serves, as before. */
    public void testTheSwitchLeavesThemOut() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor(".probe-plugin-config", "uuid-config-0000000", 1, MAPPING, null));
        try (ServerlessNode a = started("off-a", plane, Settings.builder().put("serverless.plugin_indices_view.enabled", false).build())) {
            assertNull(a.clusterService().state().metadata().index(".probe-plugin-config"));
        }
    }
}
