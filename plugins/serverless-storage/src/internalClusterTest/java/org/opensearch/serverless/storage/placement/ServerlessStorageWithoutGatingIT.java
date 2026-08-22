/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.descriptor.DescriptorGate;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * An index that uses serverless storage without being gated, which the namespace made a real
 * configuration rather than an accident.
 *
 * <h2>Why this exists</h2>
 *
 * The storage setting and the namespace mean different things now. {@code index.serverless_storage.enabled}
 * selects the object-store engine; the {@code serverless_} name decides whether the index has a cluster state
 * entry. An alias-bearing index and a data stream backing index need the first and cannot have the second,
 * so "serverless storage, ordinary metadata" has to work.
 *
 * <h2>What it caught</h2>
 *
 * It did not work, and the way it failed is why this is an integration test rather than a unit one. Computed
 * placement was decided by the setting while gating was decided by the name, so such an index got a cluster
 * state entry *and* unpublished routing: the ordinary allocator skipped it, because there was no routing
 * entry to allocate, and the on-demand opening path that serves gated indices skipped it too, because it is
 * not gated. Its shards were placed by nobody.
 *
 * <p>A write to it then did not fail. It retried, because for a shard that has not opened yet retrying is
 * the correct behaviour and nothing distinguishes "not yet" from "never". The first sighting of this was a
 * single unrelated test taking twenty minutes and reporting {@code no such index}. That is the failure this
 * asserts against, and why the write here is bounded in time rather than merely attempted.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWithoutGatingIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testAnUngatedServerlessIndexIsPlacedAndServable() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin()
                .indices()
                .create(new CreateIndexRequest("ungated-serverless").settings(serverlessStorage()))
                .actionGet()
                .isAcknowledged()
        );

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNotNull(
            "an index outside the namespace keeps its cluster state entry -- that is what not being gated is",
            state.metadata().index("ungated-serverless")
        );
        assertNotNull(
            "and its routing must be published, or nothing places its shards: the allocator only sees "
                + "published entries and on-demand opening only serves gated indices",
            state.routingTable().index("ungated-serverless")
        );

        // The assertion the twenty-minute hang would have failed. Bounded, because "not yet servable" and
        // "never servable" look identical to a write and differ only in how long you are willing to wait.
        client().prepareIndex("ungated-serverless").setId("1").setSource("f", "v").get();
        client().admin().indices().prepareRefresh("ungated-serverless").get();
        assertEquals(1, client().prepareSearch("ungated-serverless").get().getHits().getTotalHits().value());
    }

    /**
     * The control, in the same cluster: the namespaced index still gets none of that and still works. Without
     * it, registering the ordinary path for everything would pass the test above and quietly un-gate the
     * whole feature.
     */
    public void testAGatedIndexStillHasNoEntryAndNoPublishedRouting() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin()
                .indices()
                .create(new CreateIndexRequest("serverless_placed-by-computation").settings(serverlessStorage()))
                .actionGet()
                .isAcknowledged()
        );

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull("a gated index must have no cluster state entry", state.metadata().index("serverless_placed-by-computation"));
        assertNull("and no published routing entry", state.routingTable().index("serverless_placed-by-computation"));

        assertBusy(
            () -> { client().prepareIndex("serverless_placed-by-computation").setId("1").setSource("f", "v").get(); },
            60,
            TimeUnit.SECONDS
        );
        client().admin().indices().prepareRefresh("serverless_placed-by-computation").get();
        assertEquals(1, client().prepareSearch("serverless_placed-by-computation").get().getHits().getTotalHits().value());
    }

    private static Settings serverlessStorage() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
