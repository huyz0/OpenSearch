/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * T51. Auto-creation, rollover, and data stream backing index creation for gated templates
 * must complete without stalling or dying on the cluster manager's state update thread.
 */
public class GatedAutoCreateAndRolloverIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile Path sharedBasePath;
    private BlobDescriptorBackend store;

    private Path basePath() {
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
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .put(org.opensearch.serverless.storage.ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    private void installGate() throws Exception {
        store = installBlobBackedDescriptorPlane().points();
    }

    private static Settings plainSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }

    private static Settings gatedSettings() {
        return Settings.builder().put(plainSettings()).put("index.serverless_storage.enabled", true).build();
    }

    private void blockClusterStateThread(CountDownLatch release, CountDownLatch held) throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);
        clusterService.submitStateUpdateTask("block the cluster state update thread", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                held.countDown();
                release.await();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                held.countDown();
            }
        });
    }

    public void testAutoCreateAndRolloverCompleteOffClusterStateThread() throws Exception {
        installGate();

        // Install gated index template for auto-creation matching gated-*
        client().admin().indices().preparePutTemplate("gated-template").setPatterns(List.of("gated-*")).setSettings(gatedSettings()).get();

        // Warmup gated creation
        CreateIndexResponse warmup = client().admin()
            .indices()
            .create(new CreateIndexRequest("gated-warmup-1").settings(gatedSettings()))
            .actionGet();
        assertTrue(warmup.isAcknowledged());

        // Create initial index with alias for rollover test
        CreateIndexResponse init = client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-roll-000001").settings(gatedSettings())
                    .alias(new org.opensearch.action.admin.indices.alias.Alias("gated-alias"))
            )
            .actionGet();
        assertTrue(init.isAcknowledged());

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch held = new CountDownLatch(1);
        blockClusterStateThread(release, held);
        assertTrue("update thread block failed", held.await(30, TimeUnit.SECONDS));

        try {
            // Auto-create off-thread
            IndexResponse autoResp = client().prepareIndex("gated-autocreate-1").setId("1").setSource("field1", "val1").get();
            assertNotNull(autoResp.getId());

            // Rollover off-thread
            RolloverResponse rollResp = client().admin()
                .indices()
                .prepareRolloverIndex("gated-alias")
                .setNewIndexName("gated-roll-000002")
                .get();
            assertTrue(rollResp.isRolledOver());
            assertEquals("gated-roll-000002", rollResp.getNewIndex());
        } finally {
            release.countDown();
        }
    }
}
