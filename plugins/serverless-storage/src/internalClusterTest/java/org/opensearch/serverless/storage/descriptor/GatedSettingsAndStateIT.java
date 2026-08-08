/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
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
 * Asserts off-thread dynamic settings updates, open, and close operations for gated indices
 * complete while the cluster manager's state update thread is held.
 */
public class GatedSettingsAndStateIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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
            public void onFailure(String source, Exception e) {}
        });
    }

    public void testSettingsOpenCloseOffClusterStateThread() throws Exception {
        installGate();

        // Create gated indices
        CreateIndexResponse create1 = client().admin()
            .indices()
            .create(new CreateIndexRequest("gated-settings-1").settings(gatedSettings()))
            .actionGet();
        assertTrue(create1.isAcknowledged());

        CreateIndexResponse create2 = client().admin()
            .indices()
            .create(new CreateIndexRequest("gated-settings-2").settings(gatedSettings()))
            .actionGet();
        assertTrue(create2.isAcknowledged());

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch held = new CountDownLatch(1);
        blockClusterStateThread(release, held);
        assertTrue("update thread block failed", held.await(30, TimeUnit.SECONDS));

        try {
            // Update settings off-thread
            AcknowledgedResponse settingsResp = client().admin()
                .indices()
                .prepareUpdateSettings("gated-settings-1")
                .setSettings(Settings.builder().put("index.refresh_interval", "5s"))
                .get();
            assertTrue(settingsResp.isAcknowledged());

            // Close gated index off-thread
            CloseIndexResponse closeResp = client().admin()
                .indices()
                .prepareClose("gated-settings-1")
                .get();
            assertTrue(closeResp.isAcknowledged());

            // Open gated index off-thread
            OpenIndexResponse openResp = client().admin()
                .indices()
                .prepareOpen("gated-settings-1")
                .get();
            assertTrue(openResp.isAcknowledged());
        } finally {
            release.countDown();
        }
    }
}
