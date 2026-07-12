/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.scaletozero.ScaleToZeroCandidatesSchedulerTask;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Proves {@link ScaleToZeroCandidatesSchedulerTask} actually runs on its configured schedule
 * through the real {@code ServerlessStoragePlugin#createComponents} wiring, not just via a direct
 * {@code evaluateForTesting()} call ({@code ScaleToZeroCandidatesSchedulerTaskTests} already covers
 * the scheduling/gating logic itself in isolation) -- a real idle shard is genuinely discovered by
 * a real, running, multi-node cluster's background evaluation.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageScaleToZeroCandidatesSchedulerTaskIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-scale-to-zero-scheduler-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testTheScheduledEvaluationPopulatesLatestCandidatesOnTheClusterManagerNode() throws Exception {
        Path basePath = createTempDir("serverless-storage-scale-to-zero-scheduler");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_TO_ZERO_EVAL_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            // Zero idle threshold: any observed idle time (always >= 0) qualifies, so the shard is
            // guaranteed to show up as a candidate without this test needing to wait out a real
            // multi-minute idle window.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING.getKey(), TimeValue.ZERO)
            .build();

        String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        ServerlessStoragePlugin clusterManagerPlugin = internalCluster().getInstance(ServerlessStoragePlugin.class, clusterManagerNode);
        assertBusy(() -> {
            ScaleToZeroCandidatesSchedulerTask task = clusterManagerPlugin.scaleToZeroCandidatesSchedulerTaskForTesting();
            assertNotNull("a positive eval_interval setting must build a real scheduler task", task);
            assertTrue(
                "the scheduled evaluation must have run at least once on the cluster-manager node and "
                    + "found this shard, without any test code ever calling evaluateForTesting() directly",
                task.latestCandidates().stream().anyMatch(e -> e.indexUuid().equals(indexUuid) && e.shardId() == 0 && e.candidate())
            );
        }, 30, TimeUnit.SECONDS);
    }
}
