/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.wal.WalChunkNaming;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;

/**
 * Proves {@code WalGcSchedulerTask} actually runs on its configured schedule through the real
 * {@code ServerlessStoragePlugin#createComponents} wiring, not just via a direct {@code
 * sweepForTesting()} call ({@code WalGcSchedulerTaskTests} already covers the sweep logic itself
 * in isolation) -- a durably-published commit's WAL chunk is genuinely deleted from disk by a real,
 * running cluster.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWalGcSchedulerTaskIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-wal-gc-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private static BlobContainer walBlobContainer(Path basePath) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        return blobStore.blobContainer(BlobPath.cleanPath().add("wal"));
    }

    public void testTheScheduledSweepDeletesAWalChunkAlreadyCoveredByAPublishedManifest() throws Exception {
        Path basePath = createTempDir("serverless-storage-wal-gc-scheduler");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
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

        // durable ack (the default): this write is already WAL-durable by the time .get() returns.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        // Publishing only happens on flush -- makes doc 1's WAL coverage part of a real, durably
        // published manifest the sweep can compute a safe deletion bound from.
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        BlobContainer walBlobContainer = walBlobContainer(basePath);
        // The lone writer's own latest manifest now covers every chunk written so far -- the real
        // scheduled sweep should delete all of them within a couple of its 1 s ticks.
        assertBusy(
            () -> assertThat(walBlobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet(), empty()),
            30,
            TimeUnit.SECONDS
        );
    }
}
