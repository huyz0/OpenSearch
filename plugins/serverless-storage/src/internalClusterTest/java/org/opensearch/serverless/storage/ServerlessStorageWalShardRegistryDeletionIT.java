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
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.wal.RegisteredShard;
import org.opensearch.serverless.storage.wal.WalShardRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;

/**
 * Proves the real end-to-end lifecycle of {@link WalShardRegistry} against a running cluster: a
 * writer shard with WAL mirroring enabled registers itself on activation, and deleting its index
 * removes it from the registry -- {@code ServerlessStoragePlugin#onIndexModule}'s deregistration
 * listener, the counterpart of {@code ServerlessStorageCloneDeletionIT} for clone pins.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWalShardRegistryDeletionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "serverless-wal-registry-idx";

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

    public void testDeletingAWriterIndexDeregistersItsShardFromTheWalShardRegistry() throws Exception {
        Path basePath = createTempDir("serverless-storage-wal-registry-deletion");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
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
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();

        WalShardRegistry registry = new WalShardRegistry(walBlobContainer(basePath));
        assertBusy(() -> assertEquals(Set.of(new RegisteredShard(indexUuid, 0)), registry.registeredShards()), 30, TimeUnit.SECONDS);

        client().admin().indices().prepareDelete(INDEX_NAME).get();

        assertBusy(() -> assertThat(registry.registeredShards(), empty()), 30, TimeUnit.SECONDS);
    }
}
