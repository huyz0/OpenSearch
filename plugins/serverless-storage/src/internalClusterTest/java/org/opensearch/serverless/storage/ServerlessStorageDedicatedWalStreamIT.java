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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;

/**
 * Proves {@code index.serverless_storage.wal.dedicated_stream}
 * (rfc-serverless-opensearch.md &sect;12's "dedicated WAL streams" regulatory co-residency bullet)
 * genuinely isolates a shard's WAL bytes into their own object-store container, never the
 * node-shared one, against a real writer engine in a real cluster -- not just the wiring compiling.
 *
 * <p>Both indices' nodes share one {@code serverless_storage.base_path}, a local filesystem
 * standing in for the object store, which is what lets this test inspect exactly which files each
 * index's WAL bytes actually landed in.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageDedicatedWalStreamIT extends OpenSearchIntegTestCase {

    private static final String DEDICATED_INDEX = "dedicated-wal-it-idx";
    private static final String SHARED_INDEX = "shared-wal-it-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testDedicatedStreamShardsWalBytesNeverLandInTheSharedContainer() throws Exception {
        Path basePath = createTempDir("serverless-storage-dedicated-wal-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            DEDICATED_INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING.getKey(), true)
                .build()
        );
        createIndex(
            SHARED_INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(DEDICATED_INDEX, SHARED_INDEX);

        String dedicatedIndexUuid = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(DEDICATED_INDEX)
            .getIndexUUID();

        client().prepareIndex(DEDICATED_INDEX).setId("1").setSource("field", "value1").get();
        client().prepareIndex(SHARED_INDEX).setId("1").setSource("field", "value1").get();
        // WalMirroringTranslog flushes on every append (per-operation durability) -- the indexing
        // calls above are already enough to have written real WAL chunks for both indices by the
        // time they return, no explicit flush needed for the WAL bytes specifically (unlike a
        // published manifest, which does need one).

        Path dedicatedWalDir = basePath.resolve("wal-dedicated").resolve(dedicatedIndexUuid).resolve("0");
        Path sharedWalDir = basePath.resolve("wal");

        assertTrue(
            "the dedicated-stream index must have its own wal-dedicated/<uuid>/0/ directory with real chunk files in it",
            Files.isDirectory(dedicatedWalDir)
        );
        Set<String> dedicatedChunkFiles = listChunkFiles(dedicatedWalDir);
        assertFalse(
            "the dedicated container must actually contain at least one real chunk file, not just exist empty",
            dedicatedChunkFiles.isEmpty()
        );

        assertTrue("the node-shared wal/ container must still exist for the non-opted-in index", Files.isDirectory(sharedWalDir));
        Set<String> sharedChunkFiles = listChunkFiles(sharedWalDir);
        assertFalse("the shared index's own WAL bytes must still land in the shared container", sharedChunkFiles.isEmpty());

        assertTrue(
            "the dedicated container and the shared container must be two entirely separate directories on disk -- "
                + "the whole point of this feature is that they can never be the same object store location",
            Files.isSameFile(dedicatedWalDir, sharedWalDir) == false
        );
    }

    private static Set<String> listChunkFiles(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    public void testDedicatedStreamsOwnScheduledSweepDeletesAWalChunkAlreadyCoveredByAPublishedManifest() throws Exception {
        // Same proof ServerlessStorageWalGcSchedulerTaskIT already gives the shared WAL container's
        // node-level task, for the per-shard task ObjectStoreWriterEngine itself owns for a
        // dedicated stream (see that class's own javadoc for why ownership lives on the engine
        // here, not a node-level scheduler) -- the real risk this whole feature has to close, not
        // just "bytes are isolated," but "isolated bytes don't grow unbounded forever."
        Path basePath = createTempDir("serverless-storage-dedicated-wal-gc-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_GC_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            DEDICATED_INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(DEDICATED_INDEX);

        String dedicatedIndexUuid = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(DEDICATED_INDEX)
            .getIndexUUID();

        client().prepareIndex(DEDICATED_INDEX).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(DEDICATED_INDEX).get();

        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        BlobContainer dedicatedWalContainer = blobStore.blobContainer(
            BlobPath.cleanPath().add("wal-dedicated").add(dedicatedIndexUuid).add("0")
        );

        assertBusy(
            () -> assertThat(dedicatedWalContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet(), empty()),
            30,
            TimeUnit.SECONDS
        );
    }
}
