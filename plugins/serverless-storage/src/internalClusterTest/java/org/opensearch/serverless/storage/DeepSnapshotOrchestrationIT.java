/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A real, independent snapshot of a serverless index -- bytes copied into the repository, restorable by
 * core, with nothing left pointing at the source object store.
 *
 * <h2>What this proves, and why it is the piece that was uncertain</h2>
 *
 * {@code BundleBackedCommitIsCopyableTests} proved the readable half: a commit opened over bundles can be
 * read end to end, file by file, with checksums verified. What it could not answer is whether a caller
 * outside {@code SnapshotsService} can drive the rest -- {@code snapshotShard} to copy the files, then
 * {@code finalizeSnapshot} to write the repository bookkeeping that makes those files a snapshot rather than
 * a pile of blobs. That orchestration is normally owned by cluster-state machinery, and owning it from a
 * plugin was the one part of the design with no precedent to copy.
 *
 * <p>So the assertion is not "the files are there". It is that <b>core restores it</b>: the snapshot is
 * restored under a new name through the ordinary {@code _restore} API, by the same machinery that restores
 * any snapshot of any index, and the documents come back. Anything less would leave open whether what was
 * written is a snapshot or only looks like one.
 *
 * <h2>What makes it independent</h2>
 *
 * The pointer snapshot this plugin ships writes a few hundred bytes naming a manifest generation, and
 * restore reads the original bundles -- so it does not survive losing the source store. Here the bytes are
 * read out of the bundles by this node and written into the repository in the standard format, so the
 * restored index is built from the repository's own copy. The source object store is not consulted during
 * the restore, which is the whole point.
 */
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = org.opensearch.serverless.storage.readerengine.lazydirectory.CleanerDaemonThreadLeakFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DeepSnapshotOrchestrationIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "deep-snapshot-source";
    private static final String REPO_NAME = "deep-snapshot-repo";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testAServerlessIndexCanBeCopiedIntoARepositoryAndRestoredByCore() throws Exception {
        Path basePath = createTempDir("deep-snapshot");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        String clusterManager = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNode = internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        for (int i = 0; i < 25; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("f", "value-" + i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(25, count(INDEX_NAME));

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        client().admin()
            .cluster()
            .preparePutRepository(REPO_NAME)
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder().put("location", basePath.resolve("repo")))
            .get();

        // The generation being copied. In the shipped feature a pin would hold it for the duration of the
        // copy, so GC cannot reclaim what is being read; here the index is quiet, and the pin lifecycle is
        // the wiring this test exists to justify rather than something it needs in order to prove the point.
        ServerlessStoragePlugin plugin = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNode);
        BlobContainer shardContainer = plugin.blobContainerForDirectoryFactory(indexUuid, 0);
        List<CommitManifest> manifests = new BlobContainerManifestStore(shardContainer).listManifests();
        CommitManifest latest = manifests.stream()
            .max(
                (a, b) -> a.generation() != b.generation()
                    ? Long.compare(a.generation(), b.generation())
                    : Long.compare(a.primaryTerm(), b.primaryTerm())
            )
            .orElseThrow();

        Repository repository = internalCluster().getInstance(RepositoriesService.class, dataNode).repository(REPO_NAME);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, dataNode);
        SnapshotId snapshotId = new SnapshotId("deep-snapshot-1", UUID.randomUUID().toString());
        IndexId indexId = new IndexId(INDEX_NAME, indexUuid);
        ShardId shardId = new ShardId(INDEX_NAME, indexUuid, 0);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), Settings.EMPTY);

        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
        String shardGeneration;
        try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
            TransferManager transferManager = new TransferManager(
                new BlobContainerBundleStore(shardContainer)::openRange,
                fileCache,
                threadPool
            );
            // No live shard, no engine, no local Lucene directory: the copy is driven from a manifest and
            // the object store alone, which is what would let it run on a node holding no copy of the index.
            try (
                LazyBundleDirectory lazyDirectory = new LazyBundleDirectory(latest, cacheDirectory, transferManager);
                Store store = new Store(shardId, indexSettings, lazyDirectory, new DummyShardLock(shardId))
            ) {
                // Listed from the store's directory rather than the raw one. Store wraps what it is given,
                // and Store#getMetadata asserts the commit's directory is identity-equal to its own -- which
                // holds for the real path because the engine opens its writer on store.directory(). A commit
                // taken from the unwrapped directory trips that assertion, which is a good assertion: it is
                // what stops a snapshot copying files from one directory while describing another.
                IndexCommit commit = DirectoryReader.listCommits(store.directory()).get(0);
                IndexShardSnapshotStatus status = IndexShardSnapshotStatus.newInitializing(null);
                CompletableFuture<String> copied = new CompletableFuture<>();
                store.incRef();
                try {
                    repository.snapshotShard(
                        store,
                        null,
                        snapshotId,
                        indexId,
                        commit,
                        null,
                        status,
                        Version.CURRENT,
                        Collections.emptyMap(),
                        org.opensearch.core.action.ActionListener.wrap(copied::complete, copied::completeExceptionally)
                    );
                    shardGeneration = copied.get(120, java.util.concurrent.TimeUnit.SECONDS);
                } finally {
                    store.decRef();
                }
            }
        } finally {
            fileCache.clear();
        }
        assertNotNull("snapshotShard must report the shard generation it wrote", shardGeneration);

        // The bookkeeping that turns copied files into a snapshot, and it runs on the cluster manager --
        // which is not a detail, it is the shape of the feature. finalizeSnapshot submits a cluster state
        // update ("set pending repository generation"), so any other node gets NotClusterManagerException.
        // The copy above has no such constraint: it read from the object store and wrote to the repository
        // from a data node. So a deep snapshot is one cluster-manager operation per snapshot with all of the
        // byte movement distributable off it, rather than the per-index cluster-manager work this branch
        // spent the year removing.
        Metadata clusterMetadata = client().admin().cluster().prepareState().get().getState().metadata();
        Repository onClusterManager = internalCluster().getInstance(RepositoriesService.class, clusterManager).repository(REPO_NAME);
        RepositoryData repositoryData = getRepositoryData(onClusterManager);
        long now = System.currentTimeMillis();
        SnapshotInfo snapshotInfo = new SnapshotInfo(
            snapshotId,
            List.of(INDEX_NAME),
            Collections.emptyList(),
            now - 1,
            null,
            now,
            1,
            Collections.emptyList(),
            false,
            Collections.emptyMap(),
            false
        );
        CompletableFuture<RepositoryData> finalized = new CompletableFuture<>();
        onClusterManager.finalizeSnapshot(
            ShardGenerations.builder().put(indexId, 0, shardGeneration).build(),
            repositoryData.getGenId(),
            clusterMetadata,
            snapshotInfo,
            Version.CURRENT,
            state -> state,
            org.opensearch.core.action.ActionListener.wrap(finalized::complete, finalized::completeExceptionally)
        );
        finalized.get(120, java.util.concurrent.TimeUnit.SECONDS);

        assertTrue(
            "the repository must list the snapshot this wrote, or the bookkeeping did not take",
            getRepositoryData(onClusterManager).getSnapshotIds().stream().anyMatch(id -> id.getName().equals("deep-snapshot-1"))
        );

        // The assertion that matters: core restores it, through the ordinary API, with no help from this
        // plugin's own restore path. If this passes, what was written is a snapshot rather than something
        // shaped like one.
        RestoreSnapshotResponse restored = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO_NAME, "deep-snapshot-1")
            .setIndices(INDEX_NAME)
            .setRenamePattern(INDEX_NAME)
            .setRenameReplacement("deep-snapshot-restored")
            .setIndexSettings(Settings.builder().put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), false))
            .setWaitForCompletion(true)
            .get();
        assertEquals(1, restored.getRestoreInfo().successfulShards());

        ensureGreen("deep-snapshot-restored");
        client().admin().indices().prepareRefresh("deep-snapshot-restored").get();
        assertEquals("every document must come back from the repository's own copy of the bytes", 25, count("deep-snapshot-restored"));
    }

    private RepositoryData getRepositoryData(Repository repository) throws Exception {
        CompletableFuture<RepositoryData> future = new CompletableFuture<>();
        repository.getRepositoryData(org.opensearch.core.action.ActionListener.wrap(future::complete, future::completeExceptionally));
        return future.get(60, java.util.concurrent.TimeUnit.SECONDS);
    }

    private long count(String index) {
        return client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value();
    }
}
