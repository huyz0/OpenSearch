/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.snapshots.blobstore.EngineNativeShardSnapshot;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.serverless.storage.writerengine.EngineNativeSnapshotSupport;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Proves this plugin's writer engine participates in core's <em>real</em> {@code _snapshot}/{@code
 * _restore} machinery (rfc-serverless-opensearch.md's engine-native snapshot extension point, see
 * {@code docs-site/src/content/docs/design/snapshot-restore-proposal.md}) end to end: a real {@code
 * fs} repository registered through {@code PUT _snapshot}, a real {@code CreateSnapshotAction} that
 * pins the shard's current manifest generation instead of copying Lucene bytes, and a real {@code
 * RestoreSnapshotAction} that materializes back from that pin -- not just that the two custom,
 * plugin-owned {@code SnapshotPinAction}/{@code SnapshotRestoreAction} transport actions work
 * (already covered by {@link ServerlessStorageSnapshotRestoreActionIT}, a completely separate,
 * narrower "snapshot = pinned manifest set" mechanism unrelated to this one).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageEngineNativeSnapshotIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "engine-native-snapshot-it-idx";
    private static final String REPO_NAME = "engine-native-snapshot-it-repo";
    private static final String SNAPSHOT_NAME = "engine-native-snapshot-it-snap-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testRealSnapshotAndRestoreAgainstAFsRepositoryUsesTheEngineNativePathAndRecoversTheDocument() throws Exception {
        Path serverlessStorageBasePath = createTempDir("engine-native-snapshot-it-storage");
        Path snapshotRepoPath = createTempDir("engine-native-snapshot-it-repo");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", List.of(serverlessStorageBasePath.toString(), snapshotRepoPath.toString()))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), serverlessStorageBasePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        assertTrue(
            "repository registration must be acknowledged before it's usable",
            client().admin()
                .cluster()
                .preparePutRepository(REPO_NAME)
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), snapshotRepoPath.toString()))
                .get()
                .isAcknowledged()
        );

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        // A real _snapshot only ever sees a shard's already-durable state -- the writer engine
        // publishes a manifest on flush, exactly like every other real commit this plugin produces.
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        CreateSnapshotResponse createResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(REPO_NAME, SNAPSHOT_NAME)
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        assertEquals(SnapshotState.SUCCESS, createResponse.getSnapshotInfo().state());

        // The critical check: prove the engine-native branch was actually taken, not a silent
        // fallback to the classic copy-based path that would also happen to "succeed" here.
        RepositoriesService repositoriesService = internalCluster().getDataNodeInstance(RepositoriesService.class);
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REPO_NAME);
        PlainActionFuture<RepositoryData> repositoryDataFuture = PlainActionFuture.newFuture();
        repository.getRepositoryData(repositoryDataFuture);
        RepositoryData repositoryData = repositoryDataFuture.actionGet();
        IndexId indexId = repositoryData.resolveIndexId(INDEX_NAME);
        SnapshotId snapshotId = createResponse.getSnapshotInfo().snapshotId();

        Optional<EngineNativeShardSnapshot> engineNativeMetadata = repository.getEngineNativeShardSnapshotMetadata(
            snapshotId,
            indexId,
            new ShardId(INDEX_NAME, "_na_", 0)
        );
        assertTrue(
            "the snapshot must have been written via the engine-native path, not a silent fallback to the classic copy-based path",
            engineNativeMetadata.isPresent()
        );
        assertEquals(EngineNativeSnapshotSupport.ENGINE_ID, engineNativeMetadata.get().engineId());

        // Restoring into an existing index requires it closed first -- the same core restore
        // precondition ServerlessStorageSnapshotRestoreActionIT relies on for its own, unrelated
        // restore mechanism.
        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());

        RestoreSnapshotResponse restoreResponse = client().admin()
            .cluster()
            .prepareRestoreSnapshot(REPO_NAME, SNAPSHOT_NAME)
            .setIndices(INDEX_NAME)
            .setWaitForCompletion(true)
            .get();
        assertEquals(0, restoreResponse.getRestoreInfo().failedShards());
        ensureGreen(INDEX_NAME);

        assertTrue(
            "the document present at snapshot time must have been materialized back by " + "EngineNativeSnapshotSupport#restore, not lost",
            client().prepareGet(INDEX_NAME, "1").get().isExists()
        );
    }
}
