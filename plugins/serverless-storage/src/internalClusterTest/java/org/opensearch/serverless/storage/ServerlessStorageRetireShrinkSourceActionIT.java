/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.resharding.action.RetireShrinkSourceAction;
import org.opensearch.serverless.storage.resharding.action.RetireShrinkSourceRequest;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Optional;

/**
 * Proves {@link RetireShrinkSourceAction} genuinely enforces rfc-serverless-opensearch.md &sect;16
 * Phase 5's own "never auto-deletes anything it didn't itself just create" caution, applied to
 * shrink sources: never deletes without first confirming the shrink target genuinely has a
 * published manifest, over the real transport layer, in a real cluster.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageRetireShrinkSourceActionIT extends ServerlessStorageIntegTestCase {

    private static final int SHARD_ID = 0;

    @Override
    protected java.util.Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private static BlobContainer blobContainerFor(Path basePath, String indexUuid, int shardId) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        return blobStore.blobContainer(shardPath);
    }

    /** Directly publishes a manifest for {@code indexUuid}, bypassing any real shrink -- this test only needs a real published target to verify against, not a real merge. */
    private static void publishTarget(BlobContainer container, String indexUuid) throws Exception {
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(indexUuid + "-doc-0"), IdFieldMapper.Defaults.FIELD_TYPE));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                indexUuid,
                SHARD_ID,
                1,
                1,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(indexUuid, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, manifest.generation()))
            );
        }
    }

    public void testRefusesToRetireWhenTheShrinkTargetHasNoPublishedManifest() throws Exception {
        Path basePath = createTempDir("serverless-storage-retire-shrink-source-it-no-target");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            "retire-source-idx-1",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen("retire-source-idx-1");

        expectThrows(
            Exception.class,
            () -> client().execute(
                RetireShrinkSourceAction.INSTANCE,
                new RetireShrinkSourceRequest("retire-source-idx-1", "never-shrunk-target", SHARD_ID, true)
            ).get()
        );

        assertTrue(
            "refusing to retire must never delete the source index",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("retire-source-idx-1")
        );
    }

    public void testRetiresTheSourceOnceTheShrinkTargetIsVerifiedPublished() throws Exception {
        Path basePath = createTempDir("serverless-storage-retire-shrink-source-it-real-target");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            "retire-source-idx-2",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen("retire-source-idx-2");

        String targetUuid = "retire-shrink-source-it-real-target-uuid";
        publishTarget(blobContainerFor(basePath, targetUuid, SHARD_ID), targetUuid);

        AcknowledgedResponse response = client().execute(
            RetireShrinkSourceAction.INSTANCE,
            new RetireShrinkSourceRequest("retire-source-idx-2", targetUuid, SHARD_ID, true)
        ).get();
        assertTrue("retirement must be acknowledged once the target is verified", response.isAcknowledged());

        assertFalse(
            "the source index must genuinely be deleted, not merely acknowledged as a no-op",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("retire-source-idx-2")
        );
    }

    public void testRefusesToRetireAnUnfencedSourceWithoutExplicitAcknowledgement() throws Exception {
        Path basePath = createTempDir("serverless-storage-retire-shrink-source-it-unfenced");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            "retire-source-idx-3",
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen("retire-source-idx-3");

        String targetUuid = "retire-shrink-source-it-unfenced-target-uuid";
        publishTarget(blobContainerFor(basePath, targetUuid, SHARD_ID), targetUuid);

        expectThrows(
            Exception.class,
            () -> client().execute(
                RetireShrinkSourceAction.INSTANCE,
                new RetireShrinkSourceRequest("retire-source-idx-3", targetUuid, SHARD_ID, false)
            ).get()
        );

        assertTrue(
            "refusing to retire an unfenced, unacknowledged source must never delete it",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("retire-source-idx-3")
        );
    }

    public void testRefusesToRetireAnOrdinaryNonServerlessStorageIndex() throws Exception {
        Path basePath = createTempDir("serverless-storage-retire-shrink-source-it-ordinary-index");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            "ordinary-idx",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("ordinary-idx");

        expectThrows(
            Exception.class,
            () -> client().execute(RetireShrinkSourceAction.INSTANCE, new RetireShrinkSourceRequest("ordinary-idx", "some-target", 0, true))
                .get()
        );

        assertTrue(
            "this action must never touch a non-serverless-storage index",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("ordinary-idx")
        );
    }
}
