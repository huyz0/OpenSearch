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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.ShardCloner;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.empty;

/**
 * Proves {@code ServerlessStoragePlugin#onIndexModule}'s clone-cleanup listener actually fires on
 * a real index deletion, not just in a unit-level call to {@code ShardCloner.deleteClone} directly
 * (rfc-serverless-opensearch.md &sect;14).
 *
 * <p>The "source" side here is a synthetic {@code BlobContainer} location under the shared
 * {@code base_path}, not a real OpenSearch index -- {@link ShardCloner} only needs a
 * string identity and a matching on-disk layout, and this keeps the test from needing two real
 * indices with real shard allocation just to prove the deletion hook fires for the target.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageCloneDeletionIT extends ServerlessStorageIntegTestCase {

    private static final String TARGET_INDEX_NAME = "serverless-clone-target-idx";
    private static final String SOURCE_INDEX_UUID = "synthetic-clone-source-idx";
    private static final int SHARD_ID = 0;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
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

    public void testDeletingAClonedIndexReleasesTheSourceSidePinAutomatically() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-deletion");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        // A synthetic source shard, with a real published manifest, that never corresponds to any
        // actual OpenSearch index -- see class javadoc.
        BlobContainer sourceContainer = blobContainerFor(basePath, SOURCE_INDEX_UUID, SHARD_ID);
        BlobContainerBundleStore sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        BlobContainerManifestStore sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        ShardStateStore sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        DurablePinRegistry sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);

        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest sourceManifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                SOURCE_INDEX_UUID,
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
                sourceShardStateStore.compareAndSet(
                    SOURCE_INDEX_UUID,
                    SHARD_ID,
                    Optional.empty(),
                    new ShardHead(1, null, 0L, sourceManifest.generation())
                )
            );
        }

        // A real target index -- its real UUID is what ServerlessStoragePlugin's deletion listener
        // will see, so the clone must be written under that same UUID for the two to line up.
        createIndex(
            TARGET_INDEX_NAME,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen(TARGET_INDEX_NAME);
        String targetIndexUuid = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .metadata()
            .index(TARGET_INDEX_NAME)
            .getIndexUUID();

        BlobContainer targetContainer = blobContainerFor(basePath, targetIndexUuid, SHARD_ID);
        BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
        ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        BlobContainerCloneLineageStore targetLineageStore = new BlobContainerCloneLineageStore(targetContainer);

        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            targetIndexUuid,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            System.currentTimeMillis()
        );

        Set<org.opensearch.serverless.storage.retention.PinRecord> pinsBeforeDelete = sourcePinRegistry.getPins(
            SOURCE_INDEX_UUID,
            SHARD_ID
        );
        assertEquals(1, pinsBeforeDelete.size());
        assertTrue(targetLineageStore.readLineage().isPresent());

        client().admin().indices().prepareDelete(TARGET_INDEX_NAME).get();

        // afterIndexRemoved runs asynchronously with respect to the delete API response completing
        // on this node -- assertBusy rather than an immediate check.
        assertBusy(() -> {
            assertThat(sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID), empty());
            assertTrue(targetLineageStore.readLineage().isEmpty());
        }, 30, TimeUnit.SECONDS);
    }
}
