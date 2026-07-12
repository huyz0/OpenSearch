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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.action.ShardCloneAction;
import org.opensearch.serverless.storage.clone.action.ShardCloneRequest;
import org.opensearch.serverless.storage.clone.action.ShardCloneResponse;
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
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Proves {@link ShardCloneAction} genuinely works over the transport layer, in a real cluster --
 * not just a direct unit-level call to {@code ShardCloner.clone} -- covering the Guice injection
 * of {@link ServerlessStoragePlugin} into {@code TransportShardCloneAction} and the {@code
 * ThreadPool.Names#GENERIC} dispatch, both of which only exist once the action actually runs
 * inside a real node (rfc-serverless-opensearch.md &sect;14).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardCloneActionIT extends ServerlessStorageIntegTestCase {

    private static final String SOURCE_INDEX_UUID = "action-it-source-idx";
    private static final String TARGET_INDEX_UUID = "action-it-target-idx";
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

    public void testShardCloneActionClonesARealPublishedSourceOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

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

        ShardCloneResponse response = client().execute(
            ShardCloneAction.INSTANCE,
            new ShardCloneRequest(SOURCE_INDEX_UUID, SHARD_ID, TARGET_INDEX_UUID, SHARD_ID)
        ).get();
        assertTrue(response.acknowledged());

        Set<org.opensearch.serverless.storage.retention.PinRecord> pins = sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID);
        assertEquals(1, pins.size());

        BlobContainer targetContainer = blobContainerFor(basePath, TARGET_INDEX_UUID, SHARD_ID);
        ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        Optional<VersionedShardHead> targetHead = targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID);
        assertTrue(targetHead.isPresent());
        assertTrue(new BlobContainerCloneLineageStore(targetContainer).readLineage().isPresent());
    }

    public void testShardCloneActionFailsWithoutSwallowingWhenSourceHasNoPublishedManifest() throws Exception {
        Path basePath = createTempDir("serverless-storage-clone-action-it-missing-source");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        expectThrows(
            Exception.class,
            () -> client().execute(
                ShardCloneAction.INSTANCE,
                new ShardCloneRequest("never-published-idx", SHARD_ID, TARGET_INDEX_UUID, SHARD_ID)
            ).get()
        );
    }
}
