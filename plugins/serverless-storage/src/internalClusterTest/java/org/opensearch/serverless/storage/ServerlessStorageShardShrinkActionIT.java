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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
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
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.resharding.action.ShardRef;
import org.opensearch.serverless.storage.resharding.action.ShardShrinkAction;
import org.opensearch.serverless.storage.resharding.action.ShardShrinkRequest;
import org.opensearch.serverless.storage.resharding.action.ShardShrinkResponse;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Proves {@link ShardShrinkAction} genuinely merges several real, independent shards into one
 * brand-new target over the real transport layer, in a real cluster -- not just a direct
 * unit-level call to {@code ShardShrinker.shrink} (rfc-serverless-opensearch.md &sect;16 Phase 5).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardShrinkActionIT extends ServerlessStorageIntegTestCase {

    private static final String SOURCE_A_UUID = "shrink-action-it-source-a";
    private static final String SOURCE_B_UUID = "shrink-action-it-source-b";
    private static final String TARGET_UUID = "shrink-action-it-target";
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

    private static void publishSource(BlobContainer container, String indexUuid, int docCount) throws Exception {
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < docCount; i++) {
                    Document doc = new Document();
                    doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(indexUuid + "-doc-" + i), IdFieldMapper.Defaults.FIELD_TYPE));
                    writer.addDocument(doc);
                }
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
                docCount,
                docCount,
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

    public void testShardShrinkActionMergesTwoRealSourcesIntoOneTargetOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-shrink-action-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        BlobContainer sourceAContainer = blobContainerFor(basePath, SOURCE_A_UUID, SHARD_ID);
        BlobContainer sourceBContainer = blobContainerFor(basePath, SOURCE_B_UUID, SHARD_ID);
        publishSource(sourceAContainer, SOURCE_A_UUID, 20);
        publishSource(sourceBContainer, SOURCE_B_UUID, 25);

        ShardShrinkResponse response = client().execute(
            ShardShrinkAction.INSTANCE,
            new ShardShrinkRequest(
                List.of(new ShardRef(SOURCE_A_UUID, SHARD_ID), new ShardRef(SOURCE_B_UUID, SHARD_ID)),
                TARGET_UUID,
                SHARD_ID
            )
        ).get();
        assertTrue("the shrink must be acknowledged", response.acknowledged());

        BlobContainer targetContainer = blobContainerFor(basePath, TARGET_UUID, SHARD_ID);
        ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        VersionedShardHead targetHead = targetShardStateStore.get(TARGET_UUID, SHARD_ID).orElseThrow();
        BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.head().primaryTerm(),
            targetHead.head().latestManifestGeneration()
        );

        Directory targetDirectory = new ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(targetContainer)).materialize(targetManifest, targetDirectory);
        try (DirectoryReader reader = DirectoryReader.open(targetDirectory)) {
            assertEquals("the merged target must contain every document from both real sources", 45, reader.numDocs());
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < reader.maxDoc(); i++) {
                byte[] idBytes = reader.storedFields().document(i).getField(IdFieldMapper.NAME).binaryValue().bytes;
                ids.add(Uid.decodeId(idBytes));
            }
            assertEquals("no document must be lost or duplicated across the merge", 45, ids.size());
        }
    }

    public void testShardShrinkActionFailsWithoutSwallowingWhenASourceHasNoPublishedManifest() throws Exception {
        Path basePath = createTempDir("serverless-storage-shrink-action-it-missing-source");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        BlobContainer sourceAContainer = blobContainerFor(basePath, SOURCE_A_UUID + "-2", SHARD_ID);
        publishSource(sourceAContainer, SOURCE_A_UUID + "-2", 5);

        expectThrows(
            Exception.class,
            () -> client().execute(
                ShardShrinkAction.INSTANCE,
                new ShardShrinkRequest(
                    List.of(new ShardRef(SOURCE_A_UUID + "-2", SHARD_ID), new ShardRef("never-published-idx", SHARD_ID)),
                    TARGET_UUID + "-2",
                    SHARD_ID
                )
            ).get()
        );
    }
}
