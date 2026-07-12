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
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore;
import org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor;
import org.opensearch.serverless.storage.resharding.action.ShardSplitAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitRequest;
import org.opensearch.serverless.storage.resharding.action.ShardSplitResponse;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
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
 * Proves {@link ShardSplitAction} genuinely works over the transport layer, in a real cluster --
 * not just a direct unit-level call to {@code ShardSplitter.split} -- covering the Guice injection
 * of {@link ServerlessStoragePlugin} into {@code TransportShardSplitAction} and the {@code
 * ThreadPool.Names#GENERIC} dispatch, the same fidelity {@link ServerlessStorageShardCloneActionIT}
 * already established for plain clones (rfc-serverless-opensearch.md &sect;16 Phase 5). Splitting
 * one source three ways and checking every target's descriptor and the source's pin count is the
 * one thing a single clone's own IT doesn't need to cover: that a single source can be split more
 * than once without pins from one target clobbering another's.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardSplitActionIT extends ServerlessStorageIntegTestCase {

    private static final String SOURCE_INDEX_UUID = "split-action-it-source-idx";
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

    public void testShardSplitActionSplitsARealPublishedSourceThreeWaysOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-split-action-it");
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
        CommitManifest sourceManifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            sourceManifest = publisher.publishCommit(
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

        int numPartitions = 3;
        for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            String targetIndexUuid = SOURCE_INDEX_UUID + "-part-" + partitionIndex;
            ShardSplitResponse response = client().execute(
                ShardSplitAction.INSTANCE,
                new ShardSplitRequest(SOURCE_INDEX_UUID, SHARD_ID, targetIndexUuid, SHARD_ID, partitionIndex, numPartitions)
            ).get();
            assertTrue("split target " + partitionIndex + " must be acknowledged", response.acknowledged());

            BlobContainer targetContainer = blobContainerFor(basePath, targetIndexUuid, SHARD_ID);
            ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
            Optional<VersionedShardHead> targetHead = targetShardStateStore.get(targetIndexUuid, SHARD_ID);
            assertTrue("split target " + partitionIndex + " must have a real published head", targetHead.isPresent());

            BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
            CommitManifest targetManifest = targetManifestStore.readManifest(
                targetHead.get().head().primaryTerm(),
                targetHead.get().head().latestManifestGeneration()
            );
            assertEquals(
                "every split target must reference the exact same bundle files as the source",
                sourceManifest.files(),
                targetManifest.files()
            );

            Optional<ShardPartitionDescriptor> descriptor = new BlobContainerShardPartitionStore(targetContainer).readDescriptor();
            assertTrue(descriptor.isPresent());
            assertEquals(partitionIndex, descriptor.get().partitionIndex());
            assertEquals(numPartitions, descriptor.get().numPartitions());
        }

        Set<PinRecord> pins = sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID);
        assertEquals(
            "the source must carry one independent pin per split target, not one shared/clobbered pin",
            numPartitions,
            pins.size()
        );
    }
}
