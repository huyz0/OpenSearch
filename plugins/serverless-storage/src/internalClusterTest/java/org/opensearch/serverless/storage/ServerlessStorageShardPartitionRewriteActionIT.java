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
import org.opensearch.serverless.storage.resharding.BlobContainerShardPartitionStore;
import org.opensearch.serverless.storage.resharding.RoutingPartitionFilter;
import org.opensearch.serverless.storage.resharding.ShardPartitionDescriptor;
import org.opensearch.serverless.storage.resharding.action.ShardPartitionRewriteAction;
import org.opensearch.serverless.storage.resharding.action.ShardPartitionRewriteRequest;
import org.opensearch.serverless.storage.resharding.action.ShardPartitionRewriteResponse;
import org.opensearch.serverless.storage.resharding.action.ShardSplitAction;
import org.opensearch.serverless.storage.resharding.action.ShardSplitRequest;
import org.opensearch.serverless.storage.resharding.action.ShardSplitResponse;
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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Proves the full resharding-by-copy lifecycle end to end, over the real transport layer, in a
 * real cluster: split a source into a target, confirm the target's manifest still holds the full
 * pre-split document set (the "logical-first" phase), trigger {@link ShardPartitionRewriteAction},
 * then confirm the target's manifest now holds only its own partition's documents and the
 * descriptor is gone (the "physical-later" phase closing) -- rfc-serverless-opensearch.md
 * &sect;16 Phase 5.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardPartitionRewriteActionIT extends OpenSearchIntegTestCase {

    private static final String SOURCE_INDEX_UUID = "partition-rewrite-it-source-idx";
    private static final String TARGET_INDEX_UUID = "partition-rewrite-it-target-idx";
    private static final int SHARD_ID = 0;
    private static final int DOC_COUNT = 60;
    private static final int NUM_PARTITIONS = 3;
    private static final int PARTITION_INDEX = 0;

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

    public void testSplitThenRewriteEndsWithAPhysicallyPartitionedTargetAndNoDescriptor() throws Exception {
        Path basePath = createTempDir("serverless-storage-partition-rewrite-it");
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
        Set<String> expectedPartitionIds = new HashSet<>();
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < DOC_COUNT; i++) {
                    String id = "doc-" + i;
                    Document doc = new Document();
                    doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
                    writer.addDocument(doc);
                    if (RoutingPartitionFilter.matches(id, new ShardPartitionDescriptor(PARTITION_INDEX, NUM_PARTITIONS))) {
                        expectedPartitionIds.add(id);
                    }
                }
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
                DOC_COUNT,
                DOC_COUNT,
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
        assertFalse("this test needs a real, non-trivial partition to be meaningful", expectedPartitionIds.isEmpty());
        assertTrue(
            "this test needs the partition to be a strict subset to prove the rewrite actually shrank anything",
            expectedPartitionIds.size() < DOC_COUNT
        );

        ShardSplitResponse splitResponse = client().execute(
            ShardSplitAction.INSTANCE,
            new ShardSplitRequest(SOURCE_INDEX_UUID, SHARD_ID, TARGET_INDEX_UUID, SHARD_ID, PARTITION_INDEX, NUM_PARTITIONS)
        ).get();
        assertTrue(splitResponse.acknowledged());

        BlobContainer targetContainer = blobContainerFor(basePath, TARGET_INDEX_UUID, SHARD_ID);
        BlobContainerManifestStore targetManifestStore = new BlobContainerManifestStore(targetContainer);
        ShardStateStore targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        BlobContainerShardPartitionStore targetPartitionStore = new BlobContainerShardPartitionStore(targetContainer);

        VersionedShardHead afterSplit = targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).orElseThrow();
        CommitManifest manifestAfterSplit = targetManifestStore.readManifest(
            afterSplit.head().primaryTerm(),
            afterSplit.head().latestManifestGeneration()
        );
        assertEquals(
            "right after a split, the target's manifest must still be the full, unfiltered pre-split "
                + "document set -- logical-first, physical still pending",
            DOC_COUNT,
            countDocsInManifest(
                new org.opensearch.serverless.storage.clone.FallbackBundleFileReader(
                    new BlobContainerBundleStore(targetContainer),
                    sourceBundleStore
                ),
                manifestAfterSplit
            )
        );
        assertTrue(
            "the target must have a real partition descriptor right after the split",
            targetPartitionStore.readDescriptor().isPresent()
        );

        ShardPartitionRewriteResponse rewriteResponse = client().execute(
            ShardPartitionRewriteAction.INSTANCE,
            new ShardPartitionRewriteRequest(TARGET_INDEX_UUID, SHARD_ID)
        ).get();
        assertTrue("the rewrite must have actually run", rewriteResponse.rewritten());

        assertTrue("the descriptor must be gone once the rewrite is durably published", targetPartitionStore.readDescriptor().isEmpty());

        VersionedShardHead afterRewrite = targetShardStateStore.get(TARGET_INDEX_UUID, SHARD_ID).orElseThrow();
        assertTrue(
            "the rewrite must have published a strictly newer generation than the split's own",
            afterRewrite.head().latestManifestGeneration() > afterSplit.head().latestManifestGeneration()
        );
        CommitManifest manifestAfterRewrite = targetManifestStore.readManifest(
            afterRewrite.head().primaryTerm(),
            afterRewrite.head().latestManifestGeneration()
        );
        assertEquals(
            "after the rewrite, the target's manifest must hold exactly this partition's documents, "
                + "a real, strictly smaller physical bundle -- not the full pre-split set anymore",
            expectedPartitionIds.size(),
            countDocsInManifest(new BlobContainerBundleStore(targetContainer), manifestAfterRewrite)
        );

        // A second rewrite attempt must be a safe, acknowledged no-op.
        ShardPartitionRewriteResponse secondAttempt = client().execute(
            ShardPartitionRewriteAction.INSTANCE,
            new ShardPartitionRewriteRequest(TARGET_INDEX_UUID, SHARD_ID)
        ).get();
        assertFalse("a second rewrite attempt with no descriptor left must be a no-op, never an error", secondAttempt.rewritten());

        assertEquals(
            "the source's split pin must be untouched by the target's own rewrite",
            1,
            sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID).size()
        );
    }

    private static int countDocsInManifest(
        org.opensearch.serverless.storage.format.BundleFileReader bundleReadPath,
        CommitManifest manifest
    ) throws Exception {
        Directory directory = new ByteBuffersDirectory();
        new org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer(bundleReadPath).materialize(manifest, directory);
        try (org.apache.lucene.index.DirectoryReader reader = org.apache.lucene.index.DirectoryReader.open(directory)) {
            return reader.numDocs();
        }
    }
}
