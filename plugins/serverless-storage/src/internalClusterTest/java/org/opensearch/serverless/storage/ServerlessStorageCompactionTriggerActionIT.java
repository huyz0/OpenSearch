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
import org.opensearch.serverless.storage.compaction.action.CompactionTriggerAction;
import org.opensearch.serverless.storage.compaction.action.CompactionTriggerRequest;
import org.opensearch.serverless.storage.compaction.action.CompactionTriggerResponse;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
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

/**
 * Proves {@link CompactionTriggerAction} genuinely works over the transport layer, in a real
 * cluster -- not just a direct unit-level call to {@code CompactionSchedulerTask.maybeCompact} --
 * covering the Guice injection of {@link ServerlessStoragePlugin} into {@code
 * TransportCompactionTriggerAction} and the {@code ThreadPool.Names#GENERIC} dispatch, both of
 * which only exist once the action actually runs inside a real node (rfc-serverless-opensearch.md
 * &sect;7.4, &sect;16 Phase 4.5).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageCompactionTriggerActionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_UUID = "compaction-trigger-it-idx";
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

    public void testCompactionTriggerActionMergesARealFragmentedShardOverTransport() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        BlobContainer container = blobContainerFor(basePath, INDEX_UUID, SHARD_ID);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        // Ten separate flushes from the same continuing writer -- ten real, separate Lucene
        // segments -- well past CompactionPolicy.withDefaults()'s minSegmentCountToCompact (10),
        // so this shard is a genuine compaction candidate, not a synthetic one.
        CommitManifest latestManifest = null;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    writerDirectory,
                    new IndexWriterConfig().setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE)
                )
            ) {
                for (int generation = 1; generation <= 10; generation++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", Integer.toString(generation), Field.Store.YES));
                    writer.addDocument(doc);
                    writer.commit();
                    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                    latestManifest = publisher.publishCommit(
                        writerDirectory,
                        segmentInfos,
                        INDEX_UUID,
                        SHARD_ID,
                        1,
                        generation,
                        generation - 1,
                        generation - 1,
                        new WalPosition("epoch-0", 0),
                        0,
                        PruningStats.empty()
                    );
                    if (generation == 1) {
                        assertEquals(
                            CasResult.SUCCESS,
                            shardStateStore.compareAndSet(
                                INDEX_UUID,
                                SHARD_ID,
                                Optional.empty(),
                                new ShardHead(1, null, 0L, latestManifest.generation())
                            )
                        );
                    } else {
                        long currentVersion = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().version();
                        assertEquals(
                            CasResult.SUCCESS,
                            shardStateStore.compareAndSet(
                                INDEX_UUID,
                                SHARD_ID,
                                Optional.of(currentVersion),
                                new ShardHead(1, null, 0L, latestManifest.generation())
                            )
                        );
                    }
                }
            }
        }
        assertEquals(10, org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics.from(latestManifest).segmentCount);

        CompactionTriggerResponse response = client().execute(
            CompactionTriggerAction.INSTANCE,
            new CompactionTriggerRequest(INDEX_UUID, SHARD_ID)
        ).get();
        assertTrue("a 10-segment shard must be recognized as a compaction candidate", response.attempted());

        VersionedShardHead afterCompaction = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertTrue(
            "compaction must have published a strictly newer generation",
            afterCompaction.head().latestManifestGeneration() > latestManifest.generation()
        );
        CommitManifest compactedManifest = manifestStore.readManifest(1, afterCompaction.head().latestManifestGeneration());
        assertEquals(
            "the ten fragmented segments must have been merged down to one",
            1,
            org.opensearch.serverless.storage.compaction.ManifestSegmentMetrics.from(compactedManifest).segmentCount
        );
    }

    public void testCompactionTriggerActionIsANoOpForAShardThatWasNeverActivated() throws Exception {
        Path basePath = createTempDir("serverless-storage-compaction-trigger-it-never-activated");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        CompactionTriggerResponse response = client().execute(
            CompactionTriggerAction.INSTANCE,
            new CompactionTriggerRequest("never-activated-idx", SHARD_ID)
        ).get();
        assertFalse("a shard with no published head must be a safe no-op, never an error", response.attempted());
    }
}
