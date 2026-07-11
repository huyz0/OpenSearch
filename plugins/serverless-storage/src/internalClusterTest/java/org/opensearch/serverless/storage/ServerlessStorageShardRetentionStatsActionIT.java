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
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.retention.PitrRetentionPolicy;
import org.opensearch.serverless.storage.retention.action.ShardRetentionStatsAction;
import org.opensearch.serverless.storage.retention.action.ShardRetentionStatsRequest;
import org.opensearch.serverless.storage.retention.action.ShardRetentionStatsResponse;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

/**
 * Proves {@link ShardRetentionStatsAction} genuinely reflects a real shard's manifest/bundle/pin
 * state over the transport layer, in a real cluster -- covering the Guice injection into {@code
 * TransportShardRetentionStatsAction} and the dry-run policy computation staying in lockstep with
 * the real {@code GcSchedulerTask} sweep it mirrors, not just a direct unit-level call.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageShardRetentionStatsActionIT extends OpenSearchIntegTestCase {

    private static final String INDEX_UUID = "retention-stats-it-idx";
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

    public void testRetentionStatsReflectsRealManifestsAndAPitrPinProtectingAnOlderGeneration() throws Exception {
        Path basePath = createTempDir("serverless-storage-retention-stats-it");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING.getKey(), org.opensearch.common.unit.TimeValue.ZERO)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        BlobContainer container = blobContainerFor(basePath, INDEX_UUID, SHARD_ID);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        CommitManifest firstManifest;
        CommitManifest secondManifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc1 = new Document();
                doc1.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc1);
                writer.commit();
                SegmentInfos firstSegmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                firstManifest = publisher.publishCommit(
                    writerDirectory,
                    firstSegmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                );

                Document doc2 = new Document();
                doc2.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();
                SegmentInfos secondSegmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                secondManifest = publisher.publishCommit(
                    writerDirectory,
                    secondSegmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    2,
                    1,
                    1,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                );
            }
        }

        // Zero retention window (the node setting above): the older, non-latest generation is
        // immediately eligible for deletion once unpinned.
        ShardRetentionStatsResponse beforePin = client().execute(
            ShardRetentionStatsAction.INSTANCE,
            new ShardRetentionStatsRequest(INDEX_UUID, SHARD_ID)
        ).get();
        assertEquals("both published generations must be counted", 2, beforePin.manifestCount());
        assertEquals(
            "the older, non-latest generation must be deletable with no pin and a zero retention window",
            1,
            beforePin.deletableManifestCount()
        );
        assertEquals("no pins have been added yet", 0, beforePin.durablePinCount());
        assertEquals(0, beforePin.pitrPinCount());
        assertEquals(0L, beforePin.gcRetentionWindowMillis());

        // Directly seed a PITR pin on the older generation, the same way PitrRetentionReconciler
        // would once its own (much longer, 5-minute) schedule got around to it -- this test isn't
        // waiting that out, it's proving the stats action correctly reflects whatever pin state
        // already exists in the real, shared BlobContainerDurablePinRegistry.
        BlobContainerDurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(container);
        pinRegistry.addPin(
            INDEX_UUID,
            SHARD_ID,
            new PinRecord(PitrRetentionPolicy.PITR_PIN_ID, firstManifest.primaryTerm(), firstManifest.generation())
        );

        ShardRetentionStatsResponse afterPin = client().execute(
            ShardRetentionStatsAction.INSTANCE,
            new ShardRetentionStatsRequest(INDEX_UUID, SHARD_ID)
        ).get();
        assertEquals("still both generations", 2, afterPin.manifestCount());
        assertEquals(
            "a durably pinned generation must never be reported as deletable, mirroring "
                + "ManifestRetentionPolicy's own rule that GcSchedulerTask's real sweep enforces",
            0,
            afterPin.deletableManifestCount()
        );
        assertEquals(1, afterPin.durablePinCount());
        assertEquals(1, afterPin.pitrPinCount());

        assertNotNull(secondManifest);
    }
}
