/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

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
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.store.Store;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.util.Map;
import java.util.Optional;

/**
 * Exercises {@link ReaderShardActivityRegistry} through a real {@link EngineTestCase}-provisioned
 * reader engine -- both the "tracked" path (a registered, live engine) and the "not tracked" path,
 * the same shape {@code org.opensearch.serverless.storage.writerengine.ShardActivityRegistryTests}
 * already uses for the writer-tier registry.
 */
public class ReaderShardActivityRegistryTests extends EngineTestCase {

    private static final String LOCAL_NODE_ID = "test-node";
    private static final long PRIMARY_TERM = 1;

    public void testManifestGenerationLagIsEmptyForAnUnregisteredShard() {
        ReaderShardActivityRegistry registry = new ReaderShardActivityRegistry();
        assertTrue(
            "a shard nothing was ever registered for must read as not tracked",
            registry.manifestGenerationLag("never-registered-index", 0).isEmpty()
        );
    }

    public void testRegisteredEngineIsTrackedAndIncludedInSnapshotAll() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
        ShardDirectory shardDirectory = new InMemoryShardDirectory();

        Directory writerDirectory = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig());
        try (Store store = createStore()) {
            EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
            String indexUuid = engineConfig.getShardId().getIndex().getUUID();
            int shardId = engineConfig.getShardId().getId();

            Document doc = new Document();
            doc.add(new StringField("id", "1", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                indexUuid,
                shardId,
                PRIMARY_TERM,
                1,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(
                    indexUuid,
                    shardId,
                    Optional.empty(),
                    new ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
                )
            );

            try (
                ObjectStoreReaderEngine engine = ObjectStoreReaderEngine.open(
                    engineConfig,
                    manifest,
                    materializer,
                    PRIMARY_TERM,
                    shardStateStore,
                    manifestStore,
                    shardDirectory,
                    LOCAL_NODE_ID
                )
            ) {
                ReaderShardActivityRegistry registry = new ReaderShardActivityRegistry();
                registry.register(indexUuid, shardId, engine);

                Optional<Long> lag = registry.manifestGenerationLag(indexUuid, shardId);
                assertTrue("a registered, live engine must be tracked", lag.isPresent());
                assertEquals(engine.manifestGenerationLag(), (long) lag.get());

                Map<String, Long> snapshot = registry.snapshotAll();
                assertEquals(1, snapshot.size());
                assertTrue(snapshot.containsKey(indexUuid + "/" + shardId));
                assertEquals(engine.manifestGenerationLag(), (long) snapshot.get(indexUuid + "/" + shardId));
            }
        } finally {
            writer.close();
            writerDirectory.close();
        }
    }
}
