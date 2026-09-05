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
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

/**
 * All of this engine's teardown used to live in {@code close()}, and {@code Engine#failEngine} calls
 * {@code closeNoLock} directly without ever going through it. So on any engine failure -- a corrupt
 * read, an IOException out of the reader manager -- the admission permit was never released (the
 * node's effective cap shrank toward zero until it could accept no reader shards at all), the
 * manifest poll kept ticking forever against a closed store, and the compaction and GC schedulers
 * kept merging and deleting in the object store on behalf of a dead shard.
 */
public class ObjectStoreReaderEngineFailureCleanupTests extends EngineTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;
    private static final String LOCAL_NODE_ID = "test-node";
    private static final long PRIMARY_TERM = 1;

    private BlobContainer blobContainer;
    private CommitManifest manifest;

    private void publishOneCommit() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                PRIMARY_TERM,
                segmentInfos.getGeneration(),
                0,
                0,
                null,
                0,
                PruningStats.empty()
            );
        }
    }

    private ObjectStoreReaderEngine openEngine(Store store, ReaderShardAdmissionController admissionController) throws Exception {
        EngineConfig engineConfig = config(defaultSettings, store, createTempDir(), newMergePolicy(), null);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ShardDirectory shardDirectory = new InMemoryShardDirectory();
        return ObjectStoreReaderEngine.open(
            engineConfig,
            manifest,
            new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer)),
            PRIMARY_TERM,
            shardStateStore,
            manifestStore,
            shardDirectory,
            LOCAL_NODE_ID,
            admissionController,
            null,
            null
        );
    }

    /**
     * The finding itself: a failed engine must release its admission permit. Before {@code
     * closeNoLock} was overridden, this permit was gone for the life of the node, and each failed
     * reader shard shrank the node's capacity by one until it could accept none.
     */
    public void testFailingTheEngineReleasesTheAdmissionPermit() throws Exception {
        publishOneCommit();
        ReaderShardAdmissionController admissionController = new ReaderShardAdmissionController(1);
        try (Store store = createStore()) {
            ObjectStoreReaderEngine engine = openEngine(store, admissionController);
            assertEquals(0, admissionController.availablePermits());

            engine.failEngine("simulated reader failure", new java.io.IOException("boom"));

            assertEquals(
                "failEngine bypasses close() entirely -- teardown has to happen on that path too",
                1,
                admissionController.availablePermits()
            );
        }
    }

    /**
     * The other direction of the same guard. {@code Engine#close()} is itself idempotent, but this
     * class's override ran its own body before delegating and unguarded -- so a second close called
     * {@code Semaphore#release()} again, and a {@code Semaphore} has no ownership check, silently
     * <em>raising</em> the node's admission cap above what it was configured with.
     */
    public void testClosingTwiceDoesNotInflateTheAdmissionCap() throws Exception {
        publishOneCommit();
        ReaderShardAdmissionController admissionController = new ReaderShardAdmissionController(1);
        try (Store store = createStore()) {
            ObjectStoreReaderEngine engine = openEngine(store, admissionController);
            engine.close();
            engine.close();
            assertEquals("a double close must never hand back a permit it did not take", 1, admissionController.availablePermits());
        }
    }

    /** And failing after closing (or vice versa) must still only ever release once. */
    public void testCloseThenFailStillReleasesExactlyOnce() throws Exception {
        publishOneCommit();
        ReaderShardAdmissionController admissionController = new ReaderShardAdmissionController(1);
        try (Store store = createStore()) {
            ObjectStoreReaderEngine engine = openEngine(store, admissionController);
            engine.close();
            engine.failEngine("after close", new java.io.IOException("boom"));
            assertEquals(1, admissionController.availablePermits());
        }
    }

    /**
     * The registry held a dead engine until the GC happened to clear its {@code WeakReference}, so
     * every lag/idle/query-rate lookup and every {@code pollNow} for the shard was answered by an
     * engine that had already failed -- worse than answering "not on this node".
     */
    public void testAFailedEngineDeregistersItselfFromTheActivityRegistry() throws Exception {
        publishOneCommit();
        ReaderShardActivityRegistry registry = new ReaderShardActivityRegistry();
        try (Store store = createStore()) {
            ObjectStoreReaderEngine engine = openEngine(store, null);
            registry.register(INDEX_UUID, SHARD_ID, engine);
            assertTrue(registry.manifestGenerationLag(INDEX_UUID, SHARD_ID).isPresent());

            engine.failEngine("simulated reader failure", new java.io.IOException("boom"));

            assertFalse(
                "a failed engine must remove itself rather than keep answering lookups for the shard",
                registry.manifestGenerationLag(INDEX_UUID, SHARD_ID).isPresent()
            );
        }
    }
}
