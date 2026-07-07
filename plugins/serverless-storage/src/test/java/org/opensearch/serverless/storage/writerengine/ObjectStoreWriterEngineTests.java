/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.Term;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.util.Optional;

/**
 * Exercises {@link ObjectStoreWriterEngine} through a real {@link EngineTestCase}-provisioned
 * engine: proof that flushing this engine actually publishes a manifest as the shard's head, and
 * that being fenced out by a higher term fails the engine -- not just that the class compiles
 * against server internals (which is all {@code ObjectStoreCommitHeadPublisherTests} alone proves).
 */
public class ObjectStoreWriterEngineTests extends EngineTestCase {

    private static final String INDEX_UUID = "idx";
    private static final String LOCAL_NODE_ID = "test-node";

    private Store lastOpenedStore;
    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();

    private ObjectStoreWriterEngine openWriterEngine(ShardStateStore shardStateStore, ObjectStoreCommitPublisher commitPublisher)
        throws Exception {
        Store store = createStore();
        lastOpenedStore = store;
        store.createEmpty(defaultSettings.getIndexVersionCreated().luceneVersion);
        java.nio.file.Path translogPath = createTempDir();
        String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        store.associateIndexWithNewTranslog(translogUuid);

        EngineConfig engineConfig = config(defaultSettings, store, translogPath, newMergePolicy(), null);
        ObjectStoreWriterEngine engine = new ObjectStoreWriterEngine(
            engineConfig,
            new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
            shardDirectory,
            LOCAL_NODE_ID
        );
        engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        return engine;
    }

    private void index(ObjectStoreWriterEngine engine, String id) throws Exception {
        ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
        Engine.Index index = new Engine.Index(
            new Term("_id", id),
            doc,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            Versions.MATCH_ANY,
            VersionType.INTERNAL,
            Engine.Operation.Origin.PRIMARY,
            System.nanoTime(),
            -1,
            false,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0
        );
        engine.index(index);
    }

    public void testFlushPublishesAManifestOntoTheShardHead() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            index(engine, "1");
            engine.flush(true, true);

            Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> head = shardStateStore.get(
                shardId.getIndex().getUUID(),
                shardId.getId()
            );
            assertTrue("a manifest should have been published as the shard head after flush", head.isPresent());
            assertEquals(primaryTerm.get(), head.get().head().primaryTerm());
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testOpeningTheEngineReportsToTheShardDirectory() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            org.opensearch.serverless.storage.directory.ShardDirectoryEntry entry = shardDirectory.lookup(
                shardId.getIndex().getUUID(),
                shardId.getId()
            ).orElseThrow(() -> new AssertionError("opening the engine should report an entry to the shard directory"));
            assertEquals(LOCAL_NODE_ID, entry.nodeId());
            assertEquals(org.opensearch.serverless.storage.directory.ShardRole.WRITER, entry.role());
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testFlushFailsTheEngineWhenAlreadyFencedOutByAHigherTerm() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        // Another node already activated the shard at a higher term before this engine flushes --
        // this writer has been fenced out even though it doesn't know it yet.
        shardStateStore.compareAndSet(
            shardId.getIndex().getUUID(),
            shardId.getId(),
            Optional.empty(),
            new ShardHead(primaryTerm.get() + 1, "other-node", 0L, 0L)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            index(engine, "1");
            expectThrows(Exception.class, () -> engine.flush(true, true));
            // A fenced-out writer's engine is failed, not just this one call: any further use
            // must also fail rather than silently continuing to accept writes.
            expectThrows(Exception.class, () -> index(engine, "2"));
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }
}
