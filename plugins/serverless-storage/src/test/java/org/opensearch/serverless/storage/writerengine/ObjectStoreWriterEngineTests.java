/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.UUIDs;
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
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PitrRetentionConfig;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

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

    public void testOpeningAndClosingWithPitrRetentionConfiguredDoesNotThrow() throws Exception {
        // PitrRetentionSchedulerTask's own scheduling/reconciliation behavior is verified directly
        // and exhaustively in PitrRetentionSchedulerTaskTests (real background ticks, real pin
        // add/remove, cancellation on close) against a configurable short interval -- this engine
        // hardcodes a 5-minute interval, far too long to observe a real tick in a test. What this
        // test proves instead is the wiring itself: constructing and closing the engine with a
        // PitrRetentionConfig must not throw, i.e. the plumbing from engine constructor through to
        // PitrRetentionSchedulerTask's own constructor (and back through close()) is correct.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            manifestStore
        );
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(blobContainer);
        PitrRetentionConfig pitrRetentionConfig = new PitrRetentionConfig(manifestStore, pinRegistry, 60_000L);

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
            LOCAL_NODE_ID,
            pitrRetentionConfig
        );
        engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        IOUtils.close(engine, lastOpenedStore);
    }

    public void testForceMergePublishesAGenuinelyMergedManifestPreservingAllDocuments() throws Exception {
        // _forcemerge reaches an engine only while a writer is actively open (IndexShard.forceMerge
        // requires a live shard) -- exactly the case where redirecting it to a *separate*,
        // object-store-materialized compaction path (independent of this engine's own local Lucene
        // generation counter) would let the two diverge and silently drop this writer's next real
        // commit. So this doesn't override forceMerge at all: it inherits InternalEngine's real
        // local-Lucene merge, and relies on the already-overridden commitIndexWriter (the same path
        // an ordinary flush publishes through) to publish the merged result correctly. This test is
        // the proof that inheritance alone is actually correct here, not an assumption.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            // Index and flush separately so multiple real Lucene segments exist before merging.
            index(engine, "1");
            engine.flush(true, true);
            index(engine, "2");
            engine.flush(true, true);
            index(engine, "3");
            engine.flush(true, true);

            VersionedShardHead beforeMerge = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).orElseThrow();

            engine.forceMerge(true, 1, false, false, false, UUIDs.randomBase64UUID());

            VersionedShardHead afterMerge = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).orElseThrow();
            assertTrue(
                "forceMerge must publish a newer generation, not silently no-op",
                afterMerge.head().latestManifestGeneration() > beforeMerge.head().latestManifestGeneration()
            );

            CommitManifest mergedManifest = manifestStore.readManifest(
                afterMerge.head().primaryTerm(),
                afterMerge.head().latestManifestGeneration()
            );
            try (Directory materialized = new ByteBuffersDirectory()) {
                new ObjectStoreCommitMaterializer(bundleStore).materialize(mergedManifest, materialized);
                try (DirectoryReader reader = DirectoryReader.open(materialized)) {
                    assertEquals("forceMerge(maxNumSegments=1) must genuinely merge down to one segment", 1, reader.leaves().size());
                    assertEquals("no document may be lost or duplicated by the merge", 3, reader.numDocs());
                }
            }
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testConstructionFailsWhenAlreadyFencedOutByAHigherTerm() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        // Another node already activated the shard at a higher term before this engine even
        // constructs -- lease acquisition (done synchronously during construction, see
        // ObjectStoreWriterEngine's own javadoc) now catches this immediately, rather than letting a
        // doomed writer accept writes it could never publish until its first flush discovers the
        // fencing.
        shardStateStore.compareAndSet(
            shardId.getIndex().getUUID(),
            shardId.getId(),
            Optional.empty(),
            new ShardHead(primaryTerm.get() + 1, "other-node", 0L, 0L)
        );

        try {
            expectThrows(Exception.class, () -> openWriterEngine(shardStateStore, commitPublisher));
        } finally {
            IOUtils.close(lastOpenedStore);
        }
    }

    public void testTranslogDeletionPolicyIsWiredBeforeTheConstructorReturns() throws Exception {
        // ObjectStoreWriterEngine#getTranslogDeletionPolicy is called by InternalEngine's own
        // constructor from inside super(engineConfig) -- i.e. before this class's own field
        // initializers would normally run. This is the proof that the deliberately-no-initializer
        // field pattern documented on that field actually survives construction, not just that it
        // compiles (see rfc-serverless-opensearch.md &sect;7.1.1).
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            assertNotNull(
                "the durability-driven translog deletion policy must be set during construction, not left null",
                engine.translogDeletionPolicyForTesting()
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testDurablePublicationAdvancesTheTranslogRetentionWatermarkAndTrimsOldGenerations() throws Exception {
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
            assertEquals(
                "the watermark must advance to the seq-no actually covered by the first durable publish",
                0L,
                engine.translogDeletionPolicyForTesting().durablyPublishedMaxSeqNo()
            );

            index(engine, "2");
            index(engine, "3");
            engine.flush(true, true);
            assertEquals(
                "the watermark must advance again after the second durable publish",
                2L,
                engine.translogDeletionPolicyForTesting().durablyPublishedMaxSeqNo()
            );

            // Translog#trimUnreferencedReaders combines this policy's own floor with a second
            // floor derived from CombinedDeletionPolicy's safe-commit tracking
            // (TranslogDeletionPolicy#getLocalCheckpointOfSafeCommit), which -- even with
            // &sect;7.1.1 Part 2's widened globalCheckpointSupplierForCombinedDeletionPolicy --
            // is necessarily always exactly one flush cycle behind: CombinedDeletionPolicy#onCommit
            // fires synchronously as part of the Lucene commit created by super.commitIndexWriter,
            // which happens *before* this flush's own recordDurablePublication call (durability can
            // only be confirmed after the commit whose content it covers already exists locally).
            // So the achievable floor is "proportional to one publish cycle," exactly the shape
            // &sect;7.1.1's milestone states -- not literally always the single newest generation.
            Translog translog = org.opensearch.index.engine.EngineTestCase.getTranslog(engine);
            assertEquals(
                "retention should lag the current generation by exactly one flush cycle, not more",
                translog.currentFileGeneration() - 1,
                translog.getMinFileGeneration()
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testDurablePublicationLetsOldLocalLuceneCommitsBeDeletedAheadOfTheGlobalCheckpoint() throws Exception {
        // §7.1.1 Part 2: globalCheckpointSupplierForCombinedDeletionPolicy widens the safe-commit
        // threshold CombinedDeletionPolicy uses with the same durability watermark the translog
        // policy tracks -- so old local commits should be deletable even though nothing in this
        // bare EngineTestCase-provisioned engine ever advances the *real* global checkpoint (there
        // is no replica, and nothing here calls updateGlobalCheckpointOnReplica/markSeqNoAsPersisted
        // paths a full IndexShard would). If retention still depended solely on the real global
        // checkpoint, every one of these commits would remain forever.
        //
        // The achievable floor is 2, not 1: CombinedDeletionPolicy#onCommit fires synchronously as
        // part of the very commit it's evaluating, before this flush's own recordDurablePublication
        // call can mark that commit's content durable -- so the "safe" commit it settles on for
        // this flush is always the *previous* flush's (by then durable), and the newest commit is
        // always kept regardless (IndexWriter needs an open commit point to keep writing). Both are
        // retained; nothing older than that survives, matching &sect;7.1.1's "one publish cycle" bound.
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
            index(engine, "2");
            engine.flush(true, true);
            index(engine, "3");
            engine.flush(true, true);

            int commitCount = DirectoryReader.listCommits(lastOpenedStore.directory()).size();
            assertEquals(
                "with every generation durably published, only the safe commit and the newest commit should remain",
                2,
                commitCount
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testWalMirroringPublishesARealWalPositionInsteadOfThePlaceholder() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        FsBlobStore walBlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer walBlobContainer = new FsBlobContainer(walBlobStore, BlobPath.cleanPath(), walBlobStore.path());
        org.opensearch.serverless.storage.wal.WalChunkService walChunkService = new org.opensearch.serverless.storage.wal.WalChunkService(
            walBlobContainer,
            "node-epoch-0"
        );

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
            LOCAL_NODE_ID,
            null,
            walChunkService
        );
        try {
            engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            assertNotNull(
                "WAL mirroring must be wired in when a WalChunkService is configured -- the ThreadLocal bridge across super() must have worked",
                engine.walMirroringTranslogForTesting()
            );

            index(engine, "1");
            engine.flush(true, true);

            ShardHead head = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).orElseThrow().head();
            CommitManifest manifest = new BlobContainerManifestStore(blobContainer).readManifest(
                head.primaryTerm(),
                head.latestManifestGeneration()
            );
            assertEquals("node-epoch-0", manifest.walPosition().writerEpoch());
            assertTrue(
                "the published WalPosition must reflect a real flushed chunk sequence, not the old (String.valueOf(primaryTerm), 0) placeholder",
                manifest.walPosition().offset() >= 0
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testActivationWalPositionCapturesOnlyWhatExistedBeforeThisEngineActivated() throws Exception {
        // The fencing snapshot verified sound in formal/WalReplayFencing.tla: chunks written
        // before this engine activates must be captured; chunks written after must not be --
        // proving the boundary, not just that some value gets set.
        FsBlobStore walBlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer walBlobContainer = new FsBlobContainer(walBlobStore, BlobPath.cleanPath(), walBlobStore.path());
        org.opensearch.serverless.storage.wal.WalChunkService walChunkService = new org.opensearch.serverless.storage.wal.WalChunkService(
            walBlobContainer,
            "shared-epoch"
        );

        // Simulate a prior (possibly now-stale) writer having already appended chunks under the
        // same shared epoch, before this engine ever activates.
        walChunkService.append(
            new org.opensearch.serverless.storage.wal.WalRecord("other-idx", 0, 1, 0, "pre-activation".getBytes("UTF-8"))
        );
        walChunkService.flush();
        long chunksBeforeActivation = walChunkService.currentChunkSequenceUpperBound();
        assertTrue("test setup should have produced at least one prior chunk", chunksBeforeActivation > 0);

        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

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
            LOCAL_NODE_ID,
            null,
            walChunkService
        );
        try {
            assertEquals(
                "the snapshot taken at activation must equal exactly what existed just before it, no more and no less",
                chunksBeforeActivation,
                engine.activationWalPositionForTesting()
            );

            // Chunks written after activation must not retroactively change the snapshot already taken.
            engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            index(engine, "1");
            engine.flush(true, true);
            assertTrue(
                "activity after activation must have written further chunks (the WAL watermark test elsewhere already covers this directly)",
                walChunkService.currentChunkSequenceUpperBound() > chunksBeforeActivation
            );
            assertEquals(
                "the snapshot must stay fixed at its activation-time value regardless of subsequent WAL activity",
                chunksBeforeActivation,
                engine.activationWalPositionForTesting()
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testActivationWalPositionIsMinusOneWhenWalMirroringIsDisabled() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            assertEquals(-1L, engine.activationWalPositionForTesting());
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testReplayWalOperationsReturnsOperationsSinceTheLastManifestBoundedByActivation() throws Exception {
        // End-to-end proof of WalReplayFencing.tla's verified FixedReplay design against a real
        // failover: a first writer (term N) publishes a manifest, more operations land durably in
        // the WAL afterward, and a second writer (term N+1) activating later must replay exactly
        // those -- no more, no less.
        FsBlobStore walBlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer walBlobContainer = new FsBlobContainer(walBlobStore, BlobPath.cleanPath(), walBlobStore.path());
        org.opensearch.serverless.storage.wal.WalChunkService walChunkService = new org.opensearch.serverless.storage.wal.WalChunkService(
            walBlobContainer,
            "shared-epoch"
        );

        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        Store store1 = createStore();
        store1.createEmpty(defaultSettings.getIndexVersionCreated().luceneVersion);
        java.nio.file.Path translogPath1 = createTempDir();
        String translogUuid1 = Translog.createEmptyTranslog(translogPath1, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        store1.associateIndexWithNewTranslog(translogUuid1);
        EngineConfig engineConfig1 = config(defaultSettings, store1, translogPath1, newMergePolicy(), null);
        ObjectStoreWriterEngine engine1 = new ObjectStoreWriterEngine(
            engineConfig1,
            new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
            shardDirectory,
            LOCAL_NODE_ID,
            null,
            walChunkService
        );
        engine1.translogManager().recoverFromTranslog(translogHandler, engine1.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        index(engine1, "1");
        engine1.flush(true, true);
        IOUtils.close(engine1, store1);

        // Durable in the WAL, but never covered by any published manifest -- exactly what replay
        // must catch up on for the next writer.
        Translog.Index extraOp = new Translog.Index("2", 1, primaryTerm.get(), "extra".getBytes("UTF-8"));
        org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput();
        Translog.Operation.writeOperation(out, extraOp);
        walChunkService.append(
            new org.opensearch.serverless.storage.wal.WalRecord(
                shardId.getIndex().getUUID(),
                shardId.getId(),
                primaryTerm.get(),
                1,
                org.opensearch.core.common.bytes.BytesReference.toBytes(out.bytes())
            )
        );
        walChunkService.flush();

        primaryTerm.set(primaryTerm.get() + 1);
        Store store2 = createStore();
        store2.createEmpty(defaultSettings.getIndexVersionCreated().luceneVersion);
        java.nio.file.Path translogPath2 = createTempDir();
        String translogUuid2 = Translog.createEmptyTranslog(translogPath2, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        store2.associateIndexWithNewTranslog(translogUuid2);
        EngineConfig engineConfig2 = config(defaultSettings, store2, translogPath2, newMergePolicy(), null);
        ObjectStoreWriterEngine engine2 = new ObjectStoreWriterEngine(
            engineConfig2,
            new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
            shardDirectory,
            LOCAL_NODE_ID,
            null,
            walChunkService
        );
        try {
            java.util.List<Translog.Operation> replayed = engine2.replayWalOperations();
            assertEquals(1, replayed.size());
            assertEquals(1L, replayed.get(0).seqNo());
        } finally {
            IOUtils.close(engine2, store2);
        }
    }

    public void testReplayWalOperationsIsEmptyWhenWalMirroringIsDisabled() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            assertTrue(engine.replayWalOperations().isEmpty());
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }
}
