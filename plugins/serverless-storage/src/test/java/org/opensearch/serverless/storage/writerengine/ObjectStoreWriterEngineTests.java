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
import java.util.Set;

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
        return openWriterEngine(shardStateStore, commitPublisher, 0L);
    }

    private ObjectStoreWriterEngine openWriterEngine(
        ShardStateStore shardStateStore,
        ObjectStoreCommitPublisher commitPublisher,
        long publicationRateLimitMillis
    ) throws Exception {
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
            null,
            null,
            null,
            publicationRateLimitMillis
        );
        engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        return engine;
    }

    private void index(ObjectStoreWriterEngine engine, String id) throws Exception {
        ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
        Engine.Index index = new Engine.Index(
            new Term("_id", org.opensearch.index.mapper.Uid.encodeId(id)),
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

    public void testApiSourcedRefreshPublishesAManifest() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            // Engine construction itself already publishes one baseline manifest: InternalEngine's
            // own onAfterTranslogRecovery listener unconditionally calls flush(false, true) at the
            // end of recoverFromTranslog, independent of anything this test does -- so the
            // meaningful assertion is "generation advances," not "no manifest exists yet."
            long initialGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                .orElseThrow()
                .head()
                .latestManifestGeneration();

            index(engine, "1");
            engine.refresh("api");

            // The publish itself now runs asynchronously (dispatched off the calling thread so it
            // never blocks the shared REFRESH pool) -- refresh() only guarantees local search
            // visibility synchronously, so this must poll rather than read the head immediately.
            assertBusy(() -> {
                long afterRefreshGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                    .orElseThrow()
                    .head()
                    .latestManifestGeneration();
                assertTrue(
                    "an api-sourced refresh must publish a manifest -- rfc-serverless-opensearch.md section 8",
                    afterRefreshGeneration > initialGeneration
                );
            });
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testScheduleSourcedRefreshPublishesAManifest() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            long initialGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                .orElseThrow()
                .head()
                .latestManifestGeneration();

            index(engine, "1");
            engine.maybeRefresh("schedule");

            assertBusy(() -> {
                long afterRefreshGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                    .orElseThrow()
                    .head()
                    .latestManifestGeneration();
                assertTrue(
                    "a schedule-sourced refresh must publish a manifest too, same as api",
                    afterRefreshGeneration > initialGeneration
                );
            });
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testInternalSourcedRefreshDoesNotPublish() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            long initialGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                .orElseThrow()
                .head()
                .latestManifestGeneration();

            index(engine, "1");
            engine.refresh("post_recovery");

            long afterRefreshGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                .orElseThrow()
                .head()
                .latestManifestGeneration();
            assertEquals("an internal-lifecycle refresh source must not trigger a publish", initialGeneration, afterRefreshGeneration);
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testPublicationRateLimitSkipsARefreshPublishWithinTheWindow() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        // A rate limit far longer than this test can possibly take to run its two refreshes.
        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher, 5 * 60 * 1000L);
        try {
            long initialGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                .orElseThrow()
                .head()
                .latestManifestGeneration();

            index(engine, "1");
            engine.refresh("api");
            // Wait for the first (async) publish to actually land before triggering the second
            // refresh, so the rate-limit window genuinely starts from a completed first attempt.
            assertBusy(() -> {
                long generation = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                    .orElseThrow()
                    .head()
                    .latestManifestGeneration();
                assertTrue(generation > initialGeneration);
            });
            VersionedShardHead afterFirst = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).orElseThrow();

            index(engine, "2");
            engine.refresh("api");
            VersionedShardHead afterSecond = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).orElseThrow();

            assertEquals(
                "a second api-sourced refresh within the rate-limit window must not publish again",
                afterFirst.head().latestManifestGeneration(),
                afterSecond.head().latestManifestGeneration()
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testApiSourcedRefreshDoesNotBlockOnASlowPublish() throws Exception {
        // rfc-serverless-opensearch.md section 8 / code review finding: refresh()/maybeRefresh()
        // are dispatched by core on the small, node-wide-shared ThreadPool.Names.REFRESH pool, so
        // the actual object-store publish must never run inline there -- proven here with a real,
        // deliberately slow container standing in for high object-store latency, not just asserted.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawBlobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        long slowWriteMillis = 2000L;
        BlobContainer slowBlobContainer = new org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer(
            rawBlobContainer,
            new org.opensearch.serverless.storage.benchmark.LatencyProfile(0, 0, slowWriteMillis, slowWriteMillis, 0, 0)
        );
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(slowBlobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(slowBlobContainer),
            new BlobContainerManifestStore(slowBlobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            long initialGeneration = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                .orElseThrow()
                .head()
                .latestManifestGeneration();

            index(engine, "1");
            long start = System.nanoTime();
            engine.refresh("api");
            long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

            assertTrue(
                "refresh() must return well before the simulated " + slowWriteMillis + "ms publish completes, took " + elapsedMillis,
                elapsedMillis < slowWriteMillis
            );

            // The publish genuinely did happen -- just not before refresh() returned.
            assertBusy(() -> {
                long generation = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId())
                    .orElseThrow()
                    .head()
                    .latestManifestGeneration();
                assertTrue(generation > initialGeneration);
            });
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testRealtimeGetFindsAnIndexedDocumentBeforeAnyRefreshOrFlush() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            org.opensearch.serverless.storage.writerengine.RealtimeGetResult before = engine.realtimeGet("1");
            assertFalse("must not exist before it's indexed", before.exists());

            index(engine, "1");

            // Deliberately no refresh() or flush() call: a real-time get must find the document
            // straight off the live version map/translog, exactly the point of routing to the
            // writer instead of a reader engine that only sees materialized manifest generations.
            org.opensearch.serverless.storage.writerengine.RealtimeGetResult after = engine.realtimeGet("1");
            assertTrue("must exist immediately after indexing, with no refresh/flush", after.exists());
            assertTrue("version must be a real assigned version, not a placeholder", after.version() > 0);
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testFlushAndPublishQuiescentMarksTheManifestQuiescent() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            manifestStore
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            index(engine, "1");
            engine.flush(true, true);

            org.opensearch.serverless.storage.shardstate.VersionedShardHead beforeQuiesce = shardStateStore.get(
                shardId.getIndex().getUUID(),
                shardId.getId()
            ).orElseThrow();
            org.opensearch.serverless.storage.manifest.CommitManifest ordinaryManifest = manifestStore.readManifest(
                beforeQuiesce.head().primaryTerm(),
                beforeQuiesce.head().latestManifestGeneration()
            );
            assertFalse("an ordinary flush must not mark its manifest quiescent", ordinaryManifest.quiescent());

            engine.flushAndPublishQuiescent();

            org.opensearch.serverless.storage.shardstate.VersionedShardHead afterQuiesce = shardStateStore.get(
                shardId.getIndex().getUUID(),
                shardId.getId()
            ).orElseThrow();
            assertTrue(
                "flushAndPublishQuiescent must publish a strictly newer generation",
                afterQuiesce.head().latestManifestGeneration() > beforeQuiesce.head().latestManifestGeneration()
            );
            org.opensearch.serverless.storage.manifest.CommitManifest quiescentManifest = manifestStore.readManifest(
                afterQuiesce.head().primaryTerm(),
                afterQuiesce.head().latestManifestGeneration()
            );
            assertTrue("flushAndPublishQuiescent's own manifest must be marked quiescent", quiescentManifest.quiescent());
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testClosingTheEnginePublishesAFinalQuiescentManifest() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            manifestStore
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        index(engine, "1");
        engine.flush(true, true);
        try {
            org.opensearch.serverless.storage.shardstate.VersionedShardHead beforeClose = shardStateStore.get(
                shardId.getIndex().getUUID(),
                shardId.getId()
            ).orElseThrow();

            // Index one more document without an explicit flush -- close() itself must force the
            // final commit rather than relying on whatever was last published on the normal schedule.
            index(engine, "2");

            IOUtils.close(engine);

            org.opensearch.serverless.storage.shardstate.VersionedShardHead afterClose = shardStateStore.get(
                shardId.getIndex().getUUID(),
                shardId.getId()
            ).orElseThrow();
            assertTrue(
                "close() must force a strictly newer final commit, not just reuse the last flush",
                afterClose.head().latestManifestGeneration() > beforeClose.head().latestManifestGeneration()
            );
            org.opensearch.serverless.storage.manifest.CommitManifest finalManifest = manifestStore.readManifest(
                afterClose.head().primaryTerm(),
                afterClose.head().latestManifestGeneration()
            );
            assertTrue("close()'s own final manifest must be marked quiescent", finalManifest.quiescent());
        } finally {
            IOUtils.close(lastOpenedStore);
        }
    }

    public void testMillisSinceLastActivityUpdatesOnIndexAndDecreasesUntilTheNextOne() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            long freshlyOpened = engine.millisSinceLastActivity();
            assertTrue("a freshly opened engine should read as just active, not idle since the epoch", freshlyOpened < 10_000);

            // A deliberately long sleep, not a tight one: this needs enough separation from the
            // post-index bound below to stay robust even if index() itself (real, variable-duration
            // work -- parsing, Lucene indexing, translog append) takes a while, which would make a
            // *relative* "afterIndex < beforeIndex" comparison flaky by construction (index() could
            // legitimately outlast a short sleep). 2s leaves generous headroom on both sides.
            Thread.sleep(2_000);
            long beforeIndex = engine.millisSinceLastActivity();
            assertTrue("idle time must have advanced by roughly the sleep duration while nothing wrote", beforeIndex >= 1_800);

            index(engine, "1");
            long afterIndex = engine.millisSinceLastActivity();
            assertTrue(
                "indexing a document must reset the idle clock back down to just now, not leave it accumulating "
                    + "from before the write (beforeIndex was "
                    + beforeIndex
                    + "ms, afterIndex was "
                    + afterIndex
                    + "ms)",
                afterIndex < 1_500
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testMillisSinceLastActivityIgnoresTranslogRecoveryReplayOperations() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            Thread.sleep(2_000);
            long beforeReplay = engine.millisSinceLastActivity();
            assertTrue("idle time must have advanced by roughly the sleep duration", beforeReplay >= 1_800);

            // Simulates what a real crash/restart's translogManager().recoverFromTranslog(...) call
            // does internally -- re-applying an already-sequenced operation via LOCAL_TRANSLOG_RECOVERY,
            // not a fresh client write. This must NOT reset the idle clock: a shard that just
            // recovered from a crash with no new client activity since well before the crash is
            // still idle, not "just active."
            ParsedDocument doc = testParsedDocument("recovery-replay", null, testDocumentWithTextField(), SOURCE, null);
            Engine.Index replayedIndex = new Engine.Index(
                new Term("_id", "recovery-replay"),
                doc,
                0,
                primaryTerm.get(),
                1,
                null,
                Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
                System.nanoTime(),
                -1,
                false,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            );
            engine.index(replayedIndex);

            long afterReplay = engine.millisSinceLastActivity();
            assertTrue(
                "a recovery-replay operation must not reset the idle clock (beforeReplay was "
                    + beforeReplay
                    + "ms, afterReplay was "
                    + afterReplay
                    + "ms)",
                afterReplay >= 1_800
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }

    public void testWritesPerMinuteReportsZeroWhileStillInsideTheFirstWindow() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        ObjectStoreWriterEngine engine = openWriterEngine(shardStateStore, commitPublisher);
        try {
            assertEquals("a freshly opened engine must report zero writes per minute", 0L, engine.writesPerMinute());

            index(engine, "1");
            index(engine, "2");

            // writesPerMinute() deliberately reports the previous *completed* window's count, never
            // the in-progress one -- see the field's own javadoc. So an engine that has only ever
            // had writes land inside its very first (still-open) window reports 0, not the count of
            // writes it has actually seen so far.
            assertEquals(
                "an engine still inside its first write-rate window must report 0, not the in-progress count",
                0L,
                engine.writesPerMinute()
            );
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

    public void testActivatingAWriterEngineRegistersItsShardInTheWalShardRegistry() throws Exception {
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

        // Not registered before any writer engine for this shard has ever activated against this container.
        org.opensearch.serverless.storage.wal.WalShardRegistry registry = new org.opensearch.serverless.storage.wal.WalShardRegistry(
            walBlobContainer
        );
        assertTrue(registry.registeredShards().isEmpty());

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
            // Registered on activation alone -- no indexing/flush needed.
            assertEquals(
                Set.of(new org.opensearch.serverless.storage.wal.RegisteredShard(shardId.getIndex().getUUID(), shardId.getId())),
                registry.registeredShards()
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

    public void testOnPrimaryTermBumpedReSnapshotsActivationWalPositionToTheLiveBound() throws Exception {
        // The B8/B9 core hook this test proves: for an already-constructed engine (a writer
        // replica later promoted to primary, as opposed to a shard freshly recovering from
        // scratch), the constructor-time snapshot alone is stale -- see Engine#onPrimaryTermBumped's
        // own javadoc for exactly why. This engine is constructed with WAL mirroring on but with
        // no chunks written yet, then chunks are appended (simulating this engine, as a live
        // writer replica, replaying ops shipped from the then-primary) strictly after
        // construction and strictly before the term bump this test fires -- proving the
        // re-snapshot picks up activity the constructor could not have seen.
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
            long snapshotAtConstruction = engine.activationWalPositionForTesting();
            assertEquals("nothing had been appended before construction", 0L, snapshotAtConstruction);

            // Simulates real activity landing in this engine's WAL stream after it was constructed
            // as a replica but before its own promotion to primary -- the exact gap a
            // constructor-only snapshot cannot see.
            walChunkService.append(
                new org.opensearch.serverless.storage.wal.WalRecord(INDEX_UUID, 0, 1, 0, "post-construction".getBytes("UTF-8"))
            );
            walChunkService.flush();
            long liveBoundBeforeBump = walChunkService.currentChunkSequenceUpperBound();
            assertTrue("test setup should have produced a later chunk", liveBoundBeforeBump > snapshotAtConstruction);

            engine.onPrimaryTermBumped(primaryTerm.get() + 1);

            assertEquals(
                "the hook must re-snapshot to the live bound at the moment of the term bump, not leave the stale constructor-time value",
                liveBoundBeforeBump,
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

    /**
     * Proves the wiring from an {@code EncryptionKeyProvider} through {@code ObjectStoreWriterEngine}
     * to a real, mirrored WAL chunk -- not just {@code EncryptingWalChunkService}/{@code
     * WalRecordCrypto} in isolation, which {@code EncryptingWalChunkServiceTests}/{@code
     * WalRecordCryptoTests} already cover.
     */
    public void testWalMirroredRecordsAreEncryptedAtRestWhenAnEncryptionKeyProviderIsConfigured() throws Exception {
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
            "node-epoch-encrypted"
        );
        org.opensearch.serverless.storage.security.EncryptionKeyProvider keyProvider =
            org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider.fromRawKeyBytes(new byte[32]);

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
            walChunkService,
            keyProvider
        );
        try {
            engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
            index(engine, "1");
            engine.flush(true, true);

            // Every log-* blob's records, not just the first found -- per-operation flushing can
            // legitimately produce more than one chunk, and blob listing order isn't guaranteed to
            // match chunk sequence order.
            java.util.List<org.opensearch.serverless.storage.wal.WalRecord> rawRecords = new java.util.ArrayList<>();
            java.util.Map<String, ?> chunkBlobs = walBlobContainer.listBlobsByPrefix(
                org.opensearch.serverless.storage.wal.WalChunkNaming.LOG_BLOB_PREFIX
            );
            for (String chunkBlobName : chunkBlobs.keySet()) {
                byte[] chunkBytes;
                try (java.io.InputStream in = walBlobContainer.readBlob(chunkBlobName)) {
                    chunkBytes = in.readAllBytes();
                }
                rawRecords.addAll(org.opensearch.serverless.storage.wal.WalChunkReader.readRecords(chunkBytes));
            }
            assertEquals(1, rawRecords.size());

            // The raw, still-encrypted payload must not deserialize as a valid Translog.Operation --
            // proof this is genuinely ciphertext on disk, not plaintext that merely wasn't asserted on.
            expectThrows(
                Exception.class,
                () -> Translog.Operation.readOperation(org.opensearch.core.common.io.stream.StreamInput.wrap(rawRecords.get(0).payload()))
            );

            java.util.List<org.opensearch.serverless.storage.wal.WalRecord> decrypted =
                org.opensearch.serverless.storage.wal.WalRecordCrypto.decryptAll(rawRecords, keyProvider);
            Translog.Operation decryptedOp = Translog.Operation.readOperation(
                org.opensearch.core.common.io.stream.StreamInput.wrap(decrypted.get(0).payload())
            );
            assertEquals(0L, decryptedOp.seqNo());
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }
}
