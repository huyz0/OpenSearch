/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalAppendTarget;
import org.opensearch.serverless.storage.wal.WalBatchingProcessor;
import org.opensearch.serverless.storage.wal.WalChunkNaming;
import org.opensearch.serverless.storage.wal.WalChunkReader;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class WalMirroringTranslogTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "wal-mirror-idx";

    private ShardId shardId;
    private IndexSettings indexSettings;
    private BlobContainer blobContainer;
    private WalChunkService walChunkService;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Index index = new Index("wal-mirror-index", INDEX_UUID);
        shardId = new ShardId(index, 0);
        indexSettings = IndexSettingsModule.newIndexSettings(index, org.opensearch.common.settings.Settings.EMPTY);

        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        walChunkService = new WalChunkService(blobContainer, "epoch-0");
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private WalMirroringTranslog newTranslog(Path translogPath) throws Exception {
        return newTranslog(translogPath, walChunkService);
    }

    private WalMirroringTranslog newTranslog(Path translogPath, WalAppendTarget appendTarget) throws Exception {
        TranslogConfig config = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(translogPath, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
        return new WalMirroringTranslog(
            config,
            translogUUID,
            new DefaultTranslogDeletionPolicy(-1, -1, Integer.MAX_VALUE),
            () -> SequenceNumbers.UNASSIGNED_SEQ_NO,
            () -> 1L,
            seqNo -> {},
            TranslogOperationHelper.DEFAULT,
            appendTarget
        );
    }

    /** A batching processor with a short interval, attached to {@code service} so a translog built over it takes the batching path. */
    private WalBatchingProcessor attachProcessor(WalChunkService service) {
        WalBatchingProcessor processor = new WalBatchingProcessor(
            org.apache.logging.log4j.LogManager.getLogger(WalMirroringTranslogTests.class),
            10000,
            threadPool.getThreadContext(),
            threadPool,
            () -> org.opensearch.common.unit.TimeValue.timeValueMillis(50),
            service,
            null
        );
        service.attachBatchingProcessor(processor);
        return processor;
    }

    private int logChunkCount() throws java.io.IOException {
        return blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).size();
    }

    public void testAppendedOperationsAreMirroredIntoAWalChunk() throws Exception {
        Path path = createTempDir();
        try (WalMirroringTranslog translog = newTranslog(path)) {
            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
        }

        List<WalRecord> allRecords = new java.util.ArrayList<>();
        for (long chunkSeq = 0; chunkSeq < 2; chunkSeq++) {
            String blobName = WalChunkNaming.blobName("epoch-0", chunkSeq);
            byte[] chunkBytes;
            try (java.io.InputStream in = blobContainer.readBlob(blobName)) {
                chunkBytes = in.readAllBytes();
            }
            allRecords.addAll(WalChunkReader.readRecords(chunkBytes));
        }

        assertEquals(2, allRecords.size());
        assertEquals(INDEX_UUID, allRecords.get(0).indexUuid());
        assertEquals(0, allRecords.get(0).shardId());
        assertEquals(1L, allRecords.get(0).primaryTerm());
        assertEquals(0L, allRecords.get(0).seqNo());
        assertEquals(1L, allRecords.get(1).primaryTerm());
        assertEquals(1L, allRecords.get(1).seqNo());
    }

    public void testLastFlushedWalChunkSequenceAdvancesWithEachMirroredOperation() throws Exception {
        Path path = createTempDir();
        try (WalMirroringTranslog translog = newTranslog(path)) {
            assertEquals("nothing mirrored yet", -1L, translog.lastFlushedWalChunkSequence());

            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            long afterFirst = translog.lastFlushedWalChunkSequence();
            assertTrue("the first mirrored operation must advance the watermark past -1", afterFirst >= 0);

            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
            long afterSecond = translog.lastFlushedWalChunkSequence();
            assertTrue(
                "each op flushes to its own chunk today (flush-per-op), so the sequence must strictly advance",
                afterSecond > afterFirst
            );
        }
    }

    public void testLocalRecoveryStillWorksExactlyAsAPlainLocalTranslog() throws Exception {
        Path path = createTempDir();
        try (WalMirroringTranslog translog = newTranslog(path)) {
            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));

            java.util.Set<Long> seqNosSeen = new java.util.HashSet<>();
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    seqNosSeen.add(op.seqNo());
                }
            }
            assertEquals(java.util.Set.of(0L, 1L), seqNosSeen);
        }
    }

    public void testAddSurvivesTransientWalMirrorFailuresWithinTheRetryBudget() throws Exception {
        FailNTimesBlobContainer faulty = new FailNTimesBlobContainer(blobContainer, WalMirroringTranslog.MAX_MIRROR_FLUSH_ATTEMPTS - 1);
        WalChunkService flakyWalChunkService = new WalChunkService(faulty, "epoch-0");

        Path path = createTempDir();
        TranslogConfig config = new TranslogConfig(shardId, path, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
        try (
            WalMirroringTranslog translog = new WalMirroringTranslog(
                config,
                translogUUID,
                new DefaultTranslogDeletionPolicy(-1, -1, Integer.MAX_VALUE),
                () -> SequenceNumbers.UNASSIGNED_SEQ_NO,
                () -> 1L,
                seqNo -> {},
                TranslogOperationHelper.DEFAULT,
                flakyWalChunkService
            )
        ) {
            // Must not throw: the first (MAX_MIRROR_FLUSH_ATTEMPTS - 1) flush attempts fail, but
            // the last one within the retry budget succeeds.
            translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
        }

        // The successful attempt lands at whatever sequence number it reached (earlier attempts
        // that failed before writing still consume a sequence number), so scan by prefix rather
        // than assuming it's log-0.
        List<WalRecord> allRecords = new java.util.ArrayList<>();
        for (String blobName : blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet()) {
            byte[] chunkBytes;
            try (java.io.InputStream in = blobContainer.readBlob(blobName)) {
                chunkBytes = in.readAllBytes();
            }
            allRecords.addAll(WalChunkReader.readRecords(chunkBytes));
        }
        assertEquals(1, allRecords.size());
    }

    public void testAddFailsAfterExhaustingTheRetryBudget() throws Exception {
        FailNTimesBlobContainer faulty = new FailNTimesBlobContainer(blobContainer, WalMirroringTranslog.MAX_MIRROR_FLUSH_ATTEMPTS);
        WalChunkService alwaysFlakyWalChunkService = new WalChunkService(faulty, "epoch-0");

        Path path = createTempDir();
        TranslogConfig config = new TranslogConfig(shardId, path, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
        try (
            WalMirroringTranslog translog = new WalMirroringTranslog(
                config,
                translogUUID,
                new DefaultTranslogDeletionPolicy(-1, -1, Integer.MAX_VALUE),
                () -> SequenceNumbers.UNASSIGNED_SEQ_NO,
                () -> 1L,
                seqNo -> {},
                TranslogOperationHelper.DEFAULT,
                alwaysFlakyWalChunkService
            )
        ) {
            expectThrows(
                java.io.IOException.class,
                () -> translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")))
            );
        }
    }

    public void testBatchingAddReturnsWithoutBlockingAndEnsureSyncedPerformsTheDurableUpload() throws Exception {
        CountDownLatch writeStarted = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        BlobContainer gated = new GateFirstWriteBlobContainer(blobContainer, writeStarted, releaseWrite);
        WalChunkService service = new WalChunkService(gated, "epoch-0");
        attachProcessor(service);

        try (WalMirroringTranslog translog = newTranslog(createTempDir(), service)) {
            Translog.Location location = translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            // add() returned to us even though the group-commit write is gated open -- it did not
            // block on the object-store upload the way the legacy synchronous path does.
            assertTrue("the group-commit drain must have reached the gated write", writeStarted.await(10, TimeUnit.SECONDS));
            assertEquals("add() must not have durably written the chunk itself; that is ensureSynced's job now", 0, logChunkCount());

            AtomicReference<Exception> ensureError = new AtomicReference<>();
            AtomicBoolean ensureReturned = new AtomicBoolean(false);
            Thread syncer = new Thread(() -> {
                try {
                    translog.ensureSynced(location);
                    ensureReturned.set(true);
                } catch (Exception e) {
                    ensureError.set(e);
                }
            }, "ensure-synced");
            syncer.start();
            try {
                // ensureSynced must park on the pending WAL upload while the write is still gated.
                assertBusy(() -> assertEquals(Thread.State.WAITING, syncer.getState()));
                assertFalse("ensureSynced must wait for the WAL group-commit upload", ensureReturned.get());
            } finally {
                releaseWrite.countDown();
                syncer.join(TimeUnit.SECONDS.toMillis(10));
            }
            assertNull("ensureSynced must not have failed", ensureError.get());
            assertTrue("ensureSynced must return once the upload completes", ensureReturned.get());
        }

        assertEquals("the operation must be durable in exactly one chunk after ensureSynced", 1, logChunkCount());
        byte[] chunkBytes;
        try (java.io.InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 0))) {
            chunkBytes = in.readAllBytes();
        }
        List<WalRecord> records = WalChunkReader.readRecords(chunkBytes);
        assertEquals(1, records.size());
        assertEquals(0L, records.get(0).seqNo());
    }

    public void testBatchingEnsureSyncedPreservesTheIOExceptionTypeOnUploadFailure() throws Exception {
        // Fails every writeBlob, so the group-commit drain's writeChunkWithRetry exhausts its retries
        // and the batch's listener is handed the original IOException.
        WalChunkService failing = new WalChunkService(new FailNTimesBlobContainer(blobContainer, Integer.MAX_VALUE), "epoch-0");
        attachProcessor(failing);

        try (WalMirroringTranslog translog = newTranslog(createTempDir(), failing)) {
            Translog.Location location = translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            // Type preservation: ensureSynced must surface the *original* IOException from the chunk
            // write (expectThrows(IOException.class) already rules out an ExecutionException wrapper
            // leaking, since that is not an IOException), and it must be that same instance rather
            // than a generic re-wrap. A regression dropping the ExecutionException->IOException unwrap
            // would fail this: the surfaced message would no longer carry the chunk-write text.
            java.io.IOException e = expectThrows(java.io.IOException.class, () -> translog.ensureSynced(location));
            assertTrue(
                "must preserve the original chunk-write IOException, got: " + e.getMessage(),
                e.getMessage().contains("injected transient failure")
            );
        }
    }

    public void testBatchingLastFlushedWalChunkSequenceAdvancesAfterEnsureSynced() throws Exception {
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        attachProcessor(service);

        try (WalMirroringTranslog translog = newTranslog(createTempDir(), service)) {
            assertEquals("nothing mirrored yet", -1L, translog.lastFlushedWalChunkSequence());

            Translog.Location loc1 = translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            translog.ensureSynced(loc1);
            long afterFirst = translog.lastFlushedWalChunkSequence();
            assertTrue("the first mirrored+synced operation must advance the watermark past -1", afterFirst >= 0);

            Translog.Location loc2 = translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
            translog.ensureSynced(loc2);
            long afterSecond = translog.lastFlushedWalChunkSequence();
            // Unlike the legacy flush-per-op path this must not *regress*, but may stay level if two
            // ops ever fold into one chunk -- the node-shared sequence is only a resume lower bound.
            assertTrue("the watermark must not regress across mirrored+synced operations", afterSecond >= afterFirst);
        }
    }

    /** Blocks the first {@code writeBlob} (after signalling it has started) until released, then delegates every write normally. */
    private static final class GateFirstWriteBlobContainer extends FilterBlobContainer {
        private final BlobContainer delegate;
        private final CountDownLatch firstWriteStarted;
        private final CountDownLatch releaseFirstWrite;
        private final AtomicBoolean firstWriteGated = new AtomicBoolean(false);

        GateFirstWriteBlobContainer(BlobContainer delegate, CountDownLatch firstWriteStarted, CountDownLatch releaseFirstWrite) {
            super(delegate);
            this.delegate = delegate;
            this.firstWriteStarted = firstWriteStarted;
            this.releaseFirstWrite = releaseFirstWrite;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new GateFirstWriteBlobContainer(child, firstWriteStarted, releaseFirstWrite);
        }

        @Override
        public void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            if (firstWriteGated.compareAndSet(false, true)) {
                firstWriteStarted.countDown();
                try {
                    releaseFirstWrite.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new java.io.IOException("interrupted while gated", e);
                }
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws java.io.IOException {
            return delegate.readRegister(blobName);
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
            String blobName,
            long expectedGeneration,
            org.opensearch.core.common.bytes.BytesReference newValue
        ) throws java.io.IOException {
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }

    /** Fails the first {@code failureCount} writeBlob calls, then delegates normally. */
    private static final class FailNTimesBlobContainer extends FilterBlobContainer {

        // FilterBlobContainer keeps its own delegate reference private and doesn't override
        // readRegister/compareAndSwapRegister (inheriting BlobContainer's own
        // UnsupportedOperationException-throwing defaults instead of delegating) -- WalChunkService
        // now needs both for its CAS-based chunk sequence allocation, so this class keeps its own
        // reference to delegate to directly.
        private final BlobContainer delegate;
        private int remainingFailures;

        FailNTimesBlobContainer(BlobContainer delegate, int failureCount) {
            super(delegate);
            this.delegate = delegate;
            this.remainingFailures = failureCount;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new FailNTimesBlobContainer(child, remainingFailures);
        }

        @Override
        public synchronized void writeBlob(String blobName, java.io.InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws java.io.IOException {
            if (remainingFailures > 0) {
                remainingFailures--;
                throw new java.io.IOException("injected transient failure writing " + blobName);
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws java.io.IOException {
            return delegate.readRegister(blobName);
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
            String blobName,
            long expectedGeneration,
            org.opensearch.core.common.bytes.BytesReference newValue
        ) throws java.io.IOException {
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
