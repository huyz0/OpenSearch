/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.translog;

import org.apache.logging.log4j.LogManager;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.wal.WalBatchingProcessor;
import org.opensearch.serverless.storage.wal.WalChunkNaming;
import org.opensearch.serverless.storage.wal.WalChunkReader;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Regression tests for the acknowledged-write-loss bug in {@link WalMirroringTranslog}'s batching
 * path (report finding D1): {@code ensureSynced} used to ignore its {@code location} argument
 * entirely and wait on a single {@code latestPendingWalFuture} field that {@code add} assigned
 * <em>after</em> enqueueing the record. With concurrent indexing threads -- which is the normal case
 * on a primary, since {@code Translog#add} holds only a read lock -- the field could end up holding
 * an <em>older</em> record's future, and a {@code REQUEST}-durability write was then acknowledged
 * before its own WAL chunk had been uploaded.
 *
 * <p>Both tests below fail against that implementation and pass against the per-location map that
 * replaced it. They are deterministic: the interleaving is forced with latches rather than left to
 * chance.
 */
public class WalMirroringTranslogPerLocationDurabilityTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "wal-perloc-idx";

    private ShardId shardId;
    private IndexSettings indexSettings;
    private BlobContainer blobContainer;
    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Index index = new Index("wal-perloc-index", INDEX_UUID);
        shardId = new ShardId(index, 0);
        indexSettings = IndexSettingsModule.newIndexSettings(index, org.opensearch.common.settings.Settings.EMPTY);
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private WalMirroringTranslog newTranslog(Path translogPath, WalChunkService service) throws Exception {
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
            service
        );
    }

    private WalBatchingProcessor attachProcessor(WalChunkService service) {
        WalBatchingProcessor processor = new WalBatchingProcessor(
            LogManager.getLogger(WalMirroringTranslogPerLocationDurabilityTests.class),
            10000,
            threadPool.getThreadContext(),
            threadPool,
            () -> TimeValue.timeValueMillis(20),
            -1,
            -1,
            service,
            null
        );
        service.attachBatchingProcessor(processor);
        return processor;
    }

    private int logChunkCount() throws IOException {
        return blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).size();
    }

    /**
     * <b>The exact D1 interleaving, forced deterministically.</b> A processor whose {@code put}
     * returns late for the <em>first</em> record reproduces the racy field assignment the old code
     * had: the first record is enqueued (and its batch even completes) while the caller is still
     * inside {@code put}, and only afterwards does that caller reach the assignment -- so the
     * "latest pending future" field ends up holding the <em>already-completed</em> first record's
     * future, while the second record's upload is still in flight.
     *
     * <p>Against the old implementation, {@code ensureSynced(secondLocation)} therefore returned
     * immediately with the second operation's chunk not yet written: an acknowledged, un-durable
     * write. Against the per-location map it blocks until the second record's own batch lands.
     */
    public void testEnsureSyncedWaitsForItsOwnRecordEvenWhenAnOlderFutureWasRegisteredLast() throws Exception {
        CountDownLatch secondWriteStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondWrite = new CountDownLatch(1);
        BlobContainer gated = new GateNthWriteBlobContainer(blobContainer, 2, secondWriteStarted, releaseSecondWrite);
        WalChunkService service = new WalChunkService(gated, "epoch-0");

        CountDownLatch releaseFirstPutReturn = new CountDownLatch(1);
        WalBatchingProcessor processor = new LatePutReturnProcessor(
            LogManager.getLogger(WalMirroringTranslogPerLocationDurabilityTests.class),
            threadPool,
            service,
            0L,
            releaseFirstPutReturn
        );
        service.attachBatchingProcessor(processor);

        try (WalMirroringTranslog translog = newTranslog(createTempDir(), service)) {
            AtomicReference<Translog.Location> firstLocation = new AtomicReference<>();
            AtomicReference<Exception> firstError = new AtomicReference<>();
            Thread first = new Thread(() -> {
                try {
                    firstLocation.set(translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8"))));
                } catch (Exception e) {
                    firstError.set(e);
                }
            }, "first-add");
            first.start();

            // The first record's own batch drains and lands while its add() is still inside put().
            assertBusy(() -> assertEquals("the first record's chunk must be durable", 1, logChunkCount()));

            // Now enqueue a second record. Its batch write is gated open, so it is genuinely not
            // durable when ensureSynced is called below.
            Translog.Location secondLocation = translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
            assertTrue("the second record's group-commit write must have started", secondWriteStarted.await(10, TimeUnit.SECONDS));

            // Only now let the first add() return -- under the old code this is the moment the
            // already-completed first future was installed as "the latest pending" one.
            releaseFirstPutReturn.countDown();
            first.join(TimeUnit.SECONDS.toMillis(10));
            assertNull(firstError.get());
            assertNotNull(firstLocation.get());

            AtomicBoolean ensureReturned = new AtomicBoolean(false);
            AtomicReference<Exception> ensureError = new AtomicReference<>();
            Thread syncer = new Thread(() -> {
                try {
                    translog.ensureSynced(secondLocation);
                    ensureReturned.set(true);
                } catch (Exception e) {
                    ensureError.set(e);
                }
            }, "ensure-synced-second");
            syncer.start();
            try {
                assertBusy(() -> assertEquals(Thread.State.WAITING, syncer.getState()));
                assertFalse(
                    "ensureSynced(secondLocation) must NOT return while the second record's own chunk is still unwritten -- "
                        + "returning here is exactly the acknowledged-but-un-durable write D1 describes",
                    ensureReturned.get()
                );
                assertEquals("the second record's chunk must still be unwritten at this point", 1, logChunkCount());
            } finally {
                releaseSecondWrite.countDown();
                syncer.join(TimeUnit.SECONDS.toMillis(10));
            }
            assertNull(ensureError.get());
            assertTrue(ensureReturned.get());
            assertEquals("both records must be durable once ensureSynced returned", 2, logChunkCount());
            assertTrue("the second operation must actually be present in a chunk", chunksContainSeqNo(1L));
        }
    }

    /**
     * The other half of "per-location": {@code ensureSynced} must not wait for records enqueued
     * <em>after</em> the location it was asked about. The old implementation, which always waited on
     * whatever was enqueued most recently, blocks here indefinitely -- the test would time out
     * instead of failing on an assertion, which is itself the point: that implementation answered a
     * question it was never asked.
     */
    public void testEnsureSyncedDoesNotWaitForARecordEnqueuedAfterTheGivenLocation() throws Exception {
        CountDownLatch secondWriteStarted = new CountDownLatch(1);
        CountDownLatch releaseSecondWrite = new CountDownLatch(1);
        BlobContainer gated = new GateNthWriteBlobContainer(blobContainer, 2, secondWriteStarted, releaseSecondWrite);
        WalChunkService service = new WalChunkService(gated, "epoch-0");
        attachProcessor(service);

        try (WalMirroringTranslog translog = newTranslog(createTempDir(), service)) {
            Translog.Location firstLocation = translog.add(new Translog.Index("id-1", 0, 1, "{\"field\":1}".getBytes("UTF-8")));
            assertBusy(() -> assertEquals(1, logChunkCount()));

            translog.add(new Translog.Index("id-2", 1, 1, "{\"field\":2}".getBytes("UTF-8")));
            assertTrue(secondWriteStarted.await(10, TimeUnit.SECONDS));

            // Must return promptly: the first location's own record is long since durable, and the
            // second one's in-flight upload is none of this call's business.
            translog.ensureSynced(firstLocation);

            releaseSecondWrite.countDown();
            assertBusy(() -> assertEquals(2, logChunkCount()));
        }
    }

    private boolean chunksContainSeqNo(long seqNo) throws IOException {
        for (String blobName : blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet()) {
            byte[] bytes;
            try (InputStream in = blobContainer.readBlob(blobName)) {
                bytes = in.readAllBytes();
            }
            List<WalRecord> records = WalChunkReader.readRecords(bytes);
            for (WalRecord record : records) {
                if (record.seqNo() == seqNo) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * A processor whose {@link #put} enqueues normally but does not <em>return</em> for the record
     * with {@code lateReturnSeqNo} until released -- the seam that lets a test reproduce the "an
     * older record's caller reaches the assignment last" interleaving deterministically.
     */
    private static final class LatePutReturnProcessor extends WalBatchingProcessor {
        private final long lateReturnSeqNo;
        private final CountDownLatch release;

        LatePutReturnProcessor(
            org.apache.logging.log4j.Logger logger,
            ThreadPool threadPool,
            WalChunkService service,
            long lateReturnSeqNo,
            CountDownLatch release
        ) {
            super(logger, 10000, threadPool.getThreadContext(), threadPool, () -> TimeValue.timeValueMillis(20), -1, -1, service, null);
            this.lateReturnSeqNo = lateReturnSeqNo;
            this.release = release;
        }

        @Override
        public void put(WalRecord item, Consumer<Exception> listener) {
            super.put(item, listener);
            if (item.seqNo() == lateReturnSeqNo) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Blocks the {@code n}-th {@code writeBlob} (after signalling it started) until released; every
     * other write passes straight through.
     */
    private static final class GateNthWriteBlobContainer extends FilterBlobContainer {
        private final BlobContainer delegate;
        private final int gatedWriteOrdinal;
        private final CountDownLatch started;
        private final CountDownLatch release;
        private final java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();

        GateNthWriteBlobContainer(BlobContainer delegate, int gatedWriteOrdinal, CountDownLatch started, CountDownLatch release) {
            super(delegate);
            this.delegate = delegate;
            this.gatedWriteOrdinal = gatedWriteOrdinal;
            this.started = started;
            this.release = release;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new GateNthWriteBlobContainer(child, gatedWriteOrdinal, started, release);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (writes.incrementAndGet() == gatedWriteOrdinal) {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while gated", e);
                }
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws IOException {
            return delegate.readRegister(blobName);
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
            String blobName,
            long expectedGeneration,
            org.opensearch.core.common.bytes.BytesReference newValue
        ) throws IOException {
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
