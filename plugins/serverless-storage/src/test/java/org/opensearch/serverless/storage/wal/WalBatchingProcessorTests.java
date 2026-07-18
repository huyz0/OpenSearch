/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.apache.logging.log4j.LogManager;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit coverage for {@link WalBatchingProcessor}'s wiring on top of core's {@link
 * org.opensearch.common.util.concurrent.BufferedAsyncIOProcessor}: many concurrent producers fold
 * into a single group-commit, a write failure fails every caller in the batch (nothing hangs), and
 * the bounded queue is real backpressure once full.
 */
public class WalBatchingProcessorTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private BlobContainer blobContainer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        blobContainer = newBlobContainer();
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static BlobContainer newBlobContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private WalBatchingProcessor newProcessor(WalChunkService service, TimeValue interval, int queueCapacity) {
        return newProcessor(service, interval, queueCapacity, -1);
    }

    private WalBatchingProcessor newProcessor(WalChunkService service, TimeValue interval, int queueCapacity, long byteThreshold) {
        return new WalBatchingProcessor(
            LogManager.getLogger(WalBatchingProcessorTests.class),
            queueCapacity,
            threadPool.getThreadContext(),
            threadPool,
            () -> interval,
            byteThreshold,
            -1,
            service,
            null
        );
    }

    private static CompletableFuture<Void> put(WalBatchingProcessor processor, WalRecord record) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        processor.put(record, exception -> {
            if (exception != null) {
                future.completeExceptionally(exception);
            } else {
                future.complete(null);
            }
        });
        return future;
    }

    private int logChunkCount() throws IOException {
        return blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).size();
    }

    /**
     * The point of the whole subclass: a burst of concurrent {@link WalBatchingProcessor#put} calls
     * that all land while one drain is in flight collapses into a <em>single</em> group-commit, not
     * one write per caller. Made deterministic by gating the very first write (the priming op's own
     * drain) open only after the burst has fully enqueued behind the held promise-semaphore, so the
     * next drain provably sees the whole burst at once.
     */
    public void testAConcurrentBurstFoldsIntoASingleGroupCommit() throws Exception {
        CountDownLatch firstWriteStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstWrite = new CountDownLatch(1);
        BlobContainer gated = new GateFirstWriteBlobContainer(blobContainer, firstWriteStarted, releaseFirstWrite);
        WalChunkService service = new WalChunkService(gated, "epoch-0");
        WalBatchingProcessor processor = newProcessor(service, TimeValue.timeValueMillis(50), 1000);

        // Priming op: its drain fires, drains exactly this one record, and blocks inside write() --
        // holding the promise semaphore so no other drain can start until we release it.
        CompletableFuture<Void> priming = put(processor, new WalRecord("idx", 0, 1, 0, "prime".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertTrue("the priming write must reach write() before the burst is enqueued", firstWriteStarted.await(10, TimeUnit.SECONDS));

        // The burst: fires while the priming drain is blocked, so every one of these enqueues behind
        // the held semaphore and none can be drained until the priming write completes.
        int burst = 50;
        ExecutorService executor = Executors.newFixedThreadPool(burst);
        List<CompletableFuture<Void>> burstFutures = new ArrayList<>();
        CountDownLatch startLine = new CountDownLatch(1);
        // Every burst put() returns as soon as it has enqueued -- scheduleProcess can't start a new
        // drain while the priming drain holds the promise semaphore -- so counting these down tells
        // us the whole burst is queued, without needing to peek at the (package-private) queue size.
        CountDownLatch allEnqueued = new CountDownLatch(burst);
        try {
            for (int i = 0; i < burst; i++) {
                final int seqNo = i + 1;
                CompletableFuture<Void> future = new CompletableFuture<>();
                burstFutures.add(future);
                executor.submit(() -> {
                    try {
                        startLine.await();
                        processor.put(new WalRecord("idx", 0, 1, seqNo, ("v" + seqNo).getBytes(java.nio.charset.StandardCharsets.UTF_8)), exception -> {
                            if (exception != null) {
                                future.completeExceptionally(exception);
                            } else {
                                future.complete(null);
                            }
                        });
                        allEnqueued.countDown();
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                });
            }
            startLine.countDown();
            // Let every burst put() enqueue before releasing the priming write.
            assertTrue("the whole burst must enqueue before the priming write is released", allEnqueued.await(20, TimeUnit.SECONDS));
            releaseFirstWrite.countDown();

            CompletableFuture.allOf(burstFutures.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
            priming.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdown();
        }

        assertEquals("priming write + one folded burst write == exactly two chunks", 2, logChunkCount());
        byte[] burstChunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 1))) {
            burstChunkBytes = in.readAllBytes();
        }
        assertEquals("the entire burst must have folded into one chunk", burst, WalChunkReader.readRecords(burstChunkBytes).size());
    }

    /**
     * A drain failure must fail every caller whose record was in that batch with the same exception,
     * never leave any of them waiting forever -- this is base-class behavior, asserted here for this
     * subclass's own wiring (the record-list build + {@code writeChunkWithRetry} call).
     */
    public void testAWriteFailureNotifiesEveryCandidateInTheBatch() throws Exception {
        WalChunkService failing = new WalChunkService(new AlwaysFailingOnWriteBlobContainer(newBlobContainer()), "epoch-0");
        WalBatchingProcessor processor = newProcessor(failing, TimeValue.timeValueMillis(20), 1000);

        int count = 12;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(put(processor, new WalRecord("idx", 0, 1, i, ("v" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        }
        for (CompletableFuture<Void> future : futures) {
            ExecutionException e = expectThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
            assertTrue(
                "every candidate in a failed batch must be notified with the underlying IOException, got " + e.getCause(),
                e.getCause() instanceof IOException
            );
        }
    }

    /**
     * The bounded queue is real backpressure: with the drain interval set far enough out that no
     * drain runs during the window, a {@link WalBatchingProcessor#put} onto a full queue blocks the
     * calling thread (parked inside {@link java.util.concurrent.ArrayBlockingQueue#put}) rather than
     * overrunning the bound. Released by interrupting the blocked thread, which the base class turns
     * into a listener notification rather than a leak.
     */
    public void testQueueCapacityBackpressureBlocksAPutOnceFull() throws Exception {
        CountDownLatch writeStarted = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        BlobContainer gated = new GateFirstWriteBlobContainer(blobContainer, writeStarted, releaseWrite);
        WalChunkService service = new WalChunkService(gated, "epoch-0");
        WalBatchingProcessor processor = newProcessor(service, TimeValue.timeValueMillis(50), 1);

        // Priming op: its drain fires, drains this record out of the (capacity-1) queue, and blocks
        // inside the gated write() -- crucially holding the promise semaphore, so no further drain can
        // run and empty the queue behind our back.
        put(processor, new WalRecord("idx", 0, 1, 0, "a".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertTrue("the priming write must be in flight (queue drained, semaphore held)", writeStarted.await(10, TimeUnit.SECONDS));
        // With the only drain blocked, this refills the single slot; the queue is now genuinely full.
        put(processor, new WalRecord("idx", 0, 1, 1, "b".getBytes(java.nio.charset.StandardCharsets.UTF_8)));

        AtomicBoolean thirdPutReturned = new AtomicBoolean(false);
        Thread blocked = new Thread(() -> {
            processor.put(new WalRecord("idx", 0, 1, 2, "c".getBytes(java.nio.charset.StandardCharsets.UTF_8)), exception -> {});
            thirdPutReturned.set(true);
        }, "blocked-put");
        blocked.start();
        try {
            // The third put must park inside the full queue's blocking put(), not return. Accept any
            // parked state (WAITING on the not-full condition, or momentarily BLOCKED contending the
            // queue's own lock under load) -- the load-bearing fact is that it does not return.
            assertBusy(() -> {
                Thread.State state = blocked.getState();
                assertTrue(
                    "expected the blocked put to be parked, was " + state,
                    state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING || state == Thread.State.BLOCKED
                );
            });
            assertFalse("a put() onto a full queue must block, not overrun the bound", thirdPutReturned.get());
        } finally {
            // Interrupt frees the blocked put; releasing the gate lets the in-flight drain finish so
            // teardown's thread-pool termination isn't left waiting on it.
            blocked.interrupt();
            blocked.join(TimeUnit.SECONDS.toMillis(10));
            releaseWrite.countDown();
        }
        assertFalse("the interrupted put must unblock and terminate", blocked.isAlive());
    }

    /**
     * A byte threshold triggers a drain immediately once crossed, without waiting for the (here,
     * deliberately very long) interval tick -- proving the early-flush trigger is real and not just a
     * disguised interval wait. The interval is set far longer than the test's own timeout, so a chunk
     * only appears at all if the byte threshold fired the drain.
     */
    public void testByteThresholdTriggersAnImmediateDrain() throws Exception {
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        int recordPayloadBytes = "0123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        // Threshold crosses on the third post-priming record; the interval is 10 minutes, far outside
        // the 10s wait below, so the second assertion can only pass if the byte threshold -- not the
        // interval -- fired the second drain.
        WalBatchingProcessor processor = new WalBatchingProcessor(
            LogManager.getLogger(WalBatchingProcessorTests.class),
            1000,
            threadPool.getThreadContext(),
            threadPool,
            () -> TimeValue.timeValueMinutes(10),
            recordPayloadBytes * 3L,
            -1,
            service,
            null
        );

        // Priming put: the base class always schedules a first-ever drain immediately regardless of
        // the configured interval (lastRunStartTimeInNs starts at zero), so this establishes a real
        // "last run" timestamp before the timed part of the test begins, closing off that loophole.
        put(processor, new WalRecord("idx", 0, 1, -1, "prime".getBytes(java.nio.charset.StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
        assertEquals(1, logChunkCount());

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(put(processor, new WalRecord("idx", 0, 1, i, "0123456789".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(10, TimeUnit.SECONDS);

        assertEquals("the byte threshold must have folded all three records into one immediate chunk", 2, logChunkCount());
        byte[] chunkBytes;
        try (InputStream in = blobContainer.readBlob(WalChunkNaming.blobName("epoch-0", 1))) {
            chunkBytes = in.readAllBytes();
        }
        assertEquals(3, WalChunkReader.readRecords(chunkBytes).size());
    }

    /**
     * Below the byte threshold, batching stays purely interval-driven: a single record that never
     * crosses the threshold must wait out the (short, here) interval rather than draining immediately.
     */
    public void testBelowByteThresholdWaitsForTheInterval() throws Exception {
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        WalBatchingProcessor processor = new WalBatchingProcessor(
            LogManager.getLogger(WalBatchingProcessorTests.class),
            1000,
            threadPool.getThreadContext(),
            threadPool,
            () -> TimeValue.timeValueMillis(300),
            1_000_000,
            -1,
            service,
            null
        );

        // Priming put: closes the same first-ever-call loophole documented in
        // testByteThresholdTriggersAnImmediateDrain -- without it this test would pass even if the
        // interval trigger were broken, since the very first drain always fires immediately regardless.
        put(processor, new WalRecord("idx", 0, 1, -1, "prime".getBytes(java.nio.charset.StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
        assertEquals(1, logChunkCount());

        CompletableFuture<Void> future = put(processor, new WalRecord("idx", 0, 1, 0, "x".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertEquals("must not have drained yet -- interval hasn't elapsed and threshold wasn't crossed", 1, logChunkCount());
        future.get(10, TimeUnit.SECONDS);
        assertEquals("the interval drain must still land eventually", 2, logChunkCount());
    }

    /**
     * A failure encrypting a record (thrown before {@code writeChunkWithRetry} is ever reached)
     * must still release those bytes from {@link WalBatchingProcessor#backlogBytes()} -- the same
     * "must not hang, must not leak" contract {@link #testAWriteFailureNotifiesEveryCandidateInTheBatch}
     * already proves for a write-layer failure. Regression test for a real bug: the encryption loop
     * used to run outside the try/finally that decrements backlogBytes, so an encryption failure
     * permanently leaked the batch's bytes, eventually wedging every future write with
     * OpenSearchRejectedExecutionException once the (never-reset) counter crossed the backlog
     * threshold.
     */
    public void testAnEncryptionFailureStillReleasesTheBatchFromTheBacklog() throws Exception {
        WalChunkService service = new WalChunkService(blobContainer, "epoch-0");
        EncryptionKeyProvider alwaysThrows = new EncryptionKeyProvider() {
            @Override
            public javax.crypto.SecretKey currentKey() {
                throw new IllegalStateException("injected key-provider failure");
            }

            @Override
            public javax.crypto.SecretKey currentKey(String indexUuid) {
                throw new IllegalStateException("injected key-provider failure");
            }
        };
        WalBatchingProcessor processor = new WalBatchingProcessor(
            LogManager.getLogger(WalBatchingProcessorTests.class),
            1000,
            threadPool.getThreadContext(),
            threadPool,
            () -> TimeValue.timeValueMillis(20),
            -1,
            -1,
            service,
            alwaysThrows
        );

        int count = 5;
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(put(processor, new WalRecord("idx", 0, 1, i, ("v" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8))));
        }
        for (CompletableFuture<Void> future : futures) {
            expectThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
        }

        assertBusy(
            () -> assertEquals(
                "an encryption failure must still release the batch's bytes from the backlog",
                0L,
                processor.backlogBytes()
            )
        );
    }

    /** Fails every {@code writeBlob}, but delegates the register operations {@code claimNextChunkSequence} needs so the failure is purely on the chunk write. */
    private static final class AlwaysFailingOnWriteBlobContainer extends FilterBlobContainer {
        private final BlobContainer delegate;

        AlwaysFailingOnWriteBlobContainer(BlobContainer delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new AlwaysFailingOnWriteBlobContainer(child);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            throw new IOException("injected permanent failure writing " + blobName);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return delegate.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
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
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (firstWriteGated.compareAndSet(false, true)) {
                firstWriteStarted.countDown();
                try {
                    releaseFirstWrite.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while gated", e);
                }
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return delegate.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return delegate.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
