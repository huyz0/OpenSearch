/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.store.NodeLogBlob;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.serverless.store.NodeLogStore.HedgeOutcome;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A hedged node-log append: a second identical PUT when the first is slow, landing on whichever answers first, and still
 * telling a fence from its own other PUT.
 */
public class NodeLogHedgeTests extends OpenSearchTestCase {

    /** What the n-th PUT does around the real write. */
    private enum Put {
        /** Writes at once. */
        FAST,
        /** Waits, then writes: slow to land. */
        SLOW_BEFORE,
        /** Writes, then waits before answering: landed, but slow to say so. */
        SLOW_AFTER,
        /** Writes, then answers with an error: landed, and the writer cannot know. */
        LANDS_THEN_FAILS
    }

    private static final long SLOW_MILLIS = 400;

    /** A store whose PUTs follow a script, one entry per PUT in the order they are sent; FAST past its end. */
    private static final class Scripted implements BlobStore {
        private final BlobStore inner;
        private final Put[] script;
        private final AtomicInteger puts = new AtomicInteger();

        Scripted(BlobStore inner, Put... script) {
            this.inner = inner;
            this.script = script;
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new DelegatingBlobContainer(inner.blobContainer(path)) {
                @Override
                public void writeBlob(String blobName, InputStream in, long size, boolean failIfAlreadyExists) throws IOException {
                    final int n = puts.getAndIncrement();
                    final Put what = n < script.length ? script[n] : Put.FAST;
                    if (what == Put.SLOW_BEFORE) {
                        sleep();
                    }
                    super.writeBlob(blobName, in, size, failIfAlreadyExists);
                    if (what == Put.SLOW_AFTER) {
                        sleep();
                    }
                    if (what == Put.LANDS_THEN_FAILS) {
                        sleep();
                        throw new IOException("connection reset after the body was sent");
                    }
                }
            };
        }

        private static void sleep() throws IOException {
            try {
                TimeUnit.MILLISECONDS.sleep(SLOW_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }

        @Override
        public Map<String, Long> stats() {
            return inner.stats();
        }
    }

    private static final byte[] BLOB = "a window's blob".getBytes(StandardCharsets.UTF_8);

    private ExecutorService pool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        pool = Executors.newCachedThreadPool();
    }

    @Override
    public void tearDown() throws Exception {
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        super.tearDown();
    }

    private static long hedgeAfter() {
        return TimeUnit.MILLISECONDS.toNanos(50);
    }

    private static byte[] read(BlobStore store, NodeLogStore log, long epoch, long ordinal) throws IOException {
        try (InputStream in = store.blobContainer(log.epochPath(epoch)).readBlob(NodeLogStore.name(ordinal))) {
            return in.readAllBytes();
        }
    }

    public void testAFastPutSendsNoHedge() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new Scripted(new FsBlobStore(8192, createTempDir(), false)))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            assertEquals(HedgeOutcome.NONE, log.appendHedged(1L, 0L, BLOB, hedgeAfter(), pool));
            assertEquals(1L, store.blobWrites());
            assertArrayEquals(BLOB, read(store, log, 1L, 0L));
        }
    }

    public void testASlowPutIsOvertakenByItsHedge() throws Exception {
        try (
            CountingBlobStore store = new CountingBlobStore(new Scripted(new FsBlobStore(8192, createTempDir(), false), Put.SLOW_BEFORE))
        ) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final long started = System.nanoTime();
            assertEquals(HedgeOutcome.WON, log.appendHedged(1L, 0L, BLOB, hedgeAfter(), pool));
            final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue("landed by the hedge, not after the slow PUT: " + tookMillis + " ms", tookMillis < SLOW_MILLIS);
            assertArrayEquals(BLOB, read(store, log, 1L, 0L));
        }
    }

    public void testAPutThatLandedButAnswersSlowlyIsNotMistakenForAFence() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new Scripted(new FsBlobStore(8192, createTempDir(), false), Put.SLOW_AFTER))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            // The hedge finds the slot taken, by the first PUT, which then answers that it landed.
            assertEquals(HedgeOutcome.SENT, log.appendHedged(1L, 0L, BLOB, hedgeAfter(), pool));
            assertArrayEquals(BLOB, read(store, log, 1L, 0L));
        }
    }

    public void testAPutThatLandedAndFailedIsReadBackRatherThanMistakenForAFence() throws Exception {
        try (
            CountingBlobStore store = new CountingBlobStore(
                new Scripted(new FsBlobStore(8192, createTempDir(), false), Put.LANDS_THEN_FAILS)
            )
        ) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            store.reset();
            assertEquals(HedgeOutcome.SENT, log.appendHedged(1L, 0L, BLOB, hedgeAfter(), pool));
            assertEquals("one read back tells this blob from a fence", 1L, store.blobReads());
        }
    }

    public void testAFencedSlotFailsBothPutsAsAFence() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new Scripted(new FsBlobStore(8192, createTempDir(), false)))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            assertEquals(0L, log.fence(1L, 0L, "node-2", 7L));
            expectThrows(FileAlreadyExistsException.class, () -> log.appendHedged(1L, 0L, BLOB, hedgeAfter(), pool));
            assertTrue("the slot still holds the fence", log.readEntry(1L, 0L).orElseThrow() instanceof NodeLogBlob.Fence);
        }
    }

    public void testAFenceThatWinsTheRaceWithBothPutsIsAFence() throws Exception {
        final AtomicLong fenced = new AtomicLong(-1);
        final FsBlobStore fs = new FsBlobStore(8192, createTempDir(), false);
        // Both PUTs are slow, and a successor's fence lands in the slot while they wait.
        try (CountingBlobStore store = new CountingBlobStore(new Scripted(fs, Put.SLOW_BEFORE, Put.SLOW_BEFORE))) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            final Thread successor = new Thread(() -> {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                    fenced.set(new NodeLogStore(fs, BlobPath.cleanPath(), "node-1").fence(1L, 0L, "node-2", 7L));
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            successor.start();
            expectThrows(FileAlreadyExistsException.class, () -> log.appendHedged(1L, 0L, BLOB, hedgeAfter(), pool));
            successor.join();
            assertEquals(0L, fenced.get());
            assertTrue(log.readEntry(1L, 0L).orElseThrow() instanceof NodeLogBlob.Fence);
        }
    }

    public void testNoHedgeWithoutADelay() throws Exception {
        try (
            CountingBlobStore store = new CountingBlobStore(new Scripted(new FsBlobStore(8192, createTempDir(), false), Put.SLOW_BEFORE))
        ) {
            final NodeLogStore log = new NodeLogStore(store, BlobPath.cleanPath(), "node-1");
            assertEquals(HedgeOutcome.NONE, log.appendHedged(1L, 0L, BLOB, 0L, pool));
            assertEquals(1L, store.blobWrites());
        }
    }
}
