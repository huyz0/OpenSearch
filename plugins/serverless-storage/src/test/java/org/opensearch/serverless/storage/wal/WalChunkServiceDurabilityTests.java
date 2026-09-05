/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression tests for the {@link WalChunkService} findings: D4 (the per-shard overflow path dropped
 * buffered records on a write failure and held the service monitor across network I/O), D8 (a chunk
 * sequence was claimed freshly on every retry attempt, burning sequences and letting a retry land
 * above a concurrent writer's fencing cutoff), and C1 (the steady-state claim paid a {@code
 * readRegister} it did not need).
 */
public class WalChunkServiceDurabilityTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "wal-svc-idx";

    private BlobContainer blobContainer;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private static WalRecord record(int shardId, long seqNo, int payloadBytes) {
        byte[] payload = new byte[payloadBytes];
        java.util.Arrays.fill(payload, (byte) ('a' + (seqNo % 26)));
        return new WalRecord(INDEX_UUID, shardId, 1L, seqNo, payload);
    }

    /**
     * <b>D4.</b> A shard crossing its per-shard fairness budget is siphoned into its own dedicated
     * chunk. If that write fails, the records must go back into the buffer so a later flush retries
     * them -- exactly the contract {@code flush()} already honours. The old implementation removed
     * them from the buffer and then wrote, so an exhausted retry budget silently destroyed them,
     * including records that shard had buffered from <em>earlier</em> appends and that no caller was
     * even waiting on.
     */
    public void testAFailedPerShardOverflowRestoresTheRecordsInsteadOfDroppingThem() throws Exception {
        FailWritesBlobContainer failing = new FailWritesBlobContainer(blobContainer);
        // Budget of 30 bytes: two 20-byte records for shard 0 cross it on the second append.
        WalChunkService service = new WalChunkService(failing, "epoch-0", 30);

        service.append(record(0, 0, 20));
        service.append(record(1, 100, 20)); // a different shard's record, which must survive untouched
        assertEquals(2, service.bufferedRecordCount());

        failing.failWrites = true;
        expectThrows(IOException.class, () -> service.append(record(0, 1, 20)));

        assertEquals(
            "every record must still be buffered after a failed overflow write: shard 0's two records "
                + "(the one that triggered the overflow and the earlier one swept up with it) plus shard 1's",
            3,
            service.bufferedRecordCount()
        );

        // And a later flush, once the store recovers, writes all of them exactly once.
        failing.failWrites = false;
        assertTrue(service.flush() >= 0);
        assertEquals(0, service.bufferedRecordCount());
        List<WalRecord> written = readAllRecords();
        assertEquals(3, written.size());
        // The restored overflow batch goes back at the FRONT of the buffer (mirroring flush()'s own
        // restore), so shard 0's two records keep their relative order and precede the record that
        // shard 1 had buffered alongside them.
        assertEquals("shard 0's records must retain their original append order", 0L, written.get(0).seqNo());
        assertEquals(1L, written.get(1).seqNo());
        assertEquals(100L, written.get(2).seqNo());
    }

    /**
     * <b>D4, second half.</b> {@code append} must not hold this service's monitor across the
     * overflow's object-store PUT. It used to, which meant one shard crossing its budget blocked
     * <em>every</em> other shard's append on the node for the duration of that PUT plus its retry
     * backoffs -- the exact node-wide bottleneck {@code flush()}'s javadoc explains it avoids.
     */
    public void testAnOverflowWriteDoesNotBlockOtherShardsAppends() throws Exception {
        CountDownLatch writeStarted = new CountDownLatch(1);
        CountDownLatch releaseWrite = new CountDownLatch(1);
        BlobContainer gated = new GateFirstWriteBlobContainer(blobContainer, writeStarted, releaseWrite);
        WalChunkService service = new WalChunkService(gated, "epoch-0", 30);

        AtomicBoolean overflowFailed = new AtomicBoolean(false);
        Thread overflowing = new Thread(() -> {
            try {
                service.append(record(0, 0, 20));
                service.append(record(0, 1, 20)); // crosses the budget, blocks inside the gated PUT
            } catch (Exception e) {
                overflowFailed.set(true);
            }
        }, "overflowing-shard");
        overflowing.start();
        assertTrue("the overflow's PUT must have started", writeStarted.await(10, TimeUnit.SECONDS));

        AtomicBoolean otherShardAppended = new AtomicBoolean(false);
        Thread other = new Thread(() -> {
            try {
                service.append(record(1, 100, 5));
                otherShardAppended.set(true);
            } catch (Exception e) {
                // leaves otherShardAppended false
            }
        }, "other-shard");
        other.start();
        other.join(TimeUnit.SECONDS.toMillis(10));

        assertTrue(
            "another shard's append must complete while the overflowing shard's PUT is still in flight -- "
                + "holding the service monitor across that PUT serialises the whole node",
            otherShardAppended.get()
        );

        releaseWrite.countDown();
        overflowing.join(TimeUnit.SECONDS.toMillis(10));
        assertFalse(overflowFailed.get());
    }

    /**
     * <b>D8.</b> A write that fails and is retried must land at the sequence it originally claimed,
     * not at a fresh higher one. Claiming per attempt burned a sequence on every failure (leaving
     * holes) and, worse, pushed the successfully-written records above any fencing cutoff a new
     * writer snapshotted in between -- silently excluding acknowledged records from replay.
     */
    public void testARetriedChunkWriteReusesItsOriginallyClaimedSequence() throws Exception {
        FailNTimesBlobContainer flaky = new FailNTimesBlobContainer(blobContainer, WalChunkService.MAX_WRITE_ATTEMPTS - 1);
        WalChunkService service = new WalChunkService(flaky, "epoch-0");

        service.append(record(0, 0, 8));
        long sequence = service.flush();

        assertEquals("the very first chunk under a fresh register must be sequence 0", 0L, sequence);
        assertTrue("the chunk must exist at the sequence that was actually returned", blobContainer.blobExists("log-" + sequence));
        assertEquals(
            "a failed attempt must not have burned extra sequences -- the register's generation must have advanced exactly once",
            1L,
            service.currentChunkSequenceUpperBound()
        );
    }

    /**
     * <b>C1.</b> The steady-state claim costs one object-store request (the CAS), not two: the
     * {@code readRegister} is only paid when this instance has no idea what the register's
     * generation is. That is a third off the whole legacy per-operation write path and off every
     * group-commit chunk, and it is safe because a wrong cached expectation merely loses a CAS,
     * which reports the truth.
     */
    public void testSteadyStateSequenceClaimSkipsTheRegisterRead() throws Exception {
        CountingRegisterBlobContainer counting = new CountingRegisterBlobContainer(blobContainer);
        WalChunkService service = new WalChunkService(counting, "epoch-0");

        service.append(record(0, 0, 8));
        service.flush();
        assertEquals("the first claim has nothing cached, so it reads the register once", 1, counting.reads.get());
        assertEquals(1, counting.casAttempts.get());

        for (int i = 1; i <= 5; i++) {
            service.append(record(0, i, 8));
            service.flush();
        }
        assertEquals("no further reads: every later claim goes straight to the CAS", 1, counting.reads.get());
        assertEquals("one CAS per chunk, uncontended", 6, counting.casAttempts.get());
    }

    /**
     * The cached generation is a hint, never a correctness input: another writer sharing the same
     * container advances the register behind this instance's back, and the next claim must still
     * produce a unique, correct sequence rather than colliding.
     */
    public void testAStaleCachedGenerationCostsARetryButNeverACollision() throws Exception {
        WalChunkService first = new WalChunkService(blobContainer, "epoch-a");
        WalChunkService second = new WalChunkService(blobContainer, "epoch-b");

        first.append(record(0, 0, 8));
        long a1 = first.flush();
        // second has never claimed, so it reads; then first's cache is now stale by one.
        second.append(record(1, 0, 8));
        long b1 = second.flush();
        first.append(record(0, 1, 8));
        long a2 = first.flush();

        assertNotEquals(a1, b1);
        assertNotEquals(a1, a2);
        assertNotEquals(b1, a2);
        assertEquals("three chunks, three distinct sequences, no overwrite", 3, blobContainer.listBlobsByPrefix("log-").size());
    }

    private List<WalRecord> readAllRecords() throws IOException {
        List<WalRecord> all = new ArrayList<>();
        List<String> names = new ArrayList<>(blobContainer.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).keySet());
        names.sort(java.util.Comparator.comparingLong(WalChunkNaming::parseChunkSequence));
        for (String name : names) {
            byte[] bytes;
            try (InputStream in = blobContainer.readBlob(name)) {
                bytes = in.readAllBytes();
            }
            all.addAll(WalChunkReader.readRecords(bytes));
        }
        return all;
    }

    /** Delegates registers to the raw container (so CAS semantics stay real) while letting blob writes be switched to failing. */
    private static class RegisterPassthroughContainer extends FilterBlobContainer {
        final BlobContainer raw;

        RegisterPassthroughContainer(BlobContainer raw) {
            super(raw);
            this.raw = raw;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new RegisterPassthroughContainer(child);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            return raw.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            return raw.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }

    private static final class FailWritesBlobContainer extends RegisterPassthroughContainer {
        volatile boolean failWrites = false;

        FailWritesBlobContainer(BlobContainer raw) {
            super(raw);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (failWrites) {
                throw new IOException("injected transient failure writing " + blobName);
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    private static final class FailNTimesBlobContainer extends RegisterPassthroughContainer {
        private final int failures;
        private final AtomicInteger attempts = new AtomicInteger();

        FailNTimesBlobContainer(BlobContainer raw, int failures) {
            super(raw);
            this.failures = failures;
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (attempts.incrementAndGet() <= failures) {
                throw new IOException("injected transient failure writing " + blobName);
            }
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }
    }

    private static final class GateFirstWriteBlobContainer extends RegisterPassthroughContainer {
        private final CountDownLatch started;
        private final CountDownLatch release;
        private final AtomicBoolean gated = new AtomicBoolean(false);

        GateFirstWriteBlobContainer(BlobContainer raw, CountDownLatch started, CountDownLatch release) {
            super(raw);
            this.started = started;
            this.release = release;
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            if (gated.compareAndSet(false, true)) {
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
    }

    private static final class CountingRegisterBlobContainer extends FilterBlobContainer {
        private final BlobContainer raw;
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger casAttempts = new AtomicInteger();

        CountingRegisterBlobContainer(BlobContainer raw) {
            super(raw);
            this.raw = raw;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new CountingRegisterBlobContainer(child);
        }

        @Override
        public Optional<BlobRegister> readRegister(String blobName) throws IOException {
            reads.incrementAndGet();
            return raw.readRegister(blobName);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
            throws IOException {
            casAttempts.incrementAndGet();
            return raw.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
