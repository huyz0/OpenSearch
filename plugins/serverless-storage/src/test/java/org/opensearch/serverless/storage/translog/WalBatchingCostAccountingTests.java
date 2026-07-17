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
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.index.translog.TranslogOperationHelper;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.serverless.storage.wal.WalBatchingProcessor;
import org.opensearch.serverless.storage.wal.WalChunkNaming;
import org.opensearch.serverless.storage.wal.WalChunkReader;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The test that proves the point of the whole change (rfc-serverless-opensearch.md &sect;6.4's
 * cost-sanity argument), the WAL-batching counterpart to {@code GcSchedulerTaskCostAccountingTests}:
 * many concurrent {@code add()} calls across several shards, all landing within one buffer interval,
 * cost a flat <b>O(1)</b> number of PUT-shaped object-store requests -- not the O(N) one-PUT-per-op
 * the synchronous legacy path pays. Measured with the same {@link RequestCountingBlobContainer}/
 * {@link ObjectStoreRequestCounter} harness that already guards GC's DELETE cost.
 */
public class WalBatchingCostAccountingTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private WalMirroringTranslog newTranslog(int shardIdValue, WalChunkService service) throws Exception {
        Index index = new Index("cost-index", "cost-idx");
        ShardId shardId = new ShardId(index, shardIdValue);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, org.opensearch.common.settings.Settings.EMPTY);
        java.nio.file.Path path = createTempDir();
        TranslogConfig config = new TranslogConfig(shardId, path, indexSettings, BigArrays.NON_RECYCLING_INSTANCE, "node-0", false);
        String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.UNASSIGNED_SEQ_NO, shardId, 1L);
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

    public void testManyConcurrentAddsAcrossShardsCostConstantPutRequestsNotOnePerOp() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer fs = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());

        // Gate the very first chunk write open only after the whole burst has enqueued: because the
        // first drain is scheduled with zero delay (the processor has never run before), gating it
        // is what makes "the burst all lands within one interval" deterministic rather than timing-
        // dependent -- the burst provably piles up behind the held promise semaphore, then folds into
        // one drain when released.
        CountDownLatch firstWriteStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstWrite = new CountDownLatch(1);
        BlobContainer gated = new GateFirstWriteBlobContainer(fs, firstWriteStarted, releaseFirstWrite);

        ObjectStoreRequestCounter counter = new ObjectStoreRequestCounter();
        BlobContainer counting = new RequestCountingBlobContainer(gated, counter);
        WalChunkService service = new WalChunkService(counting, "epoch-0");
        WalBatchingProcessor processor = new WalBatchingProcessor(
            LogManager.getLogger(WalBatchingCostAccountingTests.class),
            100000,
            threadPool.getThreadContext(),
            threadPool,
            () -> TimeValue.timeValueMillis(50),
            -1,
            -1,
            service,
            null
        );
        service.attachBatchingProcessor(processor);

        int shardCount = 4;
        int opsPerShard = 50;
        int burstOps = shardCount * opsPerShard;

        List<WalMirroringTranslog> translogs = new ArrayList<>();
        for (int s = 0; s < shardCount; s++) {
            translogs.add(newTranslog(s, service));
        }
        try {
            // Priming op (shard 0): occupies the immediate first drain, which blocks in the gated
            // write holding the promise semaphore so no burst op can be drained until we release it.
            Translog.Location primeLocation = translogs.get(0)
                .add(new Translog.Index("prime", 999_999L, 1, "{\"p\":0}".getBytes(StandardCharsets.UTF_8)));
            assertTrue("the priming write must reach the gate", firstWriteStarted.await(10, TimeUnit.SECONDS));

            // The burst: one thread per shard, each adding opsPerShard ops, all concurrent. On the
            // batching path add() only enqueues, so every one returns while the priming drain still
            // holds the semaphore -- the whole burst accumulates in the shared queue.
            ExecutorService executor = Executors.newFixedThreadPool(shardCount);
            Translog.Location[] lastLocationPerShard = new Translog.Location[shardCount];
            CountDownLatch startLine = new CountDownLatch(1);
            CountDownLatch allEnqueued = new CountDownLatch(shardCount);
            java.util.concurrent.atomic.AtomicReference<Exception> burstError = new java.util.concurrent.atomic.AtomicReference<>();
            try {
                for (int s = 0; s < shardCount; s++) {
                    final int shard = s;
                    executor.submit(() -> {
                        try {
                            startLine.await();
                            Translog.Location last = null;
                            for (int k = 0; k < opsPerShard; k++) {
                                last = translogs.get(shard)
                                    .add(
                                        new Translog.Index(
                                            "s" + shard + "-k" + k,
                                            shard * 1000L + k,
                                            1,
                                            ("{\"v\":" + k + "}").getBytes(StandardCharsets.UTF_8)
                                        )
                                    );
                            }
                            lastLocationPerShard[shard] = last;
                            allEnqueued.countDown();
                        } catch (Exception e) {
                            burstError.compareAndSet(null, e);
                        }
                    });
                }
                startLine.countDown();
                boolean enqueued = allEnqueued.await(30, TimeUnit.SECONDS);
                if (burstError.get() != null) {
                    throw new AssertionError("a burst add() failed", burstError.get());
                }
                assertTrue("the whole burst must enqueue before the gate opens", enqueued);
            } finally {
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
            }

            // Open the gate; the priming write finishes, then a single drain folds the entire burst
            // into one chunk. ensureSynced on each shard waits for that group-commit to complete.
            releaseFirstWrite.countDown();
            translogs.get(0).ensureSynced(primeLocation);
            for (int s = 0; s < shardCount; s++) {
                translogs.get(s).ensureSynced(lastLocationPerShard[s]);
            }

            // The proof: the priming op and the entire concurrent burst together landed in exactly two
            // chunks (one priming, one folded burst), costing a flat handful of PUT-shaped requests
            // (2 CAS sequence claims + 2 blob writes = 4), independent of how many ops were added.
            long puts = counter.putCount();
            assertEquals("priming + one folded burst must be exactly two chunks", 2, logChunkCount(fs));
            assertTrue(
                "O(1), not O(N): "
                    + (burstOps + 1)
                    + " ops must cost a small constant number of PUT-shaped requests, not ~"
                    + (2L * (burstOps + 1))
                    + " (one-per-op) -- got "
                    + puts,
                puts <= 6
            );

            // ...and every op is genuinely durable: the two chunks together hold all burstOps + 1 records.
            int recovered = 0;
            for (long seq = 0; seq < 2; seq++) {
                try (InputStream in = fs.readBlob(WalChunkNaming.blobName("epoch-0", seq))) {
                    recovered += WalChunkReader.readRecords(in.readAllBytes()).size();
                }
            }
            assertEquals("no op may be silently dropped by the batching fold", burstOps + 1, recovered);
        } finally {
            for (WalMirroringTranslog translog : translogs) {
                translog.close();
            }
        }
    }

    private static int logChunkCount(BlobContainer container) throws IOException {
        return container.listBlobsByPrefix(WalChunkNaming.LOG_BLOB_PREFIX).size();
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
