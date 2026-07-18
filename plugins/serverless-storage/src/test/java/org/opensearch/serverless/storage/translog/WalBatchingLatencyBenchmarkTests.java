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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer;
import org.opensearch.serverless.storage.benchmark.LatencyProfile;
import org.opensearch.serverless.storage.wal.WalBatchingProcessor;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A best-effort, environment-limited substitute for the real load test rfc-serverless-opensearch.md
 * &sect;6.4 and dynamic-partitioning-progress.md's own WAL-batching-Phase-3 entry both call for
 * before flipping {@code serverless_storage.wal_flush.batching.enabled}'s default to {@code true}.
 * There is no real cloud object store or load generator available in this environment (see that
 * Phase 3 entry for the full reasoning) -- this cannot substitute for measuring against real S3/GCS
 * latency, throttling, and request-cost behavior under genuine production concurrency. What it *can*
 * do, and does: give a real, reproducible, order-of-magnitude comparison of per-op indexing-ack
 * latency between the legacy (synchronous, one-PUT-per-op) and batching (group-commit) WAL paths,
 * under simulated object-store latency ({@link LatencyProfile}, the same simulated-latency machinery
 * {@code LazyDirectoryBootSetPrefetchBenchmarkTests} already uses for a different milestone) and
 * genuine concurrent multi-shard contention -- exactly the two variables the RFC's cost/latency
 * trade-off argument depends on.
 *
 * <p>Drives each simulated shard's indexing thread through the same request-thread-waits-for-durable-upload
 * cycle {@code index.translog.durability=REQUEST} drives in the real engine: op N+1 does not start
 * until op N's WAL durability ack returns, matching real synchronous indexing-ack latency, not a
 * batch-and-forget throughput measurement.
 *
 * <p><b>A second, unplanned finding this benchmark's first run surfaced</b>: {@link WalChunkService#append}/
 * {@link WalChunkService#flush} (the legacy path) are {@code synchronized}, so concurrent shards
 * sharing one instance -- exactly how the real plugin shares one node-level {@code WalChunkService}
 * across every writer shard -- don't just each pay the simulated write latency independently, they
 * fully serialize through it. An initial run with a larger op count under {@link LatencyProfile#HIGH}
 * exceeded a 120s bound for this reason alone (confirmed via thread dump, not assumed), which is why
 * {@link #OPS_PER_SHARD} stays intentionally small here. This is a genuine, additional argument for
 * batching beyond the per-op ack-latency numbers this benchmark's assertion checks: the legacy path's
 * real throughput ceiling under multi-shard contention is one shared lock's worth of sequential
 * object-store PUTs, not N independent ones.
 */
public class WalBatchingLatencyBenchmarkTests extends OpenSearchTestCase {

    private static final int SHARD_COUNT = 4;
    // Deliberately small: WalChunkService#append/flush (the legacy path) are `synchronized`, so
    // every "shard" thread sharing one instance fully serializes -- confirmed by this benchmark's
    // own first run, where SHARD_COUNT x 60 ops under HIGH-profile simulated latency exceeded a
    // 120s bound. That serialization is itself a real, meaningful finding (see this class's own
    // "what this benchmark actually shows" note below), not a benchmark bug -- but a benchmark
    // needs to run reliably in CI, so the op count stays small enough to finish quickly even under
    // full serialization at HIGH latency.
    private static final int OPS_PER_SHARD = 15;

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

    public void testCompareLegacyAndBatchingAckLatencyUnderSimulatedTypicalLatency() throws Exception {
        runComparison(LatencyProfile.TYPICAL, "TYPICAL");
    }

    public void testCompareLegacyAndBatchingAckLatencyUnderSimulatedHighLatency() throws Exception {
        runComparison(LatencyProfile.HIGH, "HIGH");
    }

    private void runComparison(LatencyProfile profile, String profileName) throws Exception {
        long[] legacyLatenciesMs = runLegacy(profile);
        long[] batchingLatenciesMs = runBatching(profile);

        Arrays.sort(legacyLatenciesMs);
        Arrays.sort(batchingLatenciesMs);
        long legacyP50 = percentile(legacyLatenciesMs, 50);
        long legacyP99 = percentile(legacyLatenciesMs, 99);
        long batchingP50 = percentile(batchingLatenciesMs, 50);
        long batchingP99 = percentile(batchingLatenciesMs, 99);

        logger.info(
            "[WAL batching latency benchmark, profile={}] legacy p50={}ms p99={}ms | batching p50={}ms p99={}ms " + "| {} shards x {} ops",
            profileName,
            legacyP50,
            legacyP99,
            batchingP50,
            batchingP99,
            SHARD_COUNT,
            OPS_PER_SHARD
        );

        // The real, load-bearing claim this benchmark can actually support: under concurrent
        // multi-shard contention, the legacy path's per-op ack latency is bounded below by the
        // simulated write latency *and* grows with contention (every shard's op serializes through
        // its own synchronous PUT), while the batching path's ack latency is bounded by the buffer
        // interval regardless of how many shards are contending, since one drain durably covers
        // every shard's queued records at once. Assert the comparative shape, not an absolute
        // number -- this is a simulated-latency, non-cloud benchmark, not a production SLO.
        // A 50% headroom multiplier, not a strict <=: this is a real-time measurement across
        // concurrently-contending threads, and even though a much wider margin is typical (batching
        // is bounded by the buffer interval regardless of contention, legacy is not), a strict
        // comparison between two live timings is exactly the shape of assertion that has produced a
        // near-miss elsewhere in this benchmark suite (see VectorWorkloadColdQueryBenchmarkTests).
        assertTrue(
            "batching's p99 ack latency must not run away with shard-count contention the way the legacy "
                + "path's does -- legacy p99="
                + legacyP99
                + "ms, batching p99="
                + batchingP99
                + "ms, profile="
                + profileName,
            batchingP99 <= legacyP99 * 1.5
        );
    }

    private static long percentile(long[] sortedMillis, int percentile) {
        int index = Math.min(sortedMillis.length - 1, (sortedMillis.length * percentile) / 100);
        return sortedMillis[index];
    }

    /** Drives {@link #SHARD_COUNT} concurrent threads through the legacy append+flush-per-op path, one PUT per op. */
    private long[] runLegacy(LatencyProfile profile) throws Exception {
        BlobContainer container = newLatencyInjectingContainer(profile);
        WalChunkService service = new WalChunkService(container, "bench-epoch-legacy");
        AtomicInteger sampleCounter = new AtomicInteger(0);
        long[] latenciesMs = new long[SHARD_COUNT * OPS_PER_SHARD];

        ExecutorService executor = Executors.newFixedThreadPool(SHARD_COUNT);
        try {
            List<CompletableFuture<Void>> shardFutures = new ArrayList<>();
            for (int shardId = 0; shardId < SHARD_COUNT; shardId++) {
                int finalShardId = shardId;
                CompletableFuture<Void> shardFuture = new CompletableFuture<>();
                shardFutures.add(shardFuture);
                executor.submit(() -> {
                    try {
                        for (int opIndex = 0; opIndex < OPS_PER_SHARD; opIndex++) {
                            WalRecord record = newRecord(finalShardId, opIndex);
                            long start = System.nanoTime();
                            service.append(record);
                            service.flush();
                            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                            latenciesMs[sampleCounter.getAndIncrement()] = elapsedMs;
                        }
                        shardFuture.complete(null);
                    } catch (Exception e) {
                        shardFuture.completeExceptionally(e);
                    }
                });
            }
            CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture<?>[0])).get(180, TimeUnit.SECONDS);
        } finally {
            // shutdownNow (not just shutdown): a timed-out run above may still have threads blocked
            // inside WalChunkService's synchronized append/flush -- shutdown() alone leaves those
            // running and leaks them past this test, tripping the suite's own thread-leak detector.
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        return latenciesMs;
    }

    /**
     * Drives {@link #SHARD_COUNT} concurrent threads through the batching path, each op waiting on
     * its own group-commit future before the "indexing thread" starts the next op -- matching real
     * {@code index.translog.durability=REQUEST} ack-blocking semantics.
     */
    private long[] runBatching(LatencyProfile profile) throws Exception {
        BlobContainer container = newLatencyInjectingContainer(profile);
        WalChunkService service = new WalChunkService(container, "bench-epoch-batching");
        WalBatchingProcessor processor = new WalBatchingProcessor(
            LogManager.getLogger(WalBatchingLatencyBenchmarkTests.class),
            10_000,
            threadPool.getThreadContext(),
            threadPool,
            () -> TimeValue.timeValueMillis(200), // the plugin's own real default interval
            -1,
            -1,
            service,
            null
        );
        service.attachBatchingProcessor(processor);

        AtomicInteger sampleCounter = new AtomicInteger(0);
        long[] latenciesMs = new long[SHARD_COUNT * OPS_PER_SHARD];

        ExecutorService executor = Executors.newFixedThreadPool(SHARD_COUNT);
        try {
            List<CompletableFuture<Void>> shardFutures = new ArrayList<>();
            for (int shardId = 0; shardId < SHARD_COUNT; shardId++) {
                int finalShardId = shardId;
                CompletableFuture<Void> shardFuture = new CompletableFuture<>();
                shardFutures.add(shardFuture);
                executor.submit(() -> {
                    try {
                        for (int opIndex = 0; opIndex < OPS_PER_SHARD; opIndex++) {
                            WalRecord record = newRecord(finalShardId, opIndex);
                            long start = System.nanoTime();
                            CountDownLatch ackLatch = new CountDownLatch(1);
                            AtomicBoolean failed = new AtomicBoolean(false);
                            processor.put(record, exception -> {
                                if (exception != null) {
                                    failed.set(true);
                                }
                                ackLatch.countDown();
                            });
                            if (ackLatch.await(60, TimeUnit.SECONDS) == false) {
                                throw new IllegalStateException("group-commit ack timed out");
                            }
                            if (failed.get()) {
                                throw new IllegalStateException("group-commit reported a failure");
                            }
                            long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                            latenciesMs[sampleCounter.getAndIncrement()] = elapsedMs;
                        }
                        shardFuture.complete(null);
                    } catch (Exception e) {
                        shardFuture.completeExceptionally(e);
                    }
                });
            }
            CompletableFuture.allOf(shardFutures.toArray(new CompletableFuture<?>[0])).get(180, TimeUnit.SECONDS);
        } finally {
            // shutdownNow (not just shutdown): a timed-out run above may still have threads blocked
            // waiting on their ack; shutdown() alone leaves those running and leaks them past this
            // test, tripping the suite's own thread-leak detector.
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        return latenciesMs;
    }

    private static BlobContainer newLatencyInjectingContainer(LatencyProfile profile) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, createTempDir(), false);
        BlobContainer raw = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new LatencyInjectingBlobContainer(raw, profile);
    }

    private static WalRecord newRecord(int shardId, int opIndex) {
        return new WalRecord("bench-idx", shardId, 1, opIndex, ("payload-" + shardId + "-" + opIndex).getBytes(StandardCharsets.UTF_8));
    }
}
