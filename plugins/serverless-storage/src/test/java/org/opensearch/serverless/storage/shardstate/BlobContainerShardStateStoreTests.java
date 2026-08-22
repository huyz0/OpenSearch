/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exercises {@link ShardStateStore} through {@link BlobContainerShardStateStore} backed by a real
 * {@link FsBlobContainer} &mdash; the same correctness properties as before this class replaced
 * the bespoke {@code FsShardStateStore} (which duplicated file-locking logic now generalized into
 * {@code FsBlobContainer.compareAndSwapRegister}), just reached through the generic primitive.
 */
public class BlobContainerShardStateStoreTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer(Path dir) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, dir, false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private ShardStateStore newStore() throws Exception {
        return new BlobContainerShardStateStore(newFsBlobContainer(createTempDir()));
    }

    public void testGetOnNeverActivatedShardReturnsEmpty() throws Exception {
        ShardStateStore store = newStore();
        assertEquals(Optional.empty(), store.get("idx", 0));
    }

    public void testFirstActivationSucceedsViaPutIfAbsent() throws Exception {
        ShardStateStore store = newStore();
        ShardHead initial = ShardHead.initial();

        CasResult result = store.compareAndSet("idx", 0, Optional.empty(), initial);
        assertEquals(CasResult.SUCCESS, result);

        Optional<VersionedShardHead> read = store.get("idx", 0);
        assertTrue(read.isPresent());
        assertEquals(initial, read.get().head());
    }

    public void testSecondPutIfAbsentAttemptConflicts() throws Exception {
        ShardStateStore store = newStore();
        store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial());

        // A second actor thinking the shard is unactivated tries the same put-if-absent: must lose.
        CasResult result = store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial());
        assertEquals(CasResult.VERSION_CONFLICT, result);
    }

    public void testCasSucceedsWithMatchingVersionAndFailsWithStaleVersion() throws Exception {
        ShardStateStore store = newStore();
        store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial());
        VersionedShardHead v1 = store.get("idx", 0).orElseThrow();

        ShardHead published = v1.head().withPublishedGeneration(1);
        assertEquals(CasResult.SUCCESS, store.compareAndSet("idx", 0, Optional.of(v1.version()), published));

        // Retrying against the now-stale v1 version must fail.
        ShardHead publishedAgain = published.withPublishedGeneration(2);
        assertEquals(CasResult.VERSION_CONFLICT, store.compareAndSet("idx", 0, Optional.of(v1.version()), publishedAgain));

        // Reading fresh and retrying with the correct version succeeds.
        VersionedShardHead v2 = store.get("idx", 0).orElseThrow();
        assertEquals(published, v2.head());
        assertEquals(CasResult.SUCCESS, store.compareAndSet("idx", 0, Optional.of(v2.version()), publishedAgain));
    }

    public void testSequentialPublicationChainAdvancesGeneration() throws Exception {
        ShardStateStore store = newStore();
        store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial());

        for (int gen = 1; gen <= 20; gen++) {
            VersionedShardHead current = store.get("idx", 0).orElseThrow();
            ShardHead next = current.head().withPublishedGeneration(gen);
            assertEquals(CasResult.SUCCESS, store.compareAndSet("idx", 0, Optional.of(current.version()), next));
        }

        assertEquals(20L, store.get("idx", 0).orElseThrow().head().latestManifestGeneration());
    }

    // The concurrency-correctness property the metadata-plane RFC's placement model depends on
    // (section 6): "CAS makes concurrent activation attempts safe -- one winner, losers retry
    // elsewhere." This exercises real thread contention on the same shard head, not just
    // sequential logic.
    public void testConcurrentActivationRaceHasExactlyOneWinner() throws Exception {
        ShardStateStore store = newStore();
        int contenders = 20;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();
        List<CasResult> results = new CopyOnWriteArrayList<>();

        try {
            List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < contenders; i++) {
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        CasResult result = store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial());
                        results.add(result);
                        if (result == CasResult.SUCCESS) {
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals("exactly one contender must win the activation race", 1, successCount.get());
        assertEquals(contenders, results.size());
        assertEquals(contenders - 1, results.stream().filter(r -> r == CasResult.VERSION_CONFLICT).count());
        assertTrue(store.get("idx", 0).isPresent());
    }

    // Simulates a writer publishing many commits (rfc-serverless-opensearch.md section 6.4's
    // group-commit path) concurrently with a compaction service also trying to publish
    // (section 7.4's rebase protocol) -- every successful CAS must strictly advance the
    // generation, and the total number of successes must exactly equal the number of attempts
    // that eventually succeed after retry (no lost updates, no double-application).
    public void testConcurrentPublishersWithRetryNeverLoseAnUpdate() throws Exception {
        ShardStateStore store = newStore();
        store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial());

        int publishers = 8;
        int publicationsPerPublisher = 15;
        ExecutorService executor = Executors.newFixedThreadPool(publishers);
        try {
            List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
            for (int p = 0; p < publishers; p++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < publicationsPerPublisher; i++) {
                        publishOneGenerationWithRetry(store);
                    }
                }));
            }
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        long finalGeneration = store.get("idx", 0).orElseThrow().head().latestManifestGeneration();
        assertEquals((long) publishers * publicationsPerPublisher, finalGeneration);
    }

    private void publishOneGenerationWithRetry(ShardStateStore store) {
        while (true) {
            try {
                VersionedShardHead current = store.get("idx", 0).orElseThrow();
                ShardHead next = current.head().withPublishedGeneration(current.head().latestManifestGeneration() + 1);
                if (store.compareAndSet("idx", 0, Optional.of(current.version()), next) == CasResult.SUCCESS) {
                    return;
                }
                // lost the race -- re-read and retry, exactly as rfc-serverless-opensearch.md
                // section 7.4's compactor rebase protocol specifies.
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void testDifferentShardsAreIndependent() throws Exception {
        ShardStateStore store = newStore();
        assertEquals(CasResult.SUCCESS, store.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial()));
        assertEquals(CasResult.SUCCESS, store.compareAndSet("idx", 1, Optional.empty(), ShardHead.initial()));

        assertTrue(store.get("idx", 0).isPresent());
        assertTrue(store.get("idx", 1).isPresent());
        assertEquals(Optional.empty(), store.get("idx", 2));
    }

    public void testStateSurvivesAcrossStoreInstancesPointedAtSameDirectory() throws Exception {
        Path dir = createTempDir();
        ShardStateStore first = new BlobContainerShardStateStore(newFsBlobContainer(dir));
        first.compareAndSet("idx", 0, Optional.empty(), ShardHead.initial().withPublishedGeneration(5));

        // A different store instance (simulating a different node/process reading the same
        // backing storage) must see the same state.
        ShardStateStore second = new BlobContainerShardStateStore(newFsBlobContainer(dir));
        VersionedShardHead read = second.get("idx", 0).orElseThrow();
        assertEquals(5L, read.head().latestManifestGeneration());
    }
}
