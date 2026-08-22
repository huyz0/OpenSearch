/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CompactionRebaseExecutorTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private ShardStateStore newActivatedStore() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore store = new BlobContainerShardStateStore(blobContainer);
        store.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), ShardHead.initial());
        return store;
    }

    public void testSimplePublishSucceedsOnFirstAttemptWithNoContention() throws Exception {
        ShardStateStore store = newActivatedStore();
        CompactionRebaseExecutor executor = new CompactionRebaseExecutor(store, 5);

        RebaseResult result = executor.publish(
            INDEX_UUID,
            SHARD_ID,
            currentHead -> Optional.of(currentHead.withPublishedGeneration(currentHead.latestManifestGeneration() + 1))
        );

        assertTrue(result.isPublished());
        assertEquals(1, result.attempts());
        assertEquals(1L, store.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration());
    }

    public void testPublisherAbandoningReturnsAbandonedWithoutMutatingState() throws Exception {
        ShardStateStore store = newActivatedStore();
        CompactionRebaseExecutor executor = new CompactionRebaseExecutor(store, 5);
        long generationBefore = store.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration();

        RebaseResult result = executor.publish(INDEX_UUID, SHARD_ID, currentHead -> Optional.empty());

        assertEquals(RebaseResult.Outcome.ABANDONED, result.outcome());
        assertEquals(generationBefore, store.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration());
    }

    public void testNeverActivatedShardIsAbandonedImmediately() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore store = new BlobContainerShardStateStore(blobContainer); // never activated

        CompactionRebaseExecutor executor = new CompactionRebaseExecutor(store, 5);
        RebaseResult result = executor.publish(INDEX_UUID, SHARD_ID, currentHead -> Optional.of(currentHead.withPublishedGeneration(1)));

        assertEquals(RebaseResult.Outcome.ABANDONED, result.outcome());
        assertEquals(1, result.attempts());
    }

    // The core property from rfc-serverless-opensearch.md section 7.4: a competing publisher
    // (simulating a writer) publishes generation N+1 between the compactor's read and its CAS
    // attempt. The compactor must rebase -- recompute against the *actual* current head -- and
    // eventually succeed, rather than either failing outright or corrupting the writer's update.
    public void testRebasesAgainstConcurrentWriterPublicationAndEventuallySucceeds() throws Exception {
        ShardStateStore store = newActivatedStore();
        CompactionRebaseExecutor executor = new CompactionRebaseExecutor(store, 10);

        AtomicInteger callCount = new AtomicInteger();
        RebaseResult result = executor.publish(INDEX_UUID, SHARD_ID, currentHead -> {
            // On the compactor's *first* computeNewHead call, simulate a writer racing ahead and
            // publishing generation 1 before the compactor's CAS runs.
            if (callCount.getAndIncrement() == 0) {
                try {
                    var versioned = store.get(INDEX_UUID, SHARD_ID).orElseThrow();
                    CasResult raceResult = store.compareAndSet(
                        INDEX_UUID,
                        SHARD_ID,
                        Optional.of(versioned.version()),
                        versioned.head().withPublishedGeneration(1)
                    );
                    assertEquals(CasResult.SUCCESS, raceResult);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            // The compactor's merge is still valid; it just needs to republish on top of
            // whatever the actual current generation now is.
            return Optional.of(currentHead.withPublishedGeneration(currentHead.latestManifestGeneration() + 1));
        });

        assertTrue("compactor must eventually publish despite the writer's interleaved publication", result.isPublished());
        assertTrue("must have taken more than one attempt due to the rebase", result.attempts() >= 2);
    }

    public void testExhaustsAttemptsWhenAlwaysOutracedAndLeavesNoCorruptState() throws Exception {
        ShardStateStore store = newActivatedStore();
        int maxAttempts = 5;
        CompactionRebaseExecutor executor = new CompactionRebaseExecutor(store, maxAttempts);

        // A publisher that always loses: every time it's asked to compute a new head, a
        // concurrent "hotter" writer sneaks in and republishes first, so this compactor's CAS
        // always targets a now-stale version.
        RebaseResult result = executor.publish(INDEX_UUID, SHARD_ID, currentHead -> {
            try {
                var versioned = store.get(INDEX_UUID, SHARD_ID).orElseThrow();
                store.compareAndSet(
                    INDEX_UUID,
                    SHARD_ID,
                    Optional.of(versioned.version()),
                    versioned.head().withPublishedGeneration(versioned.head().latestManifestGeneration() + 1)
                );
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return Optional.of(currentHead.withPublishedGeneration(currentHead.latestManifestGeneration() + 1));
        });

        assertEquals(RebaseResult.Outcome.EXHAUSTED, result.outcome());
        assertEquals(maxAttempts, result.attempts());
        // State must still be well-formed: some valid generation was published by the "hotter"
        // writer, nothing corrupted or partially applied by the exhausted compactor.
        assertTrue(store.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration() > 0);
    }

    // Real concurrency: many threads each act as a compactor attempting the same logical
    // publish-next-generation operation through the executor. No update should be lost.
    public void testConcurrentRebaseExecutorsNeverLoseAnUpdate() throws Exception {
        ShardStateStore store = newActivatedStore();
        int publishers = 8;
        int publicationsPerPublisher = 10;
        ExecutorService threadPool = Executors.newFixedThreadPool(publishers);
        CountDownLatch startLine = new CountDownLatch(1);
        List<RebaseResult> allResults = new CopyOnWriteArrayList<>();

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int p = 0; p < publishers; p++) {
                futures.add(threadPool.submit(() -> {
                    try {
                        startLine.await();
                        for (int i = 0; i < publicationsPerPublisher; i++) {
                            CompactionRebaseExecutor executor = new CompactionRebaseExecutor(store, 50);
                            RebaseResult result = executor.publish(
                                INDEX_UUID,
                                SHARD_ID,
                                currentHead -> Optional.of(currentHead.withPublishedGeneration(currentHead.latestManifestGeneration() + 1))
                            );
                            allResults.add(result);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            threadPool.shutdown();
        }

        long published = allResults.stream().filter(RebaseResult::isPublished).count();
        assertEquals((long) publishers * publicationsPerPublisher, published);
        assertEquals(published, store.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration());
    }
}
