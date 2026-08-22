/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.fs;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
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

/**
 * Tests {@link BlobContainer#readRegister}/{@link BlobContainer#compareAndSwapRegister} against
 * the real {@link FsBlobContainer} implementation, including the concurrency-correctness property
 * a compare-and-swap primitive exists for in the first place: under contention, exactly one
 * concurrent writer wins and everyone else observes a conflict rather than a lost update.
 */
public class FsBlobContainerRegisterTests extends OpenSearchTestCase {

    private BlobContainer newFsBlobContainer() throws Exception {
        return newFsBlobContainer(createTempDir());
    }

    /** A container over a caller-supplied directory, so two of them can be pointed at the same one. */
    private BlobContainer newFsBlobContainer(java.nio.file.Path directory) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, directory, false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testReadRegisterNeverWrittenReturnsEmpty() throws Exception {
        assertEquals(Optional.empty(), newFsBlobContainer().readRegister("head"));
    }

    public void testFirstWriteSucceedsViaPutIfAbsent() throws Exception {
        BlobContainer container = newFsBlobContainer();
        BlobRegisterCasResult result = container.compareAndSwapRegister(
            "head",
            BlobRegister.ABSENT_GENERATION,
            new BytesArray("v1".getBytes(java.nio.charset.StandardCharsets.UTF_8))
        );
        assertTrue(result.applied());
        assertEquals(1L, result.currentGeneration());

        BlobRegister read = container.readRegister("head").orElseThrow();
        assertEquals(1L, read.generation());
        assertEquals(new BytesArray("v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)), read.value());
    }

    public void testSecondPutIfAbsentAttemptConflicts() throws Exception {
        BlobContainer container = newFsBlobContainer();
        container.compareAndSwapRegister("head", BlobRegister.ABSENT_GENERATION, new BytesArray(new byte[] { 1 }));

        BlobRegisterCasResult result = container.compareAndSwapRegister(
            "head",
            BlobRegister.ABSENT_GENERATION,
            new BytesArray(new byte[] { 2 })
        );
        assertFalse(result.applied());
        assertEquals(1L, result.currentGeneration());
    }

    public void testCasSucceedsWithMatchingGenerationAndConflictsWithStaleGeneration() throws Exception {
        BlobContainer container = newFsBlobContainer();
        container.compareAndSwapRegister("head", BlobRegister.ABSENT_GENERATION, new BytesArray(new byte[] { 1 }));

        BlobRegisterCasResult second = container.compareAndSwapRegister("head", 1L, new BytesArray(new byte[] { 2 }));
        assertTrue(second.applied());
        assertEquals(2L, second.currentGeneration());

        // Retrying against the now-stale generation 1 must conflict, reporting the real current generation.
        BlobRegisterCasResult stale = container.compareAndSwapRegister("head", 1L, new BytesArray(new byte[] { 3 }));
        assertFalse(stale.applied());
        assertEquals(2L, stale.currentGeneration());
    }

    public void testSequentialChainAdvancesGenerationByOneEachTime() throws Exception {
        BlobContainer container = newFsBlobContainer();
        container.compareAndSwapRegister("head", BlobRegister.ABSENT_GENERATION, new BytesArray(new byte[] { 0 }));

        for (int i = 1; i <= 20; i++) {
            long current = container.readRegister("head").orElseThrow().generation();
            BlobRegisterCasResult result = container.compareAndSwapRegister("head", current, new BytesArray(new byte[] { (byte) i }));
            assertTrue(result.applied());
            assertEquals(current + 1, result.currentGeneration());
        }
        assertEquals(21L, container.readRegister("head").orElseThrow().generation());
    }

    // The property compareAndSwapRegister exists to guarantee: under real thread contention,
    // exactly one writer wins a put-if-absent race and everyone else gets a conflict.
    public void testConcurrentPutIfAbsentRaceHasExactlyOneWinner() throws Exception {
        BlobContainer container = newFsBlobContainer();
        int contenders = 20;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();
        List<BlobRegisterCasResult> results = new CopyOnWriteArrayList<>();

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < contenders; i++) {
                final int contenderId = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        BlobRegisterCasResult result = container.compareAndSwapRegister(
                            "head",
                            BlobRegister.ABSENT_GENERATION,
                            new BytesArray(new byte[] { (byte) contenderId })
                        );
                        results.add(result);
                        if (result.applied()) {
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals("exactly one contender must win the put-if-absent race", 1, successCount.get());
        assertEquals(contenders - 1, results.stream().filter(r -> r.applied() == false).count());
    }

    /**
     * The same race, but through <em>separate container instances</em> over one directory.
     *
     * <p>This is the shape that actually occurs, and the one the previous implementation did not
     * arbitrate. Every test above holds a single container, so they exercised a per-instance lock and
     * passed; an internal cluster test gives each node its own container over shared storage, and there
     * both contenders read the same absent generation, both passed the equality check, both wrote, and
     * the second silently won. A uniqueness test built on that would have asserted nothing.
     *
     * <p>Contenders are spread across the containers rather than one each, so the assertion covers both
     * the within-container and across-container paths in one run.
     */
    public void testConcurrentPutIfAbsentAcrossSeparateContainersHasExactlyOneWinner() throws Exception {
        java.nio.file.Path shared = createTempDir();
        int containerCount = 4;
        List<BlobContainer> containers = new java.util.ArrayList<>();
        for (int i = 0; i < containerCount; i++) {
            containers.add(newFsBlobContainer(shared));
        }

        int contenders = 20;
        ExecutorService executor = Executors.newFixedThreadPool(contenders);
        CountDownLatch startLine = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();

        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < contenders; i++) {
                final BlobContainer container = containers.get(i % containerCount);
                final int contenderId = i;
                futures.add(executor.submit(() -> {
                    try {
                        startLine.await();
                        BlobRegisterCasResult result = container.compareAndSwapRegister(
                            "head",
                            BlobRegister.ABSENT_GENERATION,
                            new BytesArray(new byte[] { (byte) contenderId })
                        );
                        if (result.applied()) {
                            successCount.incrementAndGet();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals("exactly one contender must win across separate containers", 1, successCount.get());

        // And the winner is readable at generation 1 through a container that did not write it, so the
        // single write is genuinely shared state rather than one instance's private view.
        BlobRegister read = containers.get(containerCount - 1).readRegister("head").orElseThrow();
        assertEquals(1L, read.generation());
    }

    // No lost updates under concurrent CAS-with-retry, mirroring how a real writer/compactor pair
    // would race to advance a shard-head's generation (rfc-serverless-metadata-plane.md section 6).
    public void testConcurrentWritersWithRetryNeverLoseAnUpdate() throws Exception {
        BlobContainer container = newFsBlobContainer();
        container.compareAndSwapRegister("head", BlobRegister.ABSENT_GENERATION, new BytesArray(new byte[] { 0 }));

        int writers = 8;
        int writesPerWriter = 15;
        ExecutorService executor = Executors.newFixedThreadPool(writers);
        try {
            List<Future<?>> futures = new java.util.ArrayList<>();
            for (int w = 0; w < writers; w++) {
                futures.add(executor.submit(() -> {
                    for (int i = 0; i < writesPerWriter; i++) {
                        writeOnceWithRetry(container);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals((long) writers * writesPerWriter + 1, container.readRegister("head").orElseThrow().generation());
    }

    private void writeOnceWithRetry(BlobContainer container) {
        while (true) {
            try {
                long current = container.readRegister("head").orElseThrow().generation();
                BytesReference newValue = new BytesArray(new byte[] { (byte) current });
                if (container.compareAndSwapRegister("head", current, newValue).applied()) {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
