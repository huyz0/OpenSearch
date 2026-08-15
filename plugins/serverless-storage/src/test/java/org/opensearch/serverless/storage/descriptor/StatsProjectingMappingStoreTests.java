/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The property under test is not "both stores are called". It is that the authoritative store decides every
 * answer and the projection can fail without anyone noticing except the log, because those are the two ways
 * this composition can be got wrong: reading from the projection makes it a second source of truth, and
 * letting it fail the write makes a statistic load-bearing.
 */
public class StatsProjectingMappingStoreTests extends OpenSearchTestCase {

    /** Runs inline, so the assertions do not have to wait for a pool. */
    private static final Executor DIRECT = Runnable::run;

    /** Records what it was asked to do, and can be told to refuse or to throw. */
    private static final class RecordingStore implements MappingGenerationStore.Store {
        private final List<String> calls = new ArrayList<>();
        private MappingGenerationStore.MappingGeneration readAnswer;
        private boolean swapAnswer = true;
        private RuntimeException failure;

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            calls.add("read:" + indexUuid);
            if (failure != null) {
                throw failure;
            }
            return readAnswer;
        }

        @Override
        public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
            calls.add("swap:" + indexUuid + ":" + expectedGeneration + "->" + updated.generation());
            if (failure != null) {
                throw failure;
            }
            return swapAnswer;
        }

        @Override
        public void delete(String indexUuid) {
            calls.add("delete:" + indexUuid);
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static MappingGenerationStore.MappingGeneration generation(long generation) {
        return new MappingGenerationStore.MappingGeneration(generation, Map.of("age", Map.of("type", "long")));
    }

    public void testReadNeverConsultsTheProjection() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        authoritative.readAnswer = generation(7L);

        MappingGenerationStore.MappingGeneration read = new StatsProjectingMappingStore(authoritative, projection, DIRECT).read("uuid-1");

        assertEquals(7L, read.generation());
        assertEquals(List.of("read:uuid-1"), authoritative.calls);
        assertTrue("the projection is derived; reading it would make it a second source of truth", projection.calls.isEmpty());
    }

    public void testASuccessfulSwapIsProjected() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();

        assertTrue(new StatsProjectingMappingStore(authoritative, projection, DIRECT).compareAndSwap("uuid-1", 4L, generation(5L)));

        assertEquals(List.of("swap:uuid-1:4->5"), authoritative.calls);
        assertEquals("the projection must see the same generation, or the aggregate drifts", List.of("swap:uuid-1:4->5"), projection.calls);
    }

    public void testARefusedSwapProjectsNothing() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        authoritative.swapAnswer = false;

        assertFalse(new StatsProjectingMappingStore(authoritative, projection, DIRECT).compareAndSwap("uuid-1", 4L, generation(5L)));

        assertTrue("a mapping no descriptor ever accepted must not appear in the projection", projection.calls.isEmpty());
    }

    public void testAFailingProjectionDoesNotFailTheMappingWrite() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        projection.failure = new IllegalStateException("the mapping index is unavailable");

        assertTrue(
            "the mapping is stored; a statistic that could not be updated is not a failed write",
            new StatsProjectingMappingStore(authoritative, projection, DIRECT).compareAndSwap("uuid-1", 0L, generation(1L))
        );
    }

    public void testARejectedExecutorDoesNotFailTheMappingWriteOrStrandTheDrain() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        Executor rejecting = task -> { throw new org.opensearch.core.concurrency.OpenSearchRejectedExecutionException("queue full"); };

        StatsProjectingMappingStore store = new StatsProjectingMappingStore(authoritative, projection, rejecting);
        assertTrue(store.compareAndSwap("uuid-1", 0L, generation(1L)));
        assertTrue(projection.calls.isEmpty());
        // A submission that never ran still has to settle its in-flight count, or every later drain waits out
        // its whole timeout on a task that cannot finish -- a hang wearing a slow shutdown's clothes.
        assertTrue("a rejected projection must not strand the drain", store.awaitQuiescence(0));
    }

    /** A pool shutting down throws something other than a rejection, and it must land the same way. */
    public void testAnyOtherSubmissionFailureIsHandledIdentically() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        Executor broken = task -> { throw new IllegalStateException("pool is closed"); };

        StatsProjectingMappingStore store = new StatsProjectingMappingStore(authoritative, projection, broken);
        assertTrue(
            "the mapping is stored, so a projection that could not even be submitted is not a failed write",
            store.compareAndSwap("uuid-1", 0L, generation(1L))
        );
        assertTrue(store.awaitQuiescence(0));
    }

    public void testTheDrainWaitsForAnInFlightProjection() throws Exception {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newSingleThreadExecutor();
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        try {
            StatsProjectingMappingStore store = new StatsProjectingMappingStore(authoritative, projection, task -> pool.execute(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            }));

            assertTrue(store.compareAndSwap("uuid-1", 0L, generation(1L)));
            assertFalse("the projection is still blocked, so the drain must not claim it settled", store.awaitQuiescence(200));

            release.countDown();
            assertTrue("once it finishes, the drain returns", store.awaitQuiescence(10_000));
            assertEquals(List.of("swap:uuid-1:0->1"), projection.calls);
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /**
     * The bound that keeps a statistic from stopping the thing it measures.
     *
     * <p>Measuring creation throughput found 127 of 132 {@code GENERIC} threads parked inside this class,
     * each blocked on a projection's indexing round trip -- and gated creation runs on that same pool, so an
     * arm of 300 mapped creations never finished. Every projection here blocks, so without the bound this
     * asserts what that run found: as many threads occupied as there were mapping writes.
     */
    public void testProjectionsCannotOccupyMoreThanTheirShareOfThePool() throws Exception {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newCachedThreadPool();
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger occupied = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger peak = new java.util.concurrent.atomic.AtomicInteger();
        try {
            StatsProjectingMappingStore store = new StatsProjectingMappingStore(authoritative, projection, task -> pool.execute(() -> {
                peak.accumulateAndGet(occupied.incrementAndGet(), Math::max);
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    occupied.decrementAndGet();
                }
                task.run();
            }));

            for (int i = 0; i < 50; i++) {
                assertTrue("the mapping write itself must never be refused", store.compareAndSwap("uuid-" + i, 0L, generation(1L)));
            }
            assertBusy(() -> assertEquals("every permitted projection should have started", 4, occupied.get()));
            assertEquals("a statistic must not be able to take the pool its own creations run on", 4, peak.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    /** A dropped projection must not leave the drain waiting for work nobody is doing. */
    public void testTheDrainDoesNotWaitForProjectionsThatWereNeverSubmitted() throws Exception {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newCachedThreadPool();
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        try {
            StatsProjectingMappingStore store = new StatsProjectingMappingStore(authoritative, projection, task -> pool.execute(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                task.run();
            }));
            for (int i = 0; i < 20; i++) {
                store.compareAndSwap("uuid-" + i, 0L, generation(1L));
            }

            release.countDown();
            assertTrue("only the four that were admitted are outstanding, so the drain must return", store.awaitQuiescence(10_000));
            assertEquals("and exactly those four reached the projection", 4, projection.calls.size());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    public void testDeleteRemovesBothAndTheAuthoritativeFailurePropagates() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();

        new StatsProjectingMappingStore(authoritative, projection, DIRECT).delete("uuid-1");
        assertEquals(List.of("delete:uuid-1"), authoritative.calls);
        assertEquals(
            "a projected mapping for a deleted index would inflate the counts forever",
            List.of("delete:uuid-1"),
            projection.calls
        );

        RecordingStore failing = new RecordingStore();
        failing.failure = new IllegalStateException("could not remove");
        RecordingStore untouched = new RecordingStore();
        expectThrows(IllegalStateException.class, () -> new StatsProjectingMappingStore(failing, untouched, DIRECT).delete("uuid-2"));
        assertTrue("the projection must not be emptied while the real mapping is still there", untouched.calls.isEmpty());
    }

    public void testTheProjectionRunsOffTheCallersThread() {
        RecordingStore authoritative = new RecordingStore();
        RecordingStore projection = new RecordingStore();
        AtomicInteger submitted = new AtomicInteger();
        List<Runnable> deferred = new ArrayList<>();
        Executor later = task -> {
            submitted.incrementAndGet();
            deferred.add(task);
        };

        assertTrue(new StatsProjectingMappingStore(authoritative, projection, later).compareAndSwap("uuid-1", 0L, generation(1L)));

        assertEquals(1, submitted.get());
        assertTrue("the caller returned before the projection ran, which is the whole point", projection.calls.isEmpty());
        deferred.forEach(Runnable::run);
        assertEquals(List.of("swap:uuid-1:0->1"), projection.calls);
    }
}
