/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * H6a. Whether the mapping compare-and-swap converges when two shards infer at once.
 *
 * <p>This is the one part of H.7's design that could be quietly wrong. Two shards receiving documents
 * with different new fields at the same moment must both survive. A retry loop that re-read and then
 * overwrote would drop one field, and the document that introduced it would be silently unqueryable on
 * that field: a wrong answer that looks like a correct one, which is the shape this area has hit
 * repeatedly.
 *
 * <p><b>Tested with real concurrency rather than a sequential round trip</b>, because a sequential test
 * passes against an implementation that overwrites. The threads are started from a common latch so their
 * reads genuinely overlap, and the assertion is on the union of fields rather than on the last swap
 * succeeding.
 */
public class MappingGenerationStoreTests extends OpenSearchTestCase {

    @After
    public void clearStore() {
        MappingGenerationStore.register(null);
    }

    /**
     * The load-bearing one. Two writers, two different fields, both must survive.
     */
    public void testConcurrentInferenceOfDifferentFieldsConverges() throws Exception {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        int writers = 8;
        CountDownLatch startTogether = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(writers);
        AtomicReference<Exception> failure = new AtomicReference<>();

        for (int i = 0; i < writers; i++) {
            final String field = "field-" + i;
            new Thread(() -> {
                try {
                    startTogether.await();
                    MappingGenerationStore.updateMapping("idx", Map.of(field, "keyword"));
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                } finally {
                    finished.countDown();
                }
            }).start();
        }

        startTogether.countDown();
        assertTrue("every writer must finish", finished.await(30, TimeUnit.SECONDS));
        assertNull("no writer may fail: " + failure.get(), failure.get());

        Map<String, String> finalFields = store.read("idx").fields();
        assertEquals("every concurrently inferred field must survive the merge, none may be overwritten", writers, finalFields.size());
        for (int i = 0; i < writers; i++) {
            assertTrue("field-" + i + " was lost, so a document that introduced it is unqueryable", finalFields.containsKey("field-" + i));
        }
    }

    /**
     * The control that the retry loop is actually exercised. If the writers never collided this test
     * would pass against an implementation with no retry at all, so the collision itself is asserted.
     */
    public void testConcurrentWritersActuallyCollide() throws Exception {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        int writers = 8;
        CountDownLatch startTogether = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(writers);

        for (int i = 0; i < writers; i++) {
            final String field = "collide-" + i;
            new Thread(() -> {
                try {
                    startTogether.await();
                    MappingGenerationStore.updateMapping("idx", Map.of(field, "keyword"));
                } catch (Exception e) {
                    // counted by the store, not here
                } finally {
                    finished.countDown();
                }
            }).start();
        }

        startTogether.countDown();
        assertTrue(finished.await(30, TimeUnit.SECONDS));

        assertTrue(
            "the writers must have contended, or this suite is not testing the retry loop at all: "
                + store.failedSwaps.get()
                + " failed swaps",
            store.failedSwaps.get() > 0
        );
    }

    /** Two shards inferring the same field from the same data converge without a swap at all. */
    public void testTheSameFieldInferredTwiceDoesNotAdvanceTheGeneration() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        long first = MappingGenerationStore.updateMapping("idx", Map.of("age", "long"));
        long second = MappingGenerationStore.updateMapping("idx", Map.of("age", "long"));

        assertEquals("the first inference must create a generation", 1L, first);
        assertEquals("an identical inference must converge rather than advance the generation", first, second);
        assertEquals("and must not have written anything", 1, store.swaps.get());
    }

    /**
     * A genuine disagreement must fail rather than being resolved by whoever swaps last. This is an
     * error today, raised by the cluster manager because it serialises the two updates; here it has to
     * surface at the merge instead.
     */
    public void testAConflictingTypeFails() {
        MappingGenerationStore.register(new InMemoryStore());

        MappingGenerationStore.updateMapping("idx", Map.of("age", "long"));

        MappingGenerationStore.MappingConflictException conflict = expectThrows(
            MappingGenerationStore.MappingConflictException.class,
            () -> MappingGenerationStore.updateMapping("idx", Map.of("age", "float"))
        );

        assertTrue("the message must name the field and both types: " + conflict.getMessage(), conflict.getMessage().contains("age"));
        assertTrue(conflict.getMessage().contains("long") && conflict.getMessage().contains("float"));
    }

    /** With nothing installed, callers get a sentinel rather than an exception, and nothing is written. */
    public void testWithoutAStoreNothingHappens() {
        assertEquals(-1L, MappingGenerationStore.updateMapping("idx", Map.of("age", "long")));
        assertEquals(-1L, MappingGenerationStore.currentGeneration("idx"));
    }

    // ---------------------------------------------------------------- helpers

    /**
     * A store that swaps under a lock, counting both successful and failed attempts. The counts are what
     * let a test assert the retry loop ran rather than assuming it.
     */
    private static final class InMemoryStore implements MappingGenerationStore.Store {
        private final Map<String, MappingGenerationStore.MappingGeneration> byUuid = new ConcurrentHashMap<>();
        private final AtomicInteger swaps = new AtomicInteger();
        private final AtomicInteger failedSwaps = new AtomicInteger();

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            return byUuid.get(indexUuid);
        }

        @Override
        public synchronized boolean compareAndSwap(
            String indexUuid,
            long expectedGeneration,
            MappingGenerationStore.MappingGeneration updated
        ) {
            MappingGenerationStore.MappingGeneration current = byUuid.get(indexUuid);
            long currentGeneration = current == null ? 0L : current.generation();
            if (currentGeneration != expectedGeneration) {
                failedSwaps.incrementAndGet();
                return false;
            }
            byUuid.put(indexUuid, new MappingGenerationStore.MappingGeneration(updated.generation(), new HashMap<>(updated.fields())));
            swaps.incrementAndGet();
            return true;
        }
    }
}
