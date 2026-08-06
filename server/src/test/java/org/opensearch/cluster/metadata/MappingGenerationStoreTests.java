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
import java.util.Set;
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

        Map<String, Object> finalFields = store.read("idx").fields();
        assertEquals("every concurrently inferred field must survive the merge, none may be overwritten", writers, finalFields.size());
        for (int i = 0; i < writers; i++) {
            assertTrue("field-" + i + " was lost, so a document that introduced it is unqueryable", finalFields.containsKey("field-" + i));
        }
    }

    /**
     * The control that the retry loop is actually exercised. If the writers never collided this test
     * would pass against an implementation with no retry at all, so the collision itself is asserted.
     *
     * <p><b>The collision is arranged, not hoped for, and it took a failure to get there.</b> The first
     * version started eight threads from a common latch and asserted afterwards that some swap had failed.
     * That is a race about a race: nothing stops the scheduler running the threads far enough apart that
     * each reads a generation the previous one already wrote, and then every swap succeeds and the
     * assertion fails having found nothing wrong. It survived in isolation and fell over inside a full run
     * of 8,808 tests, which is exactly the load where thread starts spread out.
     *
     * <p>So the store now holds every reader until all of them have read. All eight leave with generation
     * zero, seven swaps must fail, and the assertion below tests the retry loop instead of testing the
     * scheduler's mood.
     */
    public void testConcurrentWritersActuallyCollide() throws Exception {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        int writers = 8;
        store.readsBeforeAnySwap = new CountDownLatch(writers);
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

        // At least writers - 1, not merely more than zero. Every writer left the gate holding generation
        // zero, so exactly one swap can land and the rest have to come back. Asserting the number the
        // arrangement guarantees is what keeps this a control: "more than zero" would pass again if the
        // gate were removed and one accidental collision happened to occur.
        assertTrue(
            "the writers must have contended, or this suite is not testing the retry loop at all: "
                + store.failedSwaps.get()
                + " failed swaps",
            store.failedSwaps.get() >= writers - 1
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
        assertEquals(-1L, MappingGenerationStore.createMapping("idx", Map.of("age", "long")));
    }

    /**
     * A creation does not read a mapping that cannot exist.
     *
     * <p>T41, and the assertion is the read count rather than a duration. The UUID is generated during the
     * creation and has never been given to anyone, so the read {@code updateMapping} opens with can only
     * answer "absent" -- one blocking round trip per creation, against the store T40 measured at 83% or more
     * of what a declared mapping costs.
     */
    public void testCreatingAMappingDoesNotReadOneThatCannotExist() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        long generation = MappingGenerationStore.createMapping("idx", Map.of("tenant", Map.of("type", "keyword")));

        assertEquals("a create writes the first generation", 1L, generation);
        assertEquals("a fresh UUID cannot have a stored mapping, so nothing is worth reading", 0, store.reads.get());
        assertEquals("one swap, and only one", 1, store.swaps.get());
        assertEquals(Map.of("type", "keyword"), store.byUuid.get("idx").definitionOf("tenant"));
    }

    /**
     * A create whose swap is refused merges rather than failing or overwriting.
     *
     * <p>The case is a retried creation task: it carries the same {@code IndexMetadata}, so the same UUID,
     * and finds generation 1 already there. The fields already stored must survive, because the only thing
     * that can have written them is an earlier attempt at this same index, and dropping them would leave an
     * index whose declared fields are missing -- the silent loss T13 found, arriving by a different route.
     */
    public void testACreateThatLosesTheSwapMergesRatherThanOverwriting() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);
        // Whatever got there first. A retry of this creation writes exactly the declared fields; this test
        // uses a different field so that a merge and an overwrite give visibly different answers.
        MappingGenerationStore.updateMapping("idx", Map.of("city", "keyword"));

        long generation = MappingGenerationStore.createMapping("idx", Map.of("age", "long"));

        assertEquals("the merge advances past what was already stored", 2L, generation);
        assertEquals(
            "the field that was already there must survive the create",
            Set.of("city", "age"),
            store.byUuid.get("idx").fields().keySet()
        );
        assertTrue("the optimistic swap must have been refused, or this proves nothing", store.failedSwaps.get() >= 1);
    }

    /**
     * A create whose write landed but was reported as a conflict converges without writing again.
     *
     * <p>The reachable shape of a refused create: the store's own {@code client.index} is retried underneath
     * it after the first attempt already landed, so the second comes back as a version conflict. The
     * creation has in fact succeeded, and the fallback has to notice that rather than fail it.
     *
     * <p>Asserted on the counts, not only on the outcome. Returning the right generation is something a
     * plain {@code updateMapping} delegation also does, so an outcome-only version of this test would pass
     * against the implementation T41 replaced. One swap and one read is what says the optimistic path ran
     * and then deferred.
     */
    public void testACreateWhoseWriteAlreadyLandedConvergesWithoutWritingAgain() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);
        Map<String, Object> declared = Map.of("tenant", Map.of("type", "keyword"));
        assertEquals(1L, MappingGenerationStore.createMapping("idx", declared));

        long generation = MappingGenerationStore.createMapping("idx", declared);

        assertEquals("the second attempt converges on the generation already stored", 1L, generation);
        assertEquals(Set.of("tenant"), store.byUuid.get("idx").fields().keySet());
        assertEquals("one write for the first create, and none for the second", 1, store.swaps.get());
        assertEquals("the first create must not have read, and the second must have read once", 1, store.reads.get());
    }

    /** Nothing declared means nothing written, the same answer updateMapping gives for an empty map. */
    public void testCreatingWithNothingDeclaredWritesNothing() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        assertEquals(0L, MappingGenerationStore.createMapping("idx", Map.of()));

        assertEquals("an empty declaration must not leave a document behind", 0, store.swaps.get());
        assertNull(store.byUuid.get("idx"));
    }

    /** Removing a mapping that was never written is not an error, since the caller cannot know. */
    public void testDeletingAMappingThatWasNeverWrittenIsNotAnError() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);

        MappingGenerationStore.deleteMapping("never-written");

        assertNull(store.read("never-written"));
    }

    /** With no store installed, deleting is a no-op rather than an exception, like every other entry. */
    public void testDeletingWithoutAStoreDoesNothing() {
        MappingGenerationStore.deleteMapping("idx");
    }

    /** A delete removes the mapping, so a later read sees nothing rather than a stale generation. */
    public void testDeletingRemovesTheMapping() {
        InMemoryStore store = new InMemoryStore();
        MappingGenerationStore.register(store);
        MappingGenerationStore.updateMapping("idx", Map.of("age", "long"));

        MappingGenerationStore.deleteMapping("idx");

        assertNull("a deleted mapping must not be readable, or nothing was pruned", store.read("idx"));
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
        /** Counted so a caller can be held to not issuing a read whose answer it already knows. */
        private final AtomicInteger reads = new AtomicInteger();

        /**
         * Holds every reader until all of them have read, so a collision is arranged rather than hoped for.
         *
         * <p>Null unless a test sets it, because only the collision test needs it. When set, each reader
         * takes its value, counts down, and waits, so all of them leave holding the same generation and all
         * but one swap must then fail.
         *
         * <p><b>Self-clearing, which is what lets the retry loop still run.</b> Once the count reaches zero
         * the latch stays open: {@code await} returns at once and {@code countDown} is a no-op, so the
         * re-reads the retry loop performs pass straight through. A {@code CyclicBarrier} would deadlock
         * here instead, because only the losers come back round and the party count would never be met.
         */
        private volatile CountDownLatch readsBeforeAnySwap;

        @Override
        public MappingGenerationStore.MappingGeneration read(String indexUuid) {
            reads.incrementAndGet();
            MappingGenerationStore.MappingGeneration value = byUuid.get(indexUuid);
            CountDownLatch gate = readsBeforeAnySwap;
            if (gate != null) {
                gate.countDown();
                try {
                    if (gate.await(30, TimeUnit.SECONDS) == false) {
                        throw new AssertionError("the readers never all arrived, so no collision was arranged");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while waiting for the other readers", e);
                }
            }
            return value;
        }

        @Override
        public void delete(String indexUuid) {
            byUuid.remove(indexUuid);
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
