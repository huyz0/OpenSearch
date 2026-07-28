/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * H6c. Whether a shard can catch up on a mapping change without being told about one.
 *
 * <p>This is what makes informing a hundred shards cost nothing: nobody is informed. The coordinator
 * stamps a request with the generation it planned against, and a shard behind that generation reads the
 * mapping before executing.
 *
 * <p>Two things have to hold, and the second is the one that decides whether the design is worth having.
 * A shard behind the stamp must catch up, or a query on a newly mapped field reports it as unmapped. And
 * a shard already current must do <em>no</em> read at all, because a refresh on every request would
 * replace a broadcast with something worse. The second is asserted by counting fetches rather than by
 * inspecting the mapping, since the right mapping comes back either way.
 */
public class MappingRefreshOnDemandTests extends OpenSearchTestCase {

    /** A shard behind the stamped generation must catch up rather than serve a stale field set. */
    public void testAShardBehindTheStampRefreshes() {
        CountingLoader loader = new CountingLoader(Map.of("age", "long"), 5L);
        MappingRefreshOnDemand refresher = new MappingRefreshOnDemand(loader);

        MappingGenerationStore.MappingGeneration mapping = refresher.ensureCurrent("idx", 5L);

        assertNotNull("a shard behind the stamp must end up with a mapping", mapping);
        assertEquals("and at the generation the coordinator planned against", 5L, mapping.generation());
        assertTrue("carrying the field it had not seen", mapping.fields().containsKey("age"));
        assertEquals("which costs exactly one read", 1, loader.loads.get());
    }

    /**
     * The measurement that decides the design. A shard already current must not read at all, or this is a
     * refresh per request rather than a refresh per change.
     */
    public void testAShardAlreadyCurrentDoesNotRead() {
        CountingLoader loader = new CountingLoader(Map.of("age", "long"), 5L);
        MappingRefreshOnDemand refresher = new MappingRefreshOnDemand(loader);

        refresher.ensureCurrent("idx", 5L);
        int afterFirst = loader.loads.get();

        for (int request = 0; request < 100; request++) {
            refresher.ensureCurrent("idx", 5L);
        }

        assertEquals("a hundred further requests at the same generation must cost no reads", afterFirst, loader.loads.get());
        assertEquals("and the counter agrees", 1L, refresher.fetchCount());
    }

    /**
     * Being ahead is normal, not an error: the shard that inferred the field swapped the mapping itself,
     * so it sits at N while a coordinator that planned a moment earlier stamps N-1.
     */
    public void testAShardAheadOfTheStampDoesNotRead() {
        CountingLoader loader = new CountingLoader(Map.of("age", "long"), 9L);
        MappingRefreshOnDemand refresher = new MappingRefreshOnDemand(loader);
        refresher.record("idx", new MappingGenerationStore.MappingGeneration(9L, Map.of("age", "long")));

        refresher.ensureCurrent("idx", 7L);

        assertEquals("a shard ahead of the stamp must not read", 0, loader.loads.get());
    }

    /**
     * A loader that cannot answer must leave the caller with what it had rather than with an empty
     * mapping. Returning empty would report every field as unmapped, which is a wrong answer that looks
     * like a valid one.
     */
    public void testAFailedLoadDoesNotEraseWhatTheShardHad() {
        MappingRefreshOnDemand refresher = new MappingRefreshOnDemand((uuid, generation) -> null);
        refresher.record("idx", new MappingGenerationStore.MappingGeneration(3L, Map.of("age", "long")));

        MappingGenerationStore.MappingGeneration mapping = refresher.ensureCurrent("idx", 8L);

        assertNotNull("a failed load must not erase the mapping the shard already had", mapping);
        assertTrue("which still carries its fields", mapping.fields().containsKey("age"));
    }

    /**
     * Concurrent refreshes must not move the shard backwards. Two requests in flight can load different
     * generations, and taking the older would undo a newer one that already landed.
     */
    public void testConcurrentRefreshesDoNotGoBackwards() throws Exception {
        MappingRefreshOnDemand refresher = new MappingRefreshOnDemand(
            (uuid, generation) -> new MappingGenerationStore.MappingGeneration(generation, Map.of("field-" + generation, "keyword"))
        );

        int threads = 8;
        CountDownLatch startTogether = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            final long generation = i + 1;
            new Thread(() -> {
                try {
                    startTogether.await();
                    refresher.ensureCurrent("idx", generation);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            }).start();
        }
        startTogether.countDown();
        assertTrue(finished.await(30, TimeUnit.SECONDS));

        assertEquals(
            "the shard must settle at the highest generation any refresh loaded, never an earlier one",
            8L,
            refresher.localMapping("idx").generation()
        );
    }

    // ---------------------------------------------------------------- helpers

    /** Counts loads, which is the only way to tell "did not need to read" from "read and got the same". */
    private static final class CountingLoader implements MappingRefreshOnDemand.MappingLoader {
        private final Map<String, String> fields;
        private final long generation;
        private final AtomicInteger loads = new AtomicInteger();

        CountingLoader(Map<String, String> fields, long generation) {
            this.fields = fields;
            this.generation = generation;
        }

        @Override
        public MappingGenerationStore.MappingGeneration load(String indexUuid, long atLeastGeneration) {
            loads.incrementAndGet();
            return new MappingGenerationStore.MappingGeneration(generation, fields);
        }
    }
}
