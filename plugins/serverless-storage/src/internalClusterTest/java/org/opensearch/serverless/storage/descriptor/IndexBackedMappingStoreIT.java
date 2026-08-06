/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * W5. Mappings stored outside cluster state, on a real node.
 *
 * <p>H4c established that mappings must leave cluster state or H3 and H5 are unusable, and H6a proved the
 * compare-and-swap converges under concurrent field inference against an in-memory stub. No backing store
 * ever existed, so {@code MappingGenerationStore} had no registration in production and the whole
 * no-broadcast design was proven and inert.
 *
 * <p>The swap is expressed as external versioning, so the generation is the thing the write is conditioned
 * on. What that has to buy is asserted here rather than assumed: a losing writer must be told it lost
 * rather than silently overwriting, because H6a's convergence argument depends on the loser re-reading and
 * merging.
 */
public class IndexBackedMappingStoreIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    @After
    public void clearRegistration() {
        MappingGenerationStore.register(null);
    }

    private IndexBackedMappingStore store() {
        return new IndexBackedMappingStore(client());
    }

    /** An index with no mapping yet reads as null rather than as an empty mapping. */
    public void testAnUnknownIndexReadsAsNull() {
        assertNull("no mapping object yet must be null, not an empty mapping", store().read("never-written-uuid"));
    }

    /** The basic swap, and the generation the read returns must be the one that was written. */
    public void testAMappingCanBeWrittenAndReadBack() {
        IndexBackedMappingStore store = store();

        assertTrue(store.compareAndSwap("idx-uuid", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("message", "text"))));

        MappingGenerationStore.MappingGeneration read = store.read("idx-uuid");
        assertNotNull(read);
        assertEquals("the generation must survive the round trip, since the swap is conditioned on it", 1L, read.generation());
        assertEquals(Map.of("message", "text"), read.fields());
    }

    /**
     * The property the whole design rests on. A second writer at the same generation must be told it lost,
     * because H6a's convergence depends on the loser re-reading and merging rather than overwriting.
     */
    public void testASecondWriterAtTheSameGenerationLoses() {
        IndexBackedMappingStore store = store();
        assertTrue(store.compareAndSwap("contested", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("a", "keyword"))));

        boolean secondWon = store.compareAndSwap("contested", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("b", "keyword")));

        assertFalse("a losing swap must report the loss rather than overwrite the winner", secondWon);
        assertEquals("and the winner's fields must survive intact", Map.of("a", "keyword"), store.read("contested").fields());
    }

    /** Advancing from the current generation succeeds, which is what the retry loop does after losing. */
    public void testAdvancingFromTheCurrentGenerationSucceeds() {
        IndexBackedMappingStore store = store();
        store.compareAndSwap("advancing", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("a", "keyword")));

        assertTrue(
            "a writer that re-read and merged must be able to advance",
            store.compareAndSwap("advancing", 1L, new MappingGenerationStore.MappingGeneration(2L, Map.of("a", "keyword", "b", "long")))
        );
        assertEquals(2L, store.read("advancing").generation());
    }

    /**
     * H6a's convergence, now against real storage rather than a stub. Every concurrently inferred field
     * must survive, which is the claim that makes dynamic mapping safe without informing any shard.
     */
    public void testConcurrentInferencesAllConverge() throws Exception {
        MappingGenerationStore.register(store());
        int writers = 8;
        CountDownLatch startTogether = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(writers);
        AtomicInteger failures = new AtomicInteger();

        for (int i = 0; i < writers; i++) {
            String field = "field-" + i;
            new Thread(() -> {
                try {
                    startTogether.await();
                    MappingGenerationStore.updateMapping("converging", Map.of(field, "keyword"));
                } catch (Exception e) {
                    failures.incrementAndGet();
                } finally {
                    done.countDown();
                }
            }).start();
        }
        startTogether.countDown();
        assertTrue("every writer must finish", done.await(2, TimeUnit.MINUTES));
        assertEquals("no writer may fail outright", 0, failures.get());

        Map<String, Object> fields = store().read("converging").fields();
        assertEquals(
            "every concurrently inferred field must survive, or a document is accepted and its field is "
                + "silently lost, which is worse than rejecting the write",
            writers,
            fields.size()
        );
        for (int i = 0; i < writers; i++) {
            assertTrue("field-" + i + " must have survived", fields.containsKey("field-" + i));
        }
    }
}
