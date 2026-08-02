/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * P3. How many times an unknown field makes this node read the mapping store.
 *
 * <p>W15 shipped a test called {@code testASecondUnknownFieldAtTheSameGenerationDoesNotRefetch} that
 * checked a boolean and a map size and never counted a single read. The name claimed a property the test
 * could not observe, and the implementation did not have it: {@code currentMapping} was called <em>before</em>
 * the cache guard, so every unknown field went to the store even when this shard was already current. A
 * document with ten new fields cost ten reads, which is exactly the cost H6c argued that pulling avoids.
 *
 * <p>Counting is the only way to tell "did not need to read" from "read and got the same answer". These
 * tests count.
 */
public class FieldRefresherReadCountTests extends OpenSearchTestCase {

    private final AtomicInteger reads = new AtomicInteger();
    private final AtomicLong now = new AtomicLong(1_000_000_000L);

    @After
    public void clearRegistration() {
        MappingGenerationStore.register(null);
    }

    /** A store that counts how often it is asked, which is the whole instrument. */
    private void registerCountingStore(Map<String, String> fields) {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
            @Override
            public MappingGenerationStore.MappingGeneration read(String indexUuid) {
                reads.incrementAndGet();
                return new MappingGenerationStore.MappingGeneration(1L, fields);
            }

            @Override
            public boolean compareAndSwap(String indexUuid, long expected, MappingGenerationStore.MappingGeneration updated) {
                return true;
            }
        });
    }

    private StoreBackedFieldRefresher refresher() {
        return new StoreBackedFieldRefresher(1_000, now::get);
    }

    /**
     * A stand-in MapperService, needed only because the refresher declines a null one before doing
     * anything. The merge it receives is irrelevant here: what is being counted is store reads.
     */
    private static MapperService mapperService() {
        return org.mockito.Mockito.mock(MapperService.class);
    }

    /**
     * The defect P3 found. Ten unknown fields on a shard that has already looked must not cost ten reads.
     */
    public void testManyUnknownFieldsCostOneRead() {
        registerCountingStore(Map.of("known", "keyword"));
        StoreBackedFieldRefresher refresher = refresher();

        for (int i = 0; i < 10; i++) {
            refresher.refresh(mapperService(), "idx-uuid", "unknown-" + i);
        }

        assertEquals(
            "a document with ten new fields must cost one look at the store, not ten. Before the guard was "
                + "moved ahead of the read this was ten, and the test that claimed otherwise never counted",
            1,
            reads.get()
        );
    }

    /** The first miss does read, or the refresher would never discover anything. */
    public void testTheFirstMissDoesRead() {
        registerCountingStore(Map.of("known", "keyword"));

        refresher().refresh(mapperService(), "idx-uuid", "anything");

        assertEquals("the first miss must go to the store", 1, reads.get());
    }

    /**
     * Staleness is bounded by the window rather than unbounded. Once it passes, the next unknown field
     * looks again, which is how a field another shard inferred becomes visible here.
     */
    public void testTheWindowExpiresSoNewFieldsAreEventuallySeen() {
        registerCountingStore(Map.of("known", "keyword"));
        StoreBackedFieldRefresher refresher = refresher();

        refresher.refresh(mapperService(), "idx-uuid", "first");
        assertEquals(1, reads.get());

        refresher.refresh(mapperService(), "idx-uuid", "second");
        assertEquals("still inside the window", 1, reads.get());

        now.addAndGet(StoreBackedFieldRefresher.RECHECK_WINDOW_NANOS + 1);
        refresher.refresh(mapperService(), "idx-uuid", "third");

        assertEquals("past the window, it must look again or a shard could never catch up", 2, reads.get());
    }

    /** Separate indices do not share a window, or one busy index would blind the others. */
    public void testTheWindowIsPerIndex() {
        registerCountingStore(Map.of("known", "keyword"));
        StoreBackedFieldRefresher refresher = refresher();

        refresher.refresh(mapperService(), "idx-a", "field");
        refresher.refresh(mapperService(), "idx-b", "field");

        assertEquals("each index must get its own first look", 2, reads.get());
    }

    /** With no store registered nothing is read, so an ordinary index pays nothing. */
    public void testNoStoreMeansNoReads() {
        refresher().refresh(mapperService(), "idx-uuid", "field");

        assertEquals(0, reads.get());
    }
}
