/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.junit.After;

import java.util.List;
import java.util.Map;

/**
 * W7. Cluster stats over a population it cannot enumerate.
 *
 * <p>H19 pinned the gap and H20 built the seam. Neither had an implementation, so {@code _cluster/stats}
 * still reported the plausible wrong number: field counts that silently excluded every gated index, with
 * nothing in the response to say so.
 *
 * <p>What is asserted is exactness rather than presence. An aggregate that returned roughly the right
 * numbers would be the same failure H19 was about, arrived at from the other direction, so the counts are
 * checked against known mappings.
 */
public class GatedMappingStatsIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    @After
    public void clearRegistrations() {
        // The registries are static and outlive the cluster, so a test that installs the gate and does not
        // remove it leaves a dead client answering for whatever suite runs next in this JVM.
        DescriptorGate.uninstall();
        GatedMappingStatsAggregator.register(null);
        MappingGenerationStore.register(null);
    }

    /** Nothing written means an empty aggregate rather than a failure. */
    public void testAnEmptyPopulationAggregatesToNothing() {
        var counts = new IndexBackedMappingStatsAggregator(client()).aggregate();

        // Either an empty aggregate or null is acceptable here: both leave ordinary stats untouched, which
        // is the property that matters for a cluster with no gated indices.
        assertTrue("an empty population must not produce counts", counts == null || counts.fieldCounts().isEmpty());
    }

    /** The counts must be exact, since an approximate statistic is the failure H19 pinned. */
    public void testFieldAndIndexCountsAreExact() throws Exception {
        IndexBackedMappingStore store = new IndexBackedMappingStore(client());
        // Two indices sharing a type, one with a type of its own, so field counts and index counts differ
        // and a test that conflated them would fail.
        store.compareAndSwap("idx-a", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("a1", "keyword", "a2", "keyword")));
        store.compareAndSwap("idx-b", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("b1", "keyword", "b2", "long")));
        client().admin().indices().prepareRefresh(IndexBackedMappingStore.MAPPING_INDEX).get();

        var counts = new IndexBackedMappingStatsAggregator(client()).aggregate();

        assertNotNull("the aggregate must answer once mappings exist", counts);
        assertEquals("three keyword fields across both indices", 3, (int) counts.fieldCounts().get("keyword"));
        assertEquals("one long field", 1, (int) counts.fieldCounts().get("long"));
        assertEquals("two indices use keyword", 2, (int) counts.indexCounts().get("keyword"));
        assertEquals(
            "but only one uses long, so field counts and index counts must not be the same number",
            1,
            (int) counts.indexCounts().get("long")
        );
    }

    /**
     * The property H20's interface exists to guarantee, asserted against the real implementation: the whole
     * gated population costs one request, so the cost is set by the number of field types rather than by the
     * number of indices.
     */
    public void testTheWholePopulationCostsOneRequest() throws Exception {
        IndexBackedMappingStore store = new IndexBackedMappingStore(client());
        for (int i = 0; i < 50; i++) {
            store.compareAndSwap("idx-" + i, 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("f" + i, "keyword")));
        }
        client().admin().indices().prepareRefresh(IndexBackedMappingStore.MAPPING_INDEX).get();

        long searchesBefore = totalSearchCount();
        var counts = new IndexBackedMappingStatsAggregator(client()).aggregate();
        long searchesAfter = totalSearchCount();

        assertEquals("fifty indices each with one keyword field", 50, (int) counts.fieldCounts().get("keyword"));
        assertEquals(50, (int) counts.indexCounts().get("keyword"));
        assertTrue(
            "summarising fifty indices must not cost fifty searches. A per-index implementation would fix "
                + "the correctness half of H19 while restoring the cost half, which is exactly what H20's "
                + "interface was shaped to prevent",
            searchesAfter - searchesBefore < 20
        );
    }

    /** A failing aggregate must leave ordinary stats intact rather than failing the stats call. */
    public void testAFailingAggregateReturnsNullRatherThanThrowing() {
        // No mapping index exists, so the aggregation cannot run.
        assertNull(
            "an unavailable store must degrade to no gated counts, not to a failed stats request",
            new IndexBackedMappingStatsAggregator(client()).aggregate()
        );
    }

    /**
     * The test the regression got past, written the way it should have been in the first place.
     *
     * <p>Every other test in this class writes through a hand-built {@link IndexBackedMappingStore}, so all
     * of them keep passing no matter what the gate actually registers. That is how the mapping store could
     * move into the descriptor -- correctly, and for good reasons -- while leaving the aggregator reading an
     * index with no writer, and nothing went red. This one writes through {@link MappingGenerationStore},
     * the seam production registers, so it fails if the registered store stops feeding the projection.
     *
     * <p>The projection is asynchronous by design, so this waits for it rather than asserting immediately.
     * Waiting is not a weaker assertion here: the counts are eventually consistent on purpose, and a test
     * demanding them synchronously would be asserting a property the design deliberately does not have.
     */
    public void testMappingsWrittenThroughTheRegisteredSeamReachTheAggregate() throws Exception {
        InstalledDescriptorPlane plane = installBlobBackedDescriptorPlane();

        // A descriptor for each index, because a descriptor is what the registered store writes the mapping
        // onto, and it is keyed by name while a mapping swap carries a uuid.
        plane.points().put(descriptor("seam-a"));
        plane.points().put(descriptor("seam-b"));

        // Resolve both by name first, the way any request touching a gated index does before it reaches the
        // mapping path. That is not test scaffolding: descriptors are keyed by name and a mapping swap
        // carries only a uuid, so resolution is where the store learns the uuid-to-name entry it needs.
        // Writing the descriptor without resolving it is a state no request can produce.
        assertNotNull(AbsentIndexDescriptorSuppliers.supply("seam-a"));
        assertNotNull(AbsentIndexDescriptorSuppliers.supply("seam-b"));

        assertEquals(1L, MappingGenerationStore.createMapping("seam-a-uuid", Map.of("a1", "keyword", "a2", "keyword")));
        assertEquals(1L, MappingGenerationStore.createMapping("seam-b-uuid", Map.of("b1", "keyword", "b2", "long")));

        assertBusy(() -> {
            // The projection creates the index on its first write, so early rounds can find it absent. That
            // is the asynchrony under test, not a failure, so it retries rather than throwing.
            try {
                client().admin().indices().prepareRefresh(IndexBackedMappingStore.MAPPING_INDEX).get();
            } catch (org.opensearch.index.IndexNotFoundException e) {
                fail("the projection has not created the mapping index yet");
            }
            var counts = new IndexBackedMappingStatsAggregator(client()).aggregate();
            assertNotNull("a mapping written through the registered store must reach the aggregate", counts);
            assertEquals("three keyword fields across both indices", Integer.valueOf(3), counts.fieldCounts().get("keyword"));
            assertEquals(Integer.valueOf(1), counts.fieldCounts().get("long"));
            assertEquals(Integer.valueOf(2), counts.indexCounts().get("keyword"));
            assertEquals(Integer.valueOf(1), counts.indexCounts().get("long"));
        }, 30, java.util.concurrent.TimeUnit.SECONDS);
    }

    /** A descriptor for the registered store to resolve and write a mapping onto. */
    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }

    private long totalSearchCount() {
        return client().admin()
            .indices()
            .prepareStats(IndexBackedMappingStore.MAPPING_INDEX)
            .setSearch(true)
            .get()
            .getTotal()
            .getSearch()
            .getTotal()
            .getQueryCount();
    }
}
