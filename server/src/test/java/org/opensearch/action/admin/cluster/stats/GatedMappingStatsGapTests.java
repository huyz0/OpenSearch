/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * H19. What cluster stats reports for a population it cannot see.
 *
 * <p>{@code MappingStats.of} walks {@code state.metadata()} building field usage counts across every
 * index. That is the same pair of problems H16 found in pagination, and the pairing is not a coincidence:
 * both are enumerations that were correct while every index lived in cluster state.
 *
 * <p><b>Cost.</b> The walk is over the whole population, so at a hundred million indices the walk is the
 * request rather than an overhead on it.
 *
 * <p><b>Correctness, and here it is worse than pagination.</b> A gated index is not in
 * {@code state.metadata()}, so its fields are missing from the counts. A paginated listing that omits an
 * index at least returns something a careful caller could notice; a statistic that omits it returns a
 * plausible number that is simply wrong, and nothing about the response distinguishes it from a correct
 * one. This is the tenth instance of the area's signature failure and the least visible of them.
 *
 * <p><b>The fix follows from the architecture rather than needing a product decision</b>, which is worth
 * saying because the previous two gaps were both mislabelled that way. A gated population's mapping stats
 * have to come from an aggregate over {@code MappingGenerationStore}, since per-index enumeration is
 * precisely what Area H exists to remove and no amount of making the enumeration faster changes that.
 *
 * <p><b>Closed by H20.</b> {@code GatedMappingStatsAggregator} returns the whole gated population's counts
 * in one call and offers no way to iterate indices, so the naive repair that would have fixed correctness
 * while restoring the population-sized cost is not expressible through the interface.
 */
public class GatedMappingStatsGapTests extends OpenSearchTestCase {

    @org.junit.After
    public void clearAggregator() {
        GatedMappingStatsAggregator.register(null);
    }

    /** With nothing registered, stats are unchanged, so an ungated cluster is untouched. */
    public void testWithoutAnAggregatorOnlyClusterStateIndicesContribute() throws Exception {
        // One ordinary index with a mapping, standing in for a cluster whose gated indices contribute
        // nothing because they are not in metadata at all.
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ordinaryIndexWithKeywordField("ordinary"), false).build())
            .build();

        MappingStats stats = MappingStats.of(state);

        long keywordCount = stats.getFieldTypeStats()
            .stream()
            .filter(each -> "keyword".equals(each.getName()))
            .mapToLong(each -> each.getCount())
            .sum();

        assertEquals("only the ordinary index contributes", 1L, keywordCount);
    }

    /** The gap H19 pinned, now closed. Gated field types appear in the counts. */
    public void testGatedFieldTypesAppearInMappingStats() throws Exception {
        GatedMappingStatsAggregator.register(
            () -> new GatedMappingStatsAggregator.GatedFieldTypeCounts(
                java.util.Map.of("keyword", 40, "long", 7),
                java.util.Map.of("keyword", 30, "long", 5)
            )
        );
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ordinaryIndexWithKeywordField("ordinary"), false).build())
            .build();

        MappingStats stats = MappingStats.of(state);

        assertEquals("the ordinary keyword field plus forty gated ones", 41L, countOf(stats, "keyword"));
        assertEquals("and a type only gated indices use must appear at all", 7L, countOf(stats, "long"));
        assertEquals(
            "index counts must fold in too, or the per-type index count silently excludes the gated population",
            31,
            stats.getFieldTypeStats().stream().filter(each -> "keyword".equals(each.getName())).findFirst().orElseThrow().getIndexCount()
        );
    }

    /**
     * The property that rules out the repair H19 warned against. The aggregate is consulted once, and the
     * count does not change with the size of the population being summarised, because the interface has no
     * way to express per-index iteration.
     */
    public void testTheAggregateIsConsultedOnceRegardlessOfPopulation() throws Exception {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        GatedMappingStatsAggregator.register(() -> {
            calls.incrementAndGet();
            return new GatedMappingStatsAggregator.GatedFieldTypeCounts(
                java.util.Map.of("keyword", 1_000_000),
                java.util.Map.of("keyword", 1_000_000)
            );
        });

        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < 200; i++) {
            metadata.put(ordinaryIndexWithKeywordField("idx-" + i), false);
        }
        MappingStats.of(ClusterState.builder(ClusterName.DEFAULT).metadata(metadata.build()).build());

        assertEquals(
            "the gated population must cost one aggregate call, not one per index. A per-index view would "
                + "fix the correctness half of H19 while restoring the cost half",
            1,
            calls.get()
        );
    }

    /** A failing aggregate leaves ordinary stats intact rather than failing the whole stats call. */
    public void testAFailingAggregateLeavesOrdinaryStatsIntact() throws Exception {
        GatedMappingStatsAggregator.register(() -> { throw new IllegalStateException("descriptor index down"); });
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(ordinaryIndexWithKeywordField("ordinary"), false).build())
            .build();

        assertEquals("the ordinary index must still be counted", 1L, countOf(MappingStats.of(state), "keyword"));
    }

    private static long countOf(MappingStats stats, String type) {
        return stats.getFieldTypeStats().stream().filter(each -> type.equals(each.getName())).mapToLong(each -> each.getCount()).sum();
    }

    /**
     * The cost half. The work is set by the population, so a cluster with more indices pays more for the
     * same call, which is the shape that does not survive a hundred million of them.
     */
    public void testTheWalkIsSetByThePopulation() throws Exception {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < 200; i++) {
            metadata.put(ordinaryIndexWithKeywordField("idx-" + i), false);
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata.build()).build();

        MappingStats stats = MappingStats.of(state);

        long keywordCount = stats.getFieldTypeStats()
            .stream()
            .filter(each -> "keyword".equals(each.getName()))
            .mapToLong(each -> each.getCount())
            .sum();

        assertEquals(
            "every index in the population is visited to answer one stats call, so the cost is the " + "population rather than the answer",
            200L,
            keywordCount
        );
    }

    private static IndexMetadata ordinaryIndexWithKeywordField(String name) throws java.io.IOException {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putMapping("{\"properties\":{\"field\":{\"type\":\"keyword\"}}}")
            .build();
    }
}
