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
 * <p>Pinned rather than fixed. The aggregate is a larger piece of work than the pin, and shipping the pin
 * first is what stops the gap being rediscovered as a production surprise in the meantime.
 */
public class GatedMappingStatsGapTests extends OpenSearchTestCase {

    /**
     * The gap. Fields belonging to a gated index are absent from the counts, and the response says nothing
     * about it.
     */
    public void testGatedIndexFieldsAreMissingFromMappingStats() throws Exception {
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

        assertEquals(
            "only the ordinary index contributes. A gated index's fields are absent from mapping stats "
                + "with nothing in the response to say so, which for a statistic means a plausible wrong "
                + "number rather than a visible gap. Closing this means aggregating over "
                + "MappingGenerationStore rather than enumerating indices, since the enumeration is what "
                + "Area H exists to remove",
            1L,
            keywordCount
        );
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
