/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Field type counts for the gated population, as one aggregate rather than an enumeration.
 *
 * <p>H19 recorded the gap this closes: {@code MappingStats.of} walks {@code state.metadata()}, so a gated
 * index contributes nothing to the counts and cluster stats returns a plausible number that is wrong.
 *
 * <p><b>The shape is the point.</b> This returns the whole gated population's counts in one call, and
 * deliberately offers no way to iterate indices. The obvious alternative, handing back a per-index view for
 * {@code MappingStats} to walk, would fix the correctness half while restoring the exact cost this area
 * exists to remove. Making that impossible in the interface is cheaper than catching it in review, which
 * is the same reasoning behind {@code MappingGenerationStore.updateMapping} taking fields to add rather
 * than a replacement map.
 *
 * <p>In production this is backed by a terms aggregation over {@code .opensearch-index-mappings}, which
 * costs one search regardless of how many indices it summarises. That index was where gated mappings lived
 * when this was written; since they moved into the descriptor it is a write-behind projection kept for
 * exactly this query, which is the only reason it still exists. With nothing registered, stats behave
 * exactly as before, so a cluster that has never gated an index is untouched.
 *
 * <p>The counts are inherently as fresh as that projection's last refresh, because an aggregation is a
 * search -- and, since the projection is written behind the mapping rather than with it, as fresh as
 * whatever has been projected. Both bounds are the right trade here: the alternative is a statistic bounded
 * by nothing because it was too expensive to compute.
 */
public final class GatedMappingStatsAggregator {

    /** Field type name to the number of fields of that type, across the whole gated population. */
    @FunctionalInterface
    public interface Aggregator {
        /**
         * @return counts keyed by field type, and the number of gated indices using each, or null when
         *         unavailable
         */
        GatedFieldTypeCounts aggregate();
    }

    /** One aggregate: how many fields of each type, and how many indices use each type. */
    public static final class GatedFieldTypeCounts {
        public static final GatedFieldTypeCounts EMPTY = new GatedFieldTypeCounts(Map.of(), Map.of());

        private final Map<String, Integer> fieldCounts;
        private final Map<String, Integer> indexCounts;

        public GatedFieldTypeCounts(Map<String, Integer> fieldCounts, Map<String, Integer> indexCounts) {
            this.fieldCounts = fieldCounts == null || fieldCounts.isEmpty() ? Map.of() : Map.copyOf(fieldCounts);
            this.indexCounts = indexCounts == null || indexCounts.isEmpty() ? Map.of() : Map.copyOf(indexCounts);
        }

        public Map<String, Integer> fieldCounts() {
            return fieldCounts;
        }

        public Map<String, Integer> indexCounts() {
            return indexCounts;
        }
    }

    private static final AtomicReference<Aggregator> AGGREGATOR = new AtomicReference<>();

    private GatedMappingStatsAggregator() {}

    /** Installs the aggregator. Registering null clears it, which is how a test restores the default. */
    public static void register(Aggregator aggregator) {
        AGGREGATOR.set(aggregator);
    }

    public static boolean isRegistered() {
        return AGGREGATOR.get() != null;
    }

    /**
     * The gated population's field type counts, or null when nothing is registered or the aggregate fails.
     *
     * <p>Failing to null rather than throwing matches the rest of this area: a stats call that returns
     * ordinary indices only is worse than one that returns everything and better than one that fails. The
     * asymmetry is uncomfortable here precisely because the failure is silent, which is why {@link
     * #isRegistered()} exists separately for a caller that needs to tell the two apart.
     */
    public static GatedFieldTypeCounts aggregate() {
        Aggregator aggregator = AGGREGATOR.get();
        if (aggregator == null) {
            return null;
        }
        try {
            return aggregator.aggregate();
        } catch (Exception e) {
            return null;
        }
    }
}
