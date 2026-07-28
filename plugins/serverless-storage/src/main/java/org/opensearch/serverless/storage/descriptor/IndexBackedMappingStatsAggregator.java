/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.nested.Nested;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.Map;

/**
 * Field type counts for the whole gated population, from one aggregation.
 *
 * <p>H19 pinned the gap: {@code MappingStats.of} walks every index in cluster state, so a gated index
 * contributes nothing and {@code _cluster/stats} reports a plausible number that is simply wrong. H20 built
 * the seam, deliberately shaped so per-index iteration cannot be expressed through it, because the naive
 * repair would fix correctness while restoring the population-sized cost.
 *
 * <p>This is the implementation that seam was waiting for. It reads the {@code fieldTypeCounts} projection
 * W5 writes beside each mapping: a nested array of {@code {type, count}}, indexed because field types are a
 * bounded vocabulary, unlike field names.
 *
 * <p><b>One request regardless of population.</b> A nested terms aggregation on {@code type} gives the
 * number of gated indices using each type as its document count, and a sum on {@code count} inside it gives
 * the number of fields of that type. Both numbers come from the same aggregation, and its cost is set by
 * the number of distinct field types rather than by the number of indices. That is the property H19 asked
 * for, and it is why the counts are exact rather than sampled.
 */
public final class IndexBackedMappingStatsAggregator implements GatedMappingStatsAggregator.Aggregator {

    private static final Logger logger = LogManager.getLogger(IndexBackedMappingStatsAggregator.class);

    /**
     * How many distinct field types the aggregation will return.
     *
     * <p>Comfortably above the number of types OpenSearch defines, so the terms aggregation is exhaustive
     * rather than a top-N. A truncated stats answer would be the same silent-wrong-number failure H19 was
     * about, arrived at from the other direction.
     */
    private static final int MAX_FIELD_TYPES = 200;

    private final Client client;

    public IndexBackedMappingStatsAggregator(Client client) {
        this.client = client;
    }

    @Override
    public GatedMappingStatsAggregator.GatedFieldTypeCounts aggregate() {
        try {
            SearchResponse response = client.prepareSearch(IndexBackedMappingStore.MAPPING_INDEX)
                .setSize(0)
                .addAggregation(
                    AggregationBuilders.nested("types", "fieldTypeCounts")
                        .subAggregation(
                            AggregationBuilders.terms("byType")
                                .field("fieldTypeCounts.type")
                                .size(MAX_FIELD_TYPES)
                                .subAggregation(AggregationBuilders.sum("fields").field("fieldTypeCounts.count"))
                        )
                )
                .get();

            Nested types = response.getAggregations().get("types");
            Terms byType = types.getAggregations().get("byType");

            Map<String, Integer> fieldCounts = new HashMap<>();
            Map<String, Integer> indexCounts = new HashMap<>();
            for (Terms.Bucket bucket : byType.getBuckets()) {
                String type = bucket.getKeyAsString();
                Sum fields = bucket.getAggregations().get("fields");
                fieldCounts.put(type, (int) fields.getValue());
                // A nested document per {type, count} entry, one entry per type per index, so the bucket's
                // document count is the number of indices using that type.
                indexCounts.put(type, (int) bucket.getDocCount());
            }
            return new GatedMappingStatsAggregator.GatedFieldTypeCounts(fieldCounts, indexCounts);
        } catch (Exception e) {
            // Returning null leaves ordinary stats intact rather than failing the whole stats call, which
            // matches how every other descriptor seam treats an unavailable store: a stats response missing
            // its gated half is bad, a stats call that fails because the mapping index hiccuped is worse.
            logger.debug("gated mapping stats aggregation failed", e);
            return null;
        }
    }
}
