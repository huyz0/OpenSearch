/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H18. What a caller may assume about seeing an index just created.
 *
 * <p>This has been called a product decision twice and deferred both times. It is not one. The answer is
 * forced by how the descriptor index stores documents, and the point of this test is to establish that by
 * measurement rather than to keep describing the question.
 *
 * <p>Two resolution paths read the same descriptors by different means. Naming an index exactly fetches a
 * document by id, which reads through the translog and therefore sees a write the instant it is
 * acknowledged. Matching a wildcard runs a search, which sees only segments as of the last refresh. If
 * that difference holds, the contract writes itself: <b>exact names are immediately consistent, wildcards
 * are refresh-bound</b>, and nobody has to choose it.
 *
 * <p>Refresh is disabled outright rather than left at its default interval. A one-second default would
 * make the wildcard arm pass or fail depending on timing, which is how a test comes to assert whatever the
 * machine did that morning.
 *
 * <p>The consequence worth stating for the API contract: an index is nameable the moment it is created,
 * and a client that creates {@code logs-2026-07} and immediately runs {@code logs-*} may not see it. That
 * is a real edge and it is bounded by the refresh interval, so it is documentable rather than surprising.
 */
public class DescriptorFreshnessContractIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";

    public void testExactNamesAreRealtimeAndWildcardsAreRefreshBound() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTORS)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        // Disabled rather than left at the default, so the wildcard arm cannot pass by
                        // being slower than a one second refresh.
                        .put("index.refresh_interval", "-1")
                        .build()
                )
                .setMapping("name", "type=keyword")
        );

        String name = "logs-2026-07";
        client().index(
            new IndexRequest(DESCRIPTORS).id(name).source("name", name).create(true).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
        ).get();

        // Exact name: a get by id reads through the translog, so the descriptor is there immediately.
        assertTrue(
            "an index must be nameable the moment it is created, since a client that creates an index and "
                + "immediately writes to it cannot be told the index does not exist",
            client().prepareGet(DESCRIPTORS, name).get().isExists()
        );

        // Wildcard: a search sees segments, and nothing has been refreshed into one yet.
        long matchedBeforeRefresh = client().prepareSearch(DESCRIPTORS)
            .setQuery(QueryBuilders.prefixQuery("name", "logs-"))
            .setSize(0)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits()
            .value();

        client().admin().indices().prepareRefresh(DESCRIPTORS).get();

        long matchedAfterRefresh = client().prepareSearch(DESCRIPTORS)
            .setQuery(QueryBuilders.prefixQuery("name", "logs-"))
            .setSize(0)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits()
            .value();

        logger.warn(
            String.format(
                java.util.Locale.ROOT,
                "%nH18 freshness: exact name visible immediately, wildcard matched %d before refresh and %d after%n",
                matchedBeforeRefresh,
                matchedAfterRefresh
            )
        );

        assertEquals(
            "a wildcard must not see the index before a refresh. This is the contract rather than a bug: "
                + "wildcard resolution is refresh-bound because it is a search, and callers needing "
                + "immediate visibility must name the index exactly",
            0L,
            matchedBeforeRefresh
        );
        assertEquals("and must see it after one", 1L, matchedAfterRefresh);
    }

    /**
     * The same asymmetry stated the other way round, because the useful half of the contract is the
     * guarantee rather than the limitation: an exact name never needs a refresh, at any population.
     */
    public void testAnExactNameNeverNeedsARefresh() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTORS)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.refresh_interval", "-1")
                        .build()
                )
                .setMapping("name", "type=keyword")
        );

        for (int i = 0; i < 100; i++) {
            String name = "idx-" + i;
            client().index(
                new IndexRequest(DESCRIPTORS).id(name).source("name", name).create(true).setRefreshPolicy(WriteRequest.RefreshPolicy.NONE)
            ).get();
            assertTrue(
                "descriptor " + name + " must be readable by name with no refresh in between",
                client().prepareGet(DESCRIPTORS, name).get().isExists()
            );
        }
    }
}
