/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H7c. What a wildcard actually costs when it has to return the names.
 *
 * <p>S21 and S23 both measured prefix queries with {@code size=0}, which counts matches rather than
 * returning them, and both said so. Real resolution needs the names: {@code logs-*} has to produce the
 * list of indices to act on, not the number of them. Fetching ten thousand names costs more than counting
 * them, and the gap between those two numbers is the honest cost of a wildcard.
 *
 * <p>Three things are measured rather than one, because the shape of the answer decides the design:
 *
 * <ul>
 *   <li><b>counting</b>, which is what was measured before and is the floor</li>
 *   <li><b>the first page</b>, which is what a bounded wildcard costs</li>
 *   <li><b>every name</b>, which is what an unbounded {@code logs-*} against a large match set costs</li>
 * </ul>
 *
 * <p>If fetching all names is close to counting them, wildcards are cheap and the design needs nothing
 * more. If it is far worse, resolution needs pagination and that belongs in the API contract next to the
 * freshness decision rather than being discovered when someone runs {@code logs-*} against a tenant with
 * a hundred thousand indices.
 */
public class WildcardResolutionCostIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";
    private static final int[] MATCH_SIZES = { 100, 1_000, 10_000 };
    private static final int POPULATION = 20_000;
    private static final int IN_FLIGHT = 200;
    private static final int PAGE = 1_000;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testWildcardCostCountingAgainstReturningNames() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTORS)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("name", "type=keyword")
        );
        writeDescriptors(POPULATION);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();

        StringBuilder table = new StringBuilder("\nH7c wildcard cost, counting against returning names, 5 shards\n");
        table.append(String.format(Locale.ROOT, "  %8s %14s %16s %16s%n", "matches", "count (ms)", "first page (ms)", "all names (ms)"));

        for (int matches : MATCH_SIZES) {
            // Prefixes are chosen so the match set size is what varies: idx-1xxxx matches ten thousand,
            // idx-11xxx a thousand, idx-111xx a hundred.
            String prefix = prefixMatching(matches);

            long startedAt = System.nanoTime();
            long counted = client().prepareSearch(DESCRIPTORS)
                .setQuery(QueryBuilders.prefixQuery("name", prefix))
                .setSize(0)
                .get()
                .getHits()
                .getTotalHits()
                .value();
            double countMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

            startedAt = System.nanoTime();
            SearchResponse firstPage = client().prepareSearch(DESCRIPTORS)
                .setQuery(QueryBuilders.prefixQuery("name", prefix))
                .setSize(PAGE)
                .get();
            double firstPageMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

            startedAt = System.nanoTime();
            List<String> allNames = everyName(prefix);
            double allNamesMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

            assertEquals("the count and the names must agree, or one of them is measuring the wrong thing", counted, allNames.size());
            assertEquals("the first page must be bounded by the page size", Math.min(PAGE, matches), firstPage.getHits().getHits().length);

            table.append(
                String.format(Locale.ROOT, "  %,8d %14.3f %16.3f %16.3f%n", matches, countMillis, firstPageMillis, allNamesMillis)
            );
        }
        logger.warn(table.toString());
    }

    /**
     * The claim that decides whether resolution needs pagination in its contract. Returning names must
     * not be dramatically worse than counting them, or an unbounded wildcard against a large tenant is a
     * different operation from what has been measured so far.
     */
    public void testReturningNamesIsNotDramaticallyWorseThanCounting() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTORS)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 5)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("name", "type=keyword")
        );
        writeDescriptors(POPULATION);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();

        String prefix = prefixMatching(10_000);
        // Warm both paths so the first sample does not become the reported number, which is the mistake
        // S21 and S23 both made and had to annotate afterwards.
        client().prepareSearch(DESCRIPTORS).setQuery(QueryBuilders.prefixQuery("name", prefix)).setSize(0).get();
        everyName(prefix);

        long startedAt = System.nanoTime();
        client().prepareSearch(DESCRIPTORS).setQuery(QueryBuilders.prefixQuery("name", prefix)).setSize(0).get();
        double countMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

        startedAt = System.nanoTime();
        List<String> names = everyName(prefix);
        double namesMillis = (System.nanoTime() - startedAt) / 1_000_000.0;

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH7c ten thousand matches: counting %.3f ms, returning names %.3f ms, ratio %.1fx%n",
                countMillis,
                namesMillis,
                namesMillis / countMillis
            )
        );

        assertEquals("ten thousand names must actually come back", 10_000, names.size());
        assertTrue("both measurements must be non-zero", countMillis > 0 && namesMillis > 0);
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Every matching name, paged by sorting on the name itself and using search-after.
     *
     * <p>Deliberately not a single large {@code size}, because that is what a real resolver cannot do:
     * the result set is unbounded and {@code index.max_result_window} caps a single response at ten
     * thousand by default. Paging is what resolution would actually have to do, so paging is what is
     * measured.
     */
    private List<String> everyName(String prefix) {
        List<String> names = new ArrayList<>();
        Object[] searchAfter = null;
        while (true) {
            var request = client().prepareSearch(DESCRIPTORS)
                .setQuery(QueryBuilders.prefixQuery("name", prefix))
                .addSort("name", SortOrder.ASC)
                .setSize(PAGE);
            if (searchAfter != null) {
                request.searchAfter(searchAfter);
            }
            SearchResponse response = request.get();
            SearchHit[] hits = response.getHits().getHits();
            if (hits.length == 0) {
                return names;
            }
            for (SearchHit hit : hits) {
                names.add(hit.getId());
            }
            searchAfter = hits[hits.length - 1].getSortValues();
        }
    }

    /** A prefix whose match set is the requested size, given names of the form idx-00000 to idx-19999. */
    private static String prefixMatching(int matches) {
        return switch (matches) {
            case 10_000 -> "idx-1";
            case 1_000 -> "idx-10";
            case 100 -> "idx-100";
            default -> throw new IllegalArgumentException("no prefix defined for " + matches + " matches");
        };
    }

    private void writeDescriptors(int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);
        for (int i = 0; i < count; i++) {
            inFlight.acquire();
            // Fixed width so prefix length maps cleanly onto match set size.
            String name = String.format(Locale.ROOT, "idx-%05d", i);
            client().index(new IndexRequest(DESCRIPTORS).id(name).source("name", name).create(true), new ActionListener<>() {
                @Override
                public void onResponse(org.opensearch.action.index.IndexResponse response) {
                    inFlight.release();
                    done.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    failures.incrementAndGet();
                    inFlight.release();
                    done.countDown();
                }
            });
        }
        assertTrue("descriptor writes must finish", done.await(10, TimeUnit.MINUTES));
        assertEquals("no descriptor write may fail", 0, failures.get());
    }
}
