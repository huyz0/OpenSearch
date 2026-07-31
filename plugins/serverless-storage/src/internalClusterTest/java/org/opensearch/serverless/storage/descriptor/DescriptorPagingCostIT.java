/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * T5. What a page of descriptors costs, split into the parts a name-only caller would not pay.
 *
 * <p>{@code DescriptorGate.pagerFor} calls {@link DescriptorStore#findByPrefix}, which fetches {@code _source}
 * for every hit and runs {@code DescriptorCodec.fromSource} over it to build a fourteen-field
 * {@link IndexDescriptor}. H16 and H17 established that the caller behind that pager is pagination, which
 * orders by name and creation date and shows names.
 *
 * <p>TiDB's equivalent bulk path does not deserialise at all. It iterates the raw stored bytes and pulls the
 * id and name out with a regex, unmarshalling only when a substring search finds a marker saying the object
 * has an attribute that must be resident ({@code meta/meta.go:1351} and {@code isTableInfoMustLoad} at 1301).
 *
 * <p><b>Measured before deciding whether to copy that.</b> Three arms over the same population:
 *
 * <ul>
 *   <li><b>full</b>, what ships: prefix search fetching {@code _source}, then {@code fromSource} per hit</li>
 *   <li><b>search only</b>, the same search with the deserialisation removed, which separates what the
 *       index costs from what the decode costs</li>
 *   <li><b>names only</b>, {@code _source} disabled and the name taken from a doc value, which is what a
 *       name-only path would actually do</li>
 * </ul>
 *
 * <p>The third arm is the one that decides it. If it is close to the first, there is nothing to win here and
 * the honest outcome is to leave {@code findByPrefix} alone rather than add a second read path for a saving
 * that does not exist.
 *
 * <p><b>Measured, across five runs of the whole test.</b> Decoding is 25 to 36 percent of a page, and a
 * name-only path saves 24 to 32 percent. One run reported 11 percent and 3.5 percent and is excluded as
 * perturbed: it is the only run where the names-only arm came out slower than the search-only arm, which
 * cannot be true since it does strictly less work.
 *
 * <p>Run-to-run variance is wide enough that a single run of this test decides nothing, which is worth
 * stating because the first run said 27.6 percent and the second said 3.5 percent. The conclusion rests on
 * the direction being consistent across runs, not on any one number.
 *
 * <p><b>A hypothesis this disproved.</b> {@code pagerFor} passes an empty prefix, so every listing page is
 * built from {@code prefixQuery("name", "")}, which looked like a term enumeration standing in for
 * {@code match_all}. Measured at 1.06x, 1.07x, 1.18x and 1.12x, so the empty prefix does cost more, but it
 * is a tenth of the query rather than the dominant term, and the query is not where the page goes. Not
 * changed, because a rewrite of the one query pagination depends on is not worth ten percent of a term
 * lookup, and P10 is the standing reminder about building the fix before measuring whether it matters.
 */
public class DescriptorPagingCostIT extends OpenSearchIntegTestCase {

    /** One page, matching {@link DescriptorStore#PAGE_SIZE}, since the question is what a page costs. */
    private static final int POPULATION = DescriptorStore.PAGE_SIZE;

    private static final int ROUNDS = 7;

    public void testWhatAPageCosts() {
        DescriptorStore store = new DescriptorStore(client(), 1);
        for (int i = 0; i < POPULATION; i++) {
            store.create(descriptor(String.format(Locale.ROOT, "paged-idx-%05d", i)));
        }
        client().admin().indices().prepareRefresh(DescriptorStore.DESCRIPTOR_INDEX).get();

        // Warmed with the same work as the measurement, since an under-warmed first arm is how S30 inverted
        // a whole curve.
        for (int i = 0; i < 2; i++) {
            full();
            searchOnly();
            namesOnly();
            emptyPrefixQuery();
            matchAllQuery();
        }

        long[] fullMicros = new long[ROUNDS];
        long[] searchMicros = new long[ROUNDS];
        long[] namesMicros = new long[ROUNDS];
        long[] emptyPrefixMicros = new long[ROUNDS];
        long[] matchAllMicros = new long[ROUNDS];
        for (int round = 0; round < ROUNDS; round++) {
            // Alternating within the round rather than running each arm to completion, so a JIT or GC state
            // favouring one arm does not persist across the measurement.
            fullMicros[round] = timeMicros(this::full);
            searchMicros[round] = timeMicros(this::searchOnly);
            namesMicros[round] = timeMicros(this::namesOnly);
            emptyPrefixMicros[round] = timeMicros(this::emptyPrefixQuery);
            matchAllMicros[round] = timeMicros(this::matchAllQuery);
        }

        long full = median(fullMicros);
        long searchOnly = median(searchMicros);
        long namesOnly = median(namesMicros);
        long emptyPrefix = median(emptyPrefixMicros);
        long matchAll = median(matchAllMicros);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT5 one page of %d descriptors, median of %d rounds%n"
                    + "  full (search + fromSource per hit)   %,8d us%n"
                    + "  search only (no deserialisation)     %,8d us%n"
                    + "  names only (no _source, doc value)   %,8d us%n"
                    + "  deserialisation is %.1f%% of the page%n"
                    + "  a name-only path would save %.1f%%%n"
                    + "%n  the query pagination actually issues, against the equivalent one%n"
                    + "  prefix(\"\") as pagerFor sends it       %,8d us%n"
                    + "  match_all                            %,8d us%n"
                    + "  match_all is %.2fx the empty prefix%n",
                POPULATION,
                ROUNDS,
                full,
                searchOnly,
                namesOnly,
                100.0 * (full - searchOnly) / full,
                100.0 * (full - namesOnly) / full,
                emptyPrefix,
                matchAll,
                emptyPrefix / (double) matchAll
            )
        );

        assertTrue("every arm must be non-zero, or this measured nothing", full > 0 && searchOnly > 0 && namesOnly > 0);
    }

    /**
     * T6. The cheap path and the full path must agree, name for name and date for date.
     *
     * <p>{@code findNamesForPage} is a second way to read the same documents, and a second read path that
     * can drift from the first is exactly what C3 established the store exists to prevent. The saving is
     * only worth having if the two cannot disagree, so this pins them together rather than trusting that
     * doc values and {@code _source} say the same thing.
     *
     * <p>Creation dates ascend with the name here, so the two paths order identically despite sorting on
     * different keys since T27, and a positional comparison is still meaningful. That is a property of this
     * fixture rather than of the store: the two orders are the same only because this data makes them so.
     */
    public void testTheNameOnlyPathAgreesWithTheFullPath() {
        DescriptorStore store = new DescriptorStore(client(), 1);
        for (int i = 0; i < 50; i++) {
            store.create(descriptor(String.format(Locale.ROOT, "agreed-idx-%05d", i), 1_700_000_000_000L + i));
        }
        client().admin().indices().prepareRefresh(DescriptorStore.DESCRIPTOR_INDEX).get();

        List<IndexDescriptor> full = store.findByPrefix("agreed-idx-", null, 50);
        var namesOnly = store.findNamesForPage(null, 0L, true, 50);

        assertEquals("both paths must return the same page size", full.size(), namesOnly.size());
        assertEquals("and a full page, or this compared almost nothing", 50, full.size());
        for (int i = 0; i < full.size(); i++) {
            assertEquals("name at position " + i, full.get(i).name(), namesOnly.get(i).name());
            assertEquals(
                "creation date for " + full.get(i).name() + ", which pagination sorts by",
                full.get(i).creationDate(),
                namesOnly.get(i).creationDate()
            );
        }
    }

    /** What ships today. */
    private void full() {
        List<IndexDescriptor> page = store().findByPrefix("paged-idx-", null, POPULATION);
        assertEquals("the page must be full, or the arms are not comparable", POPULATION, page.size());
    }

    /** The same search, without building descriptors from the hits. */
    private void searchOnly() {
        SearchResponse response = client().prepareSearch(DescriptorStore.DESCRIPTOR_INDEX)
            .setQuery(QueryBuilders.prefixQuery("name", "paged-idx-"))
            .addSort("name", SortOrder.ASC)
            .setSize(POPULATION)
            .get();
        assertEquals(POPULATION, response.getHits().getHits().length);
    }

    /**
     * The query pagination actually issues, against the one that means the same thing.
     *
     * <p>{@code DescriptorGate.pagerFor} calls {@code findByPrefix("", afterName, size)}, so the query built
     * for every page of a listing is {@code prefixQuery("name", "")}. An empty prefix matches every term,
     * but it is still a term enumeration rather than a statement that everything matches, and the page is
     * already fully determined by the sort and {@code search_after}.
     *
     * <p>Measured rather than assumed, because "this query looks wasteful" is exactly the kind of claim
     * P10 disproved after building the fix first.
     */
    private void emptyPrefixQuery() {
        SearchResponse response = client().prepareSearch(DescriptorStore.DESCRIPTOR_INDEX)
            .setQuery(QueryBuilders.prefixQuery("name", ""))
            .addSort("name", SortOrder.ASC)
            .setSize(POPULATION)
            .get();
        assertEquals(POPULATION, response.getHits().getHits().length);
    }

    /** The same page, asked for as everything rather than as an empty prefix. */
    private void matchAllQuery() {
        SearchResponse response = client().prepareSearch(DescriptorStore.DESCRIPTOR_INDEX)
            .setQuery(QueryBuilders.matchAllQuery())
            .addSort("name", SortOrder.ASC)
            .setSize(POPULATION)
            .get();
        assertEquals(POPULATION, response.getHits().getHits().length);
    }

    /** What a name-only path would do: no {@code _source} at all, the name read from a doc value. */
    private void namesOnly() {
        SearchResponse response = client().prepareSearch(DescriptorStore.DESCRIPTOR_INDEX)
            .setQuery(QueryBuilders.prefixQuery("name", "paged-idx-"))
            .addSort("name", SortOrder.ASC)
            .setFetchSource(false)
            .addDocValueField("name")
            .setSize(POPULATION)
            .get();
        List<String> names = new ArrayList<>(POPULATION);
        for (SearchHit hit : response.getHits().getHits()) {
            names.add((String) hit.field("name").getValue());
        }
        assertEquals(POPULATION, names.size());
    }

    private DescriptorStore store() {
        return new DescriptorStore(client(), 1);
    }

    private static long timeMicros(Runnable work) {
        long startedAt = System.nanoTime();
        work.run();
        return (System.nanoTime() - startedAt) / 1_000;
    }

    private static long median(long[] values) {
        long[] sorted = values.clone();
        java.util.Arrays.sort(sorted);
        return sorted[sorted.length / 2];
    }

    private static IndexDescriptor descriptor(String name) {
        return descriptor(name, 1_700_000_000_000L);
    }

    private static IndexDescriptor descriptor(String name, long creationDate) {
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
            creationDate
        );
    }
}
