/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;

/**
 * T26. Whether a bounded prefix wildcard can be answered in bounded time.
 *
 * <p>T25 measured that a wildcard cannot see gated indices at all. The obvious repair is a prefix seam
 * backed by {@code DescriptorStore.findNamesByPrefix}, with a cap on how many names a pattern may expand to,
 * enforced by asking for {@code cap + 1} and refusing when that many come back. That refusal is what makes
 * the contract statable: you may wildcard within a tenant's own namespace, and you may not wildcard across
 * the fleet.
 *
 * <p><b>The whole design rests on one assumption, which is why it is measured rather than assumed.</b> The
 * cap only bounds the work if a query with {@code size = cap + 1} costs about the same whether the prefix
 * matches a thousand names or a hundred million. Lucene does not obviously give that. Sorting by name over a
 * prefix query normally means visiting every matching document and keeping the best {@code size} in a
 * priority queue, which is proportional to matches, not to {@code size}. If that is what happens, then
 * refusing an over-cap pattern costs the same as answering it, the cap protects the client and not the
 * cluster, and the design needs something else: an index sorted on {@code name}, which lets the collector
 * stop early because the index order already is the sort order.
 *
 * <p>So this measures three arms against a growing population:
 *
 * <ul>
 *   <li><b>plain</b>, the descriptor index as it is configured today,</li>
 *   <li><b>sorted</b>, the same index with {@code index.sort.field: name},</li>
 *   <li><b>sorted, untracked</b>, which also drops total hit tracking, since a collector that must report an
 *       exact total cannot stop early no matter how the index is sorted.</li>
 * </ul>
 *
 * <p>Each arm answers both a wide prefix that matches the entire population and a narrow one that matches a
 * handful. The narrow query is the control: it is bounded by construction, so if it also grows with the
 * population then something other than the match set is being measured.
 *
 * <p><b>Decision rule, stated before the numbers.</b> If the wide query at {@code size = 101} grows roughly
 * with the population in the plain arm, then index sorting is a requirement of the design rather than a
 * tuning option, and it belongs in {@code descriptorIndexRequest} beside the merge policy. If the plain arm
 * is already flat, the cap is free and the design is only a cap.
 *
 * <p>Arms alternate within each round and medians are taken, because a single ordering measures warmup as
 * much as it measures the query.
 */
public class WildcardPrefixCostIT extends OpenSearchIntegTestCase {

    /**
     * Populations to compare. Ratios matter more than absolutes: this is a growth measurement.
     *
     * <p>Deliberately large. A first run at 5k, 20k and 80k came out flat, which was the hoped-for answer
     * and not a trustworthy one: every arm landed within a millisecond or two of the transport round trip,
     * so a linear scan of eighty thousand documents would have been hidden inside the floor. At eight
     * hundred thousand a scan is unmistakable against that floor, which is what makes flatness here mean
     * something.
     */
    private static final int[] POPULATIONS = { 50_000, 200_000, 800_000 };

    /** The cap the design would enforce, plus one to detect exceeding it. */
    private static final int SIZE = 101;

    private static final int ROUNDS = 9;

    private static final String PLAIN = "wc-plain";
    private static final String SORTED = "wc-sorted";

    public void testWhetherABoundedPrefixQueryIsBounded() throws Exception {
        StringBuilder table = new StringBuilder(
            String.format(Locale.ROOT, "%nT26 prefix query cost at size=%d, median of %d rounds%n", SIZE, ROUNDS)
        );
        table.append(
            String.format(
                Locale.ROOT,
                "  %10s %14s %14s %14s %14s%n",
                "population",
                "plain wide",
                "sorted wide",
                "untracked wide",
                "narrow (ctl)"
            )
        );

        for (int population : POPULATIONS) {
            build(population);

            long plainWide = median(() -> query(PLAIN, "t", true));
            long sortedWide = median(() -> query(SORTED, "t", true));
            long untrackedWide = median(() -> query(SORTED, "t", false));
            long narrow = median(() -> query(PLAIN, narrowPrefix(), true));

            table.append(
                String.format(
                    Locale.ROOT,
                    "  %,10d %11.2f ms %11.2f ms %11.2f ms %11.2f ms%n",
                    population,
                    plainWide / 1e6,
                    sortedWide / 1e6,
                    untrackedWide / 1e6,
                    narrow / 1e6
                )
            );
        }

        table.append("\n  wide matches the whole population; narrow matches ten names. Both ask for ").append(SIZE);
        table.append(" .\n  If wide grows with population and narrow does not, the cost follows the match set\n");
        table.append("  rather than the requested size, and a cap cannot be enforced cheaply without it.\n");
        logger.warn(table.toString());
    }

    /**
     * What index sorting costs the write path, which is the trade the read result asks for.
     *
     * <p>Sorting an index is not free at write time: segments are built in sort order rather than in
     * arrival order. Creation throughput is a headline number for this work, measured at roughly 850 per
     * second, so a repair that buys a flat wildcard by halving creation would be a bad trade made quietly.
     *
     * <p>Arms alternate within each round rather than running one after the other, so a cluster that gets
     * slower or faster over the run penalises both equally.
     */
    public void testWhatIndexSortingCostsTheWritePath() throws Exception {
        int population = 200_000;
        List<Double> plainRates = new ArrayList<>();
        List<Double> sortedRates = new ArrayList<>();

        for (int round = 0; round < 3; round++) {
            for (String index : List.of(PLAIN, SORTED)) {
                if (indexExists(index)) {
                    assertAcked(client().admin().indices().prepareDelete(index));
                }
            }
            createBoth();
            // Order alternates between rounds, so whichever arm goes first does not always pay for whatever
            // the first write of a round warms.
            if (round % 2 == 0) {
                plainRates.add(populate(PLAIN, population));
                sortedRates.add(populate(SORTED, population));
            } else {
                sortedRates.add(populate(SORTED, population));
                plainRates.add(populate(PLAIN, population));
            }
        }

        double plain = median(plainRates);
        double sorted = median(sortedRates);
        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT26b index sorting on the write path, %,d docs, median of %d rounds%n"
                    + "  plain  %,10.0f docs/s%n"
                    + "  sorted %,10.0f docs/s%n"
                    + "  ratio  %10.2fx%n",
                population,
                plainRates.size(),
                plain,
                sorted,
                plain / sorted
            )
        );

        assertTrue("a rate of zero would mean nothing was written", sorted > 0 && plain > 0);
    }

    /**
     * What index sorting costs the descriptor store's actual workload, rather than a bulk load.
     *
     * <p>S41b measured index sorting as free on the write path and that measurement was quoted to justify
     * making it a contract. It was measured under conditions the descriptor store does not share: bulk
     * requests of two thousand documents, and no reads. The store writes descriptors one at a time and reads
     * them back with realtime GETs, and applying the sort to it made the T22 skew benchmark go from passing
     * inside a seven minute suite to exceeding a twenty minute timeout on its own.
     *
     * <p>So this measures the two phases separately under the shape that regressed: single-document writes,
     * then realtime GETs by id. Separately because the remedy differs. If writes are the cost, the sort is
     * paying for wildcards with creation throughput, which is a headline number. If GETs are the cost, it is
     * paying with the read path, which every request touches.
     *
     * <p>The generalisation is the lesson rather than the number: a benchmark answers the question it was
     * run under, and this is the third time this session that quoting one past its conditions produced a
     * wrong conclusion.
     */
    public void testWhatIndexSortingCostsSingleWritesAndRealtimeGets() throws Exception {
        int documents = 2_000;
        int gets = 5_000;

        for (String index : List.of(PLAIN, SORTED)) {
            if (indexExists(index)) {
                assertAcked(client().admin().indices().prepareDelete(index));
            }
        }
        createBoth();

        // Alternated, so whichever arm runs first does not always pay for what it warms.
        long plainWrites = timeSingleWrites(PLAIN, documents);
        long sortedWrites = timeSingleWrites(SORTED, documents);
        client().admin().indices().prepareRefresh(PLAIN, SORTED).get();
        long sortedGets = timeGets(SORTED, documents, gets);
        long plainGets = timeGets(PLAIN, documents, gets);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT28b index sorting on the descriptor store's own workload%n"
                    + "  %,d single-document writes   plain %8.0f/s   sorted %8.0f/s   ratio %5.2fx%n"
                    + "  %,d realtime GETs by id      plain %8.0f/s   sorted %8.0f/s   ratio %5.2fx%n",
                documents,
                documents / (plainWrites / 1e9),
                documents / (sortedWrites / 1e9),
                (double) sortedWrites / plainWrites,
                gets,
                gets / (plainGets / 1e9),
                gets / (sortedGets / 1e9),
                (double) sortedGets / plainGets
            )
        );

        assertTrue("a zero duration would mean nothing ran", plainWrites > 0 && sortedGets > 0);
    }

    /** One document per request, which is how descriptors are actually written. */
    private long timeSingleWrites(String index, int documents) {
        long start = System.nanoTime();
        for (int i = 0; i < documents; i++) {
            client().prepareIndex(index).setId(name(i)).setSource("name", name(i)).get();
        }
        return System.nanoTime() - start;
    }

    /** Realtime GET by id, which is how descriptors are actually read. */
    private long timeGets(String index, int documents, int gets) {
        long start = System.nanoTime();
        for (int i = 0; i < gets; i++) {
            assertTrue(client().prepareGet(index, name(i % documents)).get().isExists());
        }
        return System.nanoTime() - start;
    }

    /** Rebuilds both indices at the given population, so each row is measured against a fresh index. */
    private void build(int population) throws Exception {
        for (String index : List.of(PLAIN, SORTED)) {
            if (indexExists(index)) {
                assertAcked(client().admin().indices().prepareDelete(index));
            }
        }

        createBoth();
        populate(PLAIN, population);
        populate(SORTED, population);
        // Forced to one segment in both arms, so the comparison is between collection strategies rather
        // than between segment counts.
        assertNoFailures(client().admin().indices().prepareForceMerge(PLAIN, SORTED).setMaxNumSegments(1).get());
        client().admin().indices().prepareRefresh(PLAIN, SORTED).get();
    }

    /** Creates the two arms, identical but for the index sort. */
    private void createBoth() {
        Settings.Builder plain = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        assertAcked(client().admin().indices().prepareCreate(PLAIN).setSettings(plain).setMapping("name", "type=keyword"));

        // The one difference between the arms. An index whose stored order already is the search order lets
        // the collector stop as soon as it has size hits, instead of ranking every match.
        Settings.Builder sorted = Settings.builder().put(plain.build()).put("index.sort.field", "name").put("index.sort.order", "asc");
        assertAcked(client().admin().indices().prepareCreate(SORTED).setSettings(sorted).setMapping("name", "type=keyword"));
    }

    /**
     * Bulk-loads one arm, returning the rate in documents per second.
     *
     * <p><b>Arrival order is shuffled, and that is the point.</b> The first run of this wrote names in
     * ascending order and reported index sorting as free, which was an artefact: feeding a sorted index
     * already-sorted input is the one case where sorting at flush costs nothing. Descriptors arrive in the
     * order tenants are created, which has no relationship to name order, so the shuffle is the realistic
     * case and the sorted run is the artificial one.
     *
     * <p>Seeded from the index name so both arms see the same permutation, which is what makes them
     * comparable rather than two separate experiments.
     */
    private double populate(String index, int population) {
        List<Integer> order = new ArrayList<>(population);
        for (int i = 0; i < population; i++) {
            order.add(i);
        }
        java.util.Collections.shuffle(order, new java.util.Random(0x5EED));

        long start = System.nanoTime();
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int position = 0; position < population; position++) {
            int i = order.get(position);
            bulk.add(client().prepareIndex(index).setId(name(i)).setSource("name", name(i)));
            if (bulk.numberOfActions() >= 2_000) {
                assertFalse(bulk.get().hasFailures());
                bulk = client().prepareBulk();
            }
        }
        if (bulk.numberOfActions() > 0) {
            assertFalse(bulk.get().hasFailures());
        }
        return population / ((System.nanoTime() - start) / 1e9);
    }

    private static double median(List<Double> samples) {
        List<Double> sorted = new ArrayList<>(samples);
        java.util.Collections.sort(sorted);
        return sorted.get(sorted.size() / 2);
    }

    /** One prefix query, returning its wall time and asserting it returned a full page. */
    private long query(String index, String prefix, boolean trackTotalHits) {
        long start = System.nanoTime();
        SearchResponse response = client().prepareSearch(index)
            .setQuery(QueryBuilders.prefixQuery("name", prefix))
            .addSort("name", SortOrder.ASC)
            .setFetchSource(false)
            .addDocValueField("name")
            .setTrackTotalHits(trackTotalHits)
            .setSize(SIZE)
            .get();
        long elapsed = System.nanoTime() - start;
        assertTrue(
            "a query returning nothing would measure an empty search rather than a prefix expansion",
            response.getHits().getHits().length > 0
        );
        return elapsed;
    }

    /**
     * Runs an arm {@link #ROUNDS} times and returns the median.
     *
     * <p>One discarded run first, so the first query of an arm does not carry the cost of warming
     * everything the later ones reuse. S21, S23 and S24 each reported a first-query artefact before
     * annotating it, so it is paid for here rather than explained afterwards.
     */
    private long median(java.util.function.Supplier<Long> arm) {
        arm.get();
        List<Long> samples = new ArrayList<>();
        for (int i = 0; i < ROUNDS; i++) {
            samples.add(arm.get());
        }
        java.util.Collections.sort(samples);
        return samples.get(samples.size() / 2);
    }

    /** Ten names, regardless of population, so the control is bounded by construction. */
    private static String narrowPrefix() {
        return "t00042-";
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "t%05d-idx%02d", i / 10, i % 10);
    }
}
