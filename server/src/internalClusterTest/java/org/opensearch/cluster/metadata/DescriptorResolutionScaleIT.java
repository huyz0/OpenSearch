/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H2f. Whether the descriptor index can answer resolution at scale, which S19 did not test.
 *
 * <p>S19 proved descriptor <em>writes</em> stay flat where index creation degrades. It said nothing about
 * reads, and Area H's read path has a risk its write path does not: OpenSearch routes by hash where S3
 * partitions by range. A point lookup is therefore a realtime GET against one shard, which should be flat
 * and is the S3 {@code HEAD} analogue. A wildcard is a scatter-gather across every shard of the descriptor
 * index rather than a range scan over the partitions covering the prefix, which is the S3 {@code LIST}
 * analogue only in intent.
 *
 * <p>That difference cuts both ways and both directions matter. Hash routing spreads a tenant creating a
 * million {@code logs-*} indices evenly, where range partitioning would concentrate them and need the
 * adaptive splitting S3 built. But it makes every wildcard touch every shard.
 *
 * <p>So this measures both against descriptor population, before the resolver integration is built on top
 * of them. If wildcard latency is unacceptable the answer is custom routing by name prefix, which trades
 * back toward S3's model and reintroduces hot prefixes. That is a real tradeoff and it should be decided
 * on a number rather than a preference.
 */
public class DescriptorResolutionScaleIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";
    private static final int[] POPULATIONS = { 1_000, 10_000, 50_000 };
    private static final int IN_FLIGHT = 200;

    /** Enough lookups that one slow outlier does not become the reported number. */
    private static final int SAMPLES = 50;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testLookupAndWildcardAgainstPopulation() throws Exception {
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
                .setMapping("name", "type=keyword", "uuid", "type=keyword")
        );

        StringBuilder table = new StringBuilder("\nH2f descriptor resolution against population, ").append(internalCluster().size())
            .append(" nodes, 5 shards\n");
        table.append(String.format(Locale.ROOT, "  %9s %14s %16s %14s%n", "descriptors", "GET (ms)", "prefix hits", "prefix (ms)"));

        int written = 0;
        for (int population : POPULATIONS) {
            writeDescriptors(written, population - written);
            written = population;
            client().admin().indices().prepareRefresh(DESCRIPTORS).get();

            double getMillis = timeLookups(population);
            long prefixHits;
            double prefixMillis;
            {
                long startedAt = System.nanoTime();
                SearchResponse response = client().prepareSearch(DESCRIPTORS)
                    .setQuery(QueryBuilders.prefixQuery("name", "idx-1"))
                    .setSize(0)
                    .get();
                prefixMillis = (System.nanoTime() - startedAt) / 1_000_000.0;
                prefixHits = response.getHits().getTotalHits().value();
            }

            assertTrue("the prefix query must match something, or this measured an empty scan", prefixHits > 0);
            table.append(String.format(Locale.ROOT, "  %,11d %14.3f %,16d %14.3f%n", population, getMillis, prefixHits, prefixMillis));
        }
        logger.warn(table.toString());
    }

    /**
     * The claim that matters most: a point lookup must not care how many descriptors exist, because that
     * is what makes resolution O(1) and the whole area viable.
     */
    public void testPointLookupIsFlatAgainstPopulation() throws Exception {
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
        );

        writeDescriptors(0, 1_000);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();
        double atThousand = timeLookups(1_000);

        writeDescriptors(1_000, 49_000);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();
        double atFiftyThousand = timeLookups(50_000);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH2f point lookup: %.3f ms at 1,000 descriptors, %.3f ms at 50,000, ratio %.2fx%n",
                atThousand,
                atFiftyThousand,
                atFiftyThousand / atThousand
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", atThousand > 0 && atFiftyThousand > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "a point lookup must not scale with the number of descriptors, which is the property Area H "
                    + "depends on: %.3f ms at 1,000 against %.3f ms at 50,000",
                atThousand,
                atFiftyThousand
            ),
            atFiftyThousand < atThousand * 3
        );
    }

    // ---------------------------------------------------------------- helpers

    /** Realtime GET by id, which is the S3 HEAD analogue and needs no refresh. */
    private double timeLookups(int population) {
        long total = 0;
        for (int sample = 0; sample < SAMPLES; sample++) {
            String name = "idx-" + randomIntBetween(0, population - 1);
            long startedAt = System.nanoTime();
            GetResponse response = client().prepareGet(DESCRIPTORS, name).get();
            total += System.nanoTime() - startedAt;
            assertTrue("every sampled descriptor must exist, or the timing is of misses: " + name, response.isExists());
        }
        return (total / (double) SAMPLES) / 1_000_000.0;
    }

    private void writeDescriptors(int from, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);
        for (int i = 0; i < count; i++) {
            inFlight.acquire();
            String name = "idx-" + (from + i);
            client().index(
                new IndexRequest(DESCRIPTORS).id(name).source("name", name, "uuid", name + "-uuid", "shards", 1).create(true),
                new ActionListener<>() {
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
                }
            );
        }
        assertTrue("descriptor writes must finish within the budget", done.await(10, TimeUnit.MINUTES));
        assertEquals("no descriptor write may fail, or the population is not what it claims", 0, failures.get());
    }
}
