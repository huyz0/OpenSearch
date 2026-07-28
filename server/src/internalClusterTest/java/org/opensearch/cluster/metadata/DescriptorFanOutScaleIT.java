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
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H7b. What the descriptor index costs as its own shard count grows.
 *
 * <p>S21 measured lookups against descriptor <em>population</em> and found them flat, then flagged its own
 * limitation: it ran on five shards, and a hundred million descriptors needs closer to a hundred. Per-shard
 * work is a term dictionary seek and does not grow with population, which is what those flat numbers show.
 * The cost that does grow is the coordination of a scatter-gather, and that grows with shard count rather
 * than with population, so it was invisible to the measurement that flagged it.
 *
 * <p>The two operations diverge here and that is the whole point of measuring them separately. A point
 * lookup routes by id to exactly one shard, so it should stay flat however many shards exist. A wildcard
 * touches every shard, so it should grow with shard count. If the point lookup grows, resolution does not
 * scale and Area H's read path is in trouble; if only the wildcard grows, the question becomes how many
 * shards the descriptor index can afford, which is a sizing decision rather than a design flaw.
 *
 * <p>Population is held constant so shard count is the only variable. S21 already established that
 * population does not matter.
 */
public class DescriptorFanOutScaleIT extends OpenSearchIntegTestCase {

    private static final int[] SHARD_COUNTS = { 1, 5, 20, 60 };
    private static final int DESCRIPTORS = 20_000;
    private static final int IN_FLIGHT = 200;
    private static final int SAMPLES = 50;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testLookupAndWildcardAgainstShardCount() throws Exception {
        StringBuilder table = new StringBuilder("\nH7b descriptor cost against shard count, ").append(internalCluster().size())
            .append(" nodes, ")
            .append(String.format(Locale.ROOT, "%,d", DESCRIPTORS))
            .append(" descriptors\n");
        table.append(String.format(Locale.ROOT, "  %7s %14s %16s %14s%n", "shards", "GET (ms)", "prefix hits", "prefix (ms)"));

        for (int shards : SHARD_COUNTS) {
            String index = "descriptors-" + shards;
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .setMapping("name", "type=keyword")
            );
            writeDescriptors(index, DESCRIPTORS);
            client().admin().indices().prepareRefresh(index).get();

            double getMillis = timeLookups(index);

            long startedAt = System.nanoTime();
            SearchResponse response = client().prepareSearch(index).setQuery(QueryBuilders.prefixQuery("name", "idx-1")).setSize(0).get();
            double prefixMillis = (System.nanoTime() - startedAt) / 1_000_000.0;
            long hits = response.getHits().getTotalHits().value();

            assertEquals(
                "every shard must have been searched, or the fan-out is not what is being measured",
                shards,
                response.getTotalShards()
            );
            assertTrue("the prefix must match something", hits > 0);

            table.append(String.format(Locale.ROOT, "  %,7d %14.3f %,16d %14.3f%n", shards, getMillis, hits, prefixMillis));
        }
        logger.warn(table.toString());
    }

    /**
     * The claim Area H's read path depends on: a point lookup routes to one shard, so it must not care
     * how many shards the descriptor index has. If this grows, resolution does not scale.
     */
    public void testPointLookupIsFlatAgainstShardCount() throws Exception {
        double atOne = lookupCostWithShards(1);
        double atSixty = lookupCostWithShards(60);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH7b point lookup: %.3f ms with 1 shard, %.3f ms with 60, ratio %.2fx%n",
                atOne,
                atSixty,
                atSixty / atOne
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", atOne > 0 && atSixty > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "a point lookup routes to one shard and must not scale with the shard count of the "
                    + "descriptor index: %.3f ms at 1 shard against %.3f ms at 60",
                atOne,
                atSixty
            ),
            atSixty < atOne * 3
        );
    }

    // ---------------------------------------------------------------- helpers

    private double lookupCostWithShards(int shards) throws Exception {
        String index = "lookup-" + shards;
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
        );
        writeDescriptors(index, DESCRIPTORS);
        client().admin().indices().prepareRefresh(index).get();
        return timeLookups(index);
    }

    /** Realtime GET by id, which routes to exactly one shard whatever the shard count. */
    private double timeLookups(String index) {
        long total = 0;
        for (int sample = 0; sample < SAMPLES; sample++) {
            String name = "idx-" + randomIntBetween(0, DESCRIPTORS - 1);
            long startedAt = System.nanoTime();
            assertTrue("every sampled descriptor must exist: " + name, client().prepareGet(index, name).get().isExists());
            total += System.nanoTime() - startedAt;
        }
        return (total / (double) SAMPLES) / 1_000_000.0;
    }

    private void writeDescriptors(String index, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);
        for (int i = 0; i < count; i++) {
            inFlight.acquire();
            String name = "idx-" + i;
            client().index(
                new IndexRequest(index).id(name).source("name", name, "uuid", name + "-uuid").create(true),
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
        assertTrue("descriptor writes must finish", done.await(10, TimeUnit.MINUTES));
        assertEquals("no descriptor write may fail, or the population is not what it claims", 0, failures.get());
    }
}
