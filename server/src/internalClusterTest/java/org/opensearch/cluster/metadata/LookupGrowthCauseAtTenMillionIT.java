/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H22. Why the descriptor lookup grows, and whether that is a design parameter or a fact.
 *
 * <p>S28 measured point lookup rising 1.88x from one million descriptors to ten million, which turned this
 * plan's extrapolation to a hundred million from a flat curve into arithmetic on a growth rate. Two causes
 * are consistent with that number and they have opposite consequences.
 *
 * <ul>
 *   <li><b>Segment count.</b> A get by id must consult every segment that could hold the id, so more
 *       segments means more work per lookup. If this is the cause, the growth is an artefact of merge
 *       policy and force merging removes it, which means 100M lookup is something the design controls.</li>
 *   <li><b>Term dictionary size.</b> Looking a term up in a larger dictionary costs more, logarithmically.
 *       If this is the cause the growth is irreducible, and the extrapolation stands as arithmetic rather
 *       than as something to be engineered away.</li>
 * </ul>
 *
 * <p>Force merging separates them cleanly: it collapses segments without shrinking the dictionary. If
 * lookup falls back toward the one-million figure, the cause was segments. If it does not move, the cause
 * was the dictionary.
 *
 * <p><b>This is the ten million arm, and it is the one that actually decides it.</b> The same comparison at
 * one million gave 29 segments down to 5 for a ratio of 0.81, which is a real contribution and nowhere near
 * enough to explain S28's 1.88x. That run could not settle the question because the growth it was trying to
 * explain happens between one million and ten, so the experiment has to be run where the effect lives.
 *
 * <p>What each outcome means here is sharper than at one million. S28 measured 0.3809 ms at a million and
 * 0.7147 ms at ten. If force merging at ten million brings lookup back toward 0.38, segment count is the
 * whole story and 100M lookup is a design parameter. If it only shaves the same fifth seen at one million,
 * landing near 0.58, then most of the growth is the term dictionary and it is irreducible.
 *
 * <p>Requires a raised heap, as S28 records: both {@code -Dtests.heap.size} and
 * {@code -Poptions.forkOptions.memoryMaximumSize}.
 */
public class LookupGrowthCauseAtTenMillionIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";
    private static final int POPULATION = 10_000_000;
    private static final int IN_FLIGHT = 200;
    private static final int LOOKUPS = 300;

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        // Same reasoning as H14: the framework's accounting plugins cannot hold a population this size.
        return List.of(getTestTransportPlugin(), MockHttpTransport.TestPlugin.class, TestSeedPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testWhetherSegmentCountOrDictionarySizeDrivesTheGrowth() throws Exception {
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
        createDescriptors(POPULATION);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();

        long segmentsBefore = segmentCount();
        double beforeMillis = averageLookupMillis();

        // Collapses segments without shrinking the term dictionary, which is exactly the separation needed.
        client().admin().indices().prepareForceMerge(DESCRIPTORS).setMaxNumSegments(1).get();
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();

        long segmentsAfter = segmentCount();
        double afterMillis = averageLookupMillis();

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH22 lookup growth cause at %,d descriptors%n"
                    + "  segments %d -> %d%n"
                    + "  lookup   %.4f ms -> %.4f ms, ratio %.2f%n"
                    + "  A large fall means segment count drove S28's growth and 100M lookup is a design%n"
                    + "  parameter. No movement means the term dictionary drove it and the growth is%n"
                    + "  irreducible but bounded.%n",
                POPULATION,
                segmentsBefore,
                segmentsAfter,
                beforeMillis,
                afterMillis,
                afterMillis / beforeMillis
            )
        );

        assertTrue("both lookup measurements must be non-zero, or this measured nothing", beforeMillis > 0 && afterMillis > 0);
        assertTrue(
            "the force merge must actually have collapsed segments, or the comparison is between two "
                + "identical states and says nothing: " + segmentsBefore + " -> " + segmentsAfter,
            segmentsAfter < segmentsBefore
        );
    }

    // ---------------------------------------------------------------- helpers

    private long segmentCount() {
        IndicesStatsResponse stats = client().admin().indices().prepareStats(DESCRIPTORS).setSegments(true).get();
        return stats.getTotal().getSegments().getCount();
    }

    private double averageLookupMillis() {
        for (int i = 0; i < 30; i++) {
            client().prepareGet(DESCRIPTORS, nameOf(i * (POPULATION / 30))).get();
        }
        long startedAt = System.nanoTime();
        for (int i = 0; i < LOOKUPS; i++) {
            client().prepareGet(DESCRIPTORS, nameOf((i * (POPULATION / LOOKUPS)) % POPULATION)).get();
        }
        return (System.nanoTime() - startedAt) / 1_000_000.0 / LOOKUPS;
    }

    private void createDescriptors(int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);
        for (int i = 0; i < count; i++) {
            inFlight.acquire();
            String name = nameOf(i);
            client().index(new IndexRequest(DESCRIPTORS).id(name).source("name", name).create(true), new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse response) {
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
        assertTrue("descriptor creation must finish", done.await(20, TimeUnit.MINUTES));
        assertEquals("no descriptor creation may fail", 0, failures.get());
    }

    private static String nameOf(int i) {
        return String.format(Locale.ROOT, "idx-%08d", i);
    }
}
