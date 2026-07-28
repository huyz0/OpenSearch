/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.QueryBuilders;
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
 * H14. The descriptor path at a million indices.
 *
 * <p>Every figure Area H has produced tops out at fifty thousand. The curves are flat where flatness is the
 * claim, which is the strongest evidence available without a fleet, and flat to 50k is not flat to 100M. The
 * review that closed the last cycle named this run as the single most valuable next step for that reason.
 *
 * <p>A million is reachable because S26 measured creation at 20,577 per second, so the population itself
 * costs under a minute. That is the whole argument for picking it: it is twenty times more evidence for the
 * same extrapolation, at a cost the test suite can absorb.
 *
 * <p>What is under test is flatness, not speed. A point lookup that holds from 50k to 1M rests the
 * extrapolation on a decade of population rather than on a hunch. One that bends is the finding, and the
 * shape of the bend says whether the problem is the descriptor index or something behind it.
 *
 * <p><b>Two out-of-memory failures before this ran, both the harness rather than the design, and the first
 * diagnosis was incomplete.</b> The first attempt set {@code indices.memory.index_buffer_size} to 512 MB
 * with a thousand requests in flight; every node of an {@code internalClusterTest} shares one JVM, so that
 * buffer was allocated per node inside a single heap. Removing it was not enough and the second attempt
 * died the same way, which is the useful part: the remaining cost is the test framework's own mock
 * plugins, whose leak-tracking directories and engines retain per-operation state precisely so correctness
 * tests can catch leaks. That accounting is what cannot hold a million documents, so this test disables
 * them.
 *
 * <p>Recorded at this length because both failures looked exactly like the finding the test exists to look
 * for. Reporting a harness limit as a scaling ceiling would have been worse than not running it, and the
 * first diagnosis being right but insufficient is the reason the second run was not simply assumed to
 * pass.
 *
 * <p>Deliberately not asserted as an absolute latency. The number that matters is the ratio against the
 * fifty thousand measurement on the same machine in the same run, since comparing across runs produced a
 * wrong conclusion once already in this project.
 */
public class MillionDescriptorIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";
    private static final int BASELINE = 50_000;
    private static final int TARGET = 1_000_000;
    private static final int IN_FLIGHT = 200;
    private static final int LOOKUPS = 200;

    /**
     * Turns off the mock plugins that account for every operation, keeping the ones the framework needs to
     * start a node at all.
     *
     * <p>{@code MockFSIndexStore} tracks every open file, {@code MockEngineFactoryPlugin} wraps every engine
     * operation, and {@code MockSearchService} retains contexts, all so correctness tests can detect leaks.
     * That per-operation retention is exactly what a million-document population cannot afford, and it is
     * what the second out-of-memory failure was made of.
     *
     * <p>Emptying the list entirely does not work and is worth recording rather than quietly fixing: the
     * node then has no HTTP transport and fails to start with {@code Unsupported http.type []}, which is a
     * startup error and not a memory one. The transport and seed plugins stay for that reason.
     *
     * <p>Correctness is not what this test measures, so trading leak detection for population size is right
     * here and wrong nearly everywhere else.
     */
    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return List.of(getTestTransportPlugin(), MockHttpTransport.TestPlugin.class, TestSeedPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testDescriptorPathAtAMillionIndices() throws Exception {
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

        // Fifty thousand first, measured on this machine in this run, so the comparison is not across runs.
        double baselineCreateRate = createDescriptors(0, BASELINE);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();
        double baselineLookupMillis = averageLookupMillis(BASELINE);

        double millionCreateRate = createDescriptors(BASELINE, TARGET - BASELINE);
        client().admin().indices().prepareRefresh(DESCRIPTORS).get();
        double millionLookupMillis = averageLookupMillis(TARGET);

        // trackTotalHits is essential rather than cosmetic: total hits are capped at 10,000 by default, so
        // without it this assertion reads 10,000 at any population above that and would have "verified" a
        // million-document index by finding ten thousand.
        long counted = client().prepareSearch(DESCRIPTORS)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(0)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits()
            .value();

        StringBuilder report = new StringBuilder("\nH14 the descriptor path at a million indices\n");
        report.append(String.format(Locale.ROOT, "  %12s %18s %20s%n", "population", "creation (idx/s)", "point lookup (ms)"));
        report.append(String.format(Locale.ROOT, "  %,12d %18.0f %20.4f%n", BASELINE, baselineCreateRate, baselineLookupMillis));
        report.append(String.format(Locale.ROOT, "  %,12d %18.0f %20.4f%n", TARGET, millionCreateRate, millionLookupMillis));
        report.append(
            String.format(
                Locale.ROOT,
                "%n  creation ratio %.2fx, lookup ratio %.2fx across a twentyfold population increase%n",
                millionCreateRate / baselineCreateRate,
                millionLookupMillis / baselineLookupMillis
            )
        );
        logger.warn(report.toString());

        assertEquals("every descriptor must be there, or the population is not what was measured", TARGET, counted);
        assertTrue("both lookup measurements must be non-zero", baselineLookupMillis > 0 && millionLookupMillis > 0);

        // A point lookup by id is a term lookup, so it should grow with the logarithm of the population at
        // worst. Twenty times the documents costing more than five times the latency would mean something
        // other than the index structure dominates, and that is the case worth failing on.
        assertTrue(
            String.format(
                Locale.ROOT,
                "point lookup must not degrade with population: %.4f ms at %,d against %.4f ms at %,d. "
                    + "A lookup that grows with the population is the descriptor index becoming the new "
                    + "ceiling, which is the one failure that would invalidate the whole approach",
                baselineLookupMillis,
                BASELINE,
                millionLookupMillis,
                TARGET
            ),
            millionLookupMillis < baselineLookupMillis * 5
        );

        assertTrue(
            String.format(
                Locale.ROOT,
                "creation must not collapse with population: %,.0f/s at %,d against %,.0f/s at %,d",
                baselineCreateRate,
                BASELINE,
                millionCreateRate,
                TARGET
            ),
            millionCreateRate > baselineCreateRate / 10
        );
    }

    // ---------------------------------------------------------------- helpers

    /** Average latency of resolving a descriptor by name, sampled across the population. */
    private double averageLookupMillis(int population) {
        // Warmed first, because the first sample would otherwise become the reported number, which is the
        // mistake S21 and S23 both made and had to annotate afterwards.
        for (int i = 0; i < 20; i++) {
            client().prepareGet(DESCRIPTORS, nameOf(i * (population / 20))).get();
        }

        long startedAt = System.nanoTime();
        for (int i = 0; i < LOOKUPS; i++) {
            // Spread across the population rather than clustered, so this measures the index and not one
            // hot segment.
            client().prepareGet(DESCRIPTORS, nameOf((i * (population / LOOKUPS)) % population)).get();
        }
        return (System.nanoTime() - startedAt) / 1_000_000.0 / LOOKUPS;
    }

    /** Creates descriptors and returns the achieved rate in indices per second. */
    private double createDescriptors(int from, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);

        long startedAt = System.nanoTime();
        for (int i = from; i < from + count; i++) {
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
        assertTrue("descriptor creation must finish", done.await(30, TimeUnit.MINUTES));
        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        assertEquals("no descriptor creation may fail, or the rate is measuring rejections", 0, failures.get());
        return count / elapsedSeconds;
    }

    private static String nameOf(int i) {
        return String.format(Locale.ROOT, "idx-%08d", i);
    }
}
