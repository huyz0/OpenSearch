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
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.MockHttpTransport;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * H24. How the descriptor path scales, measured as a curve rather than at a point.
 *
 * <p>The target is a hundred million indices. Reaching it locally is neither necessary nor informative:
 * what decides whether the design holds is the <em>shape</em> of the cost, and a shape needs several points.
 * This measures four, at a hundred thousand, a quarter million, a half million and a million, and projects
 * from the slope.
 *
 * <p><b>Two curves, not one, and the reason is S29.</b> Lookup latency tracks segment count rather than
 * population: at ten million, collapsing 39 segments to 5 moved lookup from 0.5314 ms to 0.3199 ms, and at
 * equal segment counts one million and ten million differed by only 1.05x. So a single curve measured on
 * naturally-merged indices would extrapolate the merge policy while appearing to extrapolate scale. Both
 * are recorded at every checkpoint:
 *
 * <ul>
 *   <li><b>natural</b>, what an untuned deployment sees, which mixes population growth with segment growth</li>
 *   <li><b>force merged</b>, segment count held at one per shard, which isolates population</li>
 * </ul>
 *
 * <p>The merged curve is the one to extrapolate, and the natural curve is what says how much the merge
 * policy is worth.
 *
 * <p><b>What the projection is and is not.</b> Fitting a per-decade factor and applying it twice past a
 * million is arithmetic on a measured slope. It is not a measurement at a hundred million, and it assumes
 * the slope stays constant across two further decades, which is exactly the assumption that a run at scale
 * would test. Stated in the output so the number cannot be quoted as though it were measured.
 */
public class DescriptorScalingCurveIT extends OpenSearchIntegTestCase {

    private static final String DESCRIPTORS = "descriptors";
    private static final int[] CHECKPOINTS = { 100_000, 250_000, 500_000, 1_000_000 };
    private static final long TARGET = 100_000_000L;
    private static final int SHARDS = 5;
    private static final int IN_FLIGHT = 200;
    private static final int LOOKUPS = 300;

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        // Per H14: the framework's accounting plugins retain per-operation state and cannot hold these
        // populations.
        return List.of(getTestTransportPlugin(), MockHttpTransport.TestPlugin.class, TestSeedPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    public void testScalingCurveAndProjectionToOneHundredMillion() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTORS)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("name", "type=keyword")
        );

        // Warms the JVM before any checkpoint is measured. Without this the first checkpoint absorbs
        // class loading and JIT compilation for the whole run, which is enough on its own to invert the
        // measured slope.
        warmUpBeforeMeasuring();

        List<Point> points = new ArrayList<>();
        int created = 0;
        for (int checkpoint : CHECKPOINTS) {
            double rate = createDescriptors(created, checkpoint - created);
            created = checkpoint;
            client().admin().indices().prepareRefresh(DESCRIPTORS).get();

            long naturalSegments = segmentCount();
            double naturalLookup = averageLookupMillis(checkpoint);

            client().admin().indices().prepareForceMerge(DESCRIPTORS).setMaxNumSegments(1).get();
            client().admin().indices().prepareRefresh(DESCRIPTORS).get();

            long mergedSegments = segmentCount();
            double mergedLookup = averageLookupMillis(checkpoint);

            points.add(new Point(checkpoint, rate, naturalSegments, naturalLookup, mergedSegments, mergedLookup));
        }

        long counted = client().prepareSearch(DESCRIPTORS)
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(0)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits()
            .value();

        report(points);

        assertEquals("the final population must be what was measured", CHECKPOINTS[CHECKPOINTS.length - 1], counted);
        for (Point each : points) {
            assertTrue("every lookup measurement must be non-zero at " + each.population, each.naturalLookup > 0 && each.mergedLookup > 0);
            assertTrue(
                "the force merge must collapse segments at "
                    + each.population
                    + ", or the merged curve is "
                    + "the natural curve under another name",
                each.mergedSegments <= each.naturalSegments
            );
        }

        // The claim the projection depends on: with segments held constant, lookup must not climb steeply
        // with population. A tenfold population increase costing more than double would make any
        // extrapolation across two further decades meaningless.
        Point first = points.get(0);
        Point last = points.get(points.size() - 1);
        double decades = Math.log10((double) last.population / first.population);
        double mergedPerDecade = Math.pow(last.mergedLookup / first.mergedLookup, 1.0 / decades);
        assertTrue(
            String.format(
                Locale.ROOT,
                "merged lookup must not climb steeply with population, or projecting to %,d is meaningless: "
                    + "%.4f ms at %,d against %.4f ms at %,d, %.2fx per decade",
                TARGET,
                first.mergedLookup,
                first.population,
                last.mergedLookup,
                last.population,
                mergedPerDecade
            ),
            mergedPerDecade < 2.0
        );

        // The guard the first version lacked. A lookup that improves with population is warmup leaking
        // into the measurement, not a discovery, and projecting from it produces a confidently absurd
        // number. Failing here is correct: the run measured the JVM, not the design.
        assertTrue(
            String.format(
                Locale.ROOT,
                "merged lookup must not improve materially with population: %.4f ms at %,d against %.4f ms "
                    + "at %,d, %.2fx per decade. A falling curve means warmup is still contaminating the "
                    + "earliest checkpoint, and any projection from it is meaningless",
                first.mergedLookup,
                first.population,
                last.mergedLookup,
                last.population,
                mergedPerDecade
            ),
            mergedPerDecade > 0.7
        );
    }

    /**
     * Populates and exercises a throwaway slice so the measured checkpoints run on a warm JVM.
     *
     * <p>Deliberately uses its own index, so the populations reported later are exactly the populations
     * measured and this warmup does not become an invisible addend to the first checkpoint.
     */
    private void warmUpBeforeMeasuring() throws Exception {
        String warmIndex = "warmup";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(warmIndex)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                .setMapping("name", "type=keyword")
        );
        CountDownLatch done = new CountDownLatch(20_000);
        Semaphore inFlight = new Semaphore(IN_FLIGHT);
        for (int i = 0; i < 20_000; i++) {
            inFlight.acquire();
            String name = nameOf(i);
            client().index(new IndexRequest(warmIndex).id(name).source("name", name).create(true), new ActionListener<>() {
                @Override
                public void onResponse(IndexResponse response) {
                    inFlight.release();
                    done.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    inFlight.release();
                    done.countDown();
                }
            });
        }
        assertTrue("warmup population must finish", done.await(10, TimeUnit.MINUTES));
        client().admin().indices().prepareRefresh(warmIndex).get();
        for (int round = 0; round < 3; round++) {
            for (int i = 0; i < 1_000; i++) {
                client().prepareGet(warmIndex, nameOf((i * 20) % 20_000)).get();
            }
        }
        assertAcked(client().admin().indices().prepareDelete(warmIndex));
    }

    // ---------------------------------------------------------------- reporting

    private void report(List<Point> points) {
        StringBuilder out = new StringBuilder("\nH24 descriptor scaling curve\n");
        out.append(
            String.format(
                Locale.ROOT,
                "  %11s %14s %10s %14s %10s %14s%n",
                "population",
                "creation/sec",
                "segments",
                "lookup (ms)",
                "merged seg",
                "merged (ms)"
            )
        );
        for (Point p : points) {
            out.append(
                String.format(
                    Locale.ROOT,
                    "  %,11d %14.0f %10d %14.4f %10d %14.4f%n",
                    p.population,
                    p.creationRate,
                    p.naturalSegments,
                    p.naturalLookup,
                    p.mergedSegments,
                    p.mergedLookup
                )
            );
        }

        Point first = points.get(0);
        Point last = points.get(points.size() - 1);
        double decades = Math.log10((double) last.population / first.population);
        double naturalPerDecade = Math.pow(last.naturalLookup / first.naturalLookup, 1.0 / decades);
        double mergedPerDecade = Math.pow(last.mergedLookup / first.mergedLookup, 1.0 / decades);
        double creationPerDecade = Math.pow(last.creationRate / first.creationRate, 1.0 / decades);

        double decadesToTarget = Math.log10((double) TARGET / last.population);
        double projectedNatural = last.naturalLookup * Math.pow(naturalPerDecade, decadesToTarget);
        double projectedMerged = last.mergedLookup * Math.pow(mergedPerDecade, decadesToTarget);

        out.append(
            String.format(
                Locale.ROOT,
                "%n  measured across %.1f decades:%n"
                    + "    creation      %.2fx per decade%n"
                    + "    lookup        %.2fx per decade  (natural segments)%n"
                    + "    lookup        %.2fx per decade  (segments held at one per shard)%n"
                    + "%n  projected to %,d, which is %.1f decades beyond the last measurement:%n"
                    + "    lookup        %.3f ms  (natural)%n"
                    + "    lookup        %.3f ms  (segments bounded)%n"
                    + "%n  The projection is arithmetic on a measured slope, not a measurement. It assumes the%n"
                    + "  slope holds across two further decades, which is the one thing only a run at scale%n"
                    + "  can test. The bounded figure is the one the design promises, and it is contingent on%n"
                    + "  merge policy being managed, per S29.%n",
                decades,
                creationPerDecade,
                naturalPerDecade,
                mergedPerDecade,
                TARGET,
                decadesToTarget,
                projectedNatural,
                projectedMerged
            )
        );
        logger.warn(out.toString());
    }

    private static final class Point {
        final int population;
        final double creationRate;
        final long naturalSegments;
        final double naturalLookup;
        final long mergedSegments;
        final double mergedLookup;

        Point(int population, double creationRate, long naturalSegments, double naturalLookup, long mergedSegments, double mergedLookup) {
            this.population = population;
            this.creationRate = creationRate;
            this.naturalSegments = naturalSegments;
            this.naturalLookup = naturalLookup;
            this.mergedSegments = mergedSegments;
            this.mergedLookup = mergedLookup;
        }
    }

    // ---------------------------------------------------------------- helpers

    private long segmentCount() {
        IndicesStatsResponse stats = client().admin().indices().prepareStats(DESCRIPTORS).setSegments(true).get();
        return stats.getTotal().getSegments().getCount();
    }

    private double averageLookupMillis(int population) {
        // A full discarded pass rather than a token thirty gets. The first version warmed with 30 and
        // produced a curve where lookup got *faster* with population, projecting 0.013 ms at a hundred
        // million: the first checkpoint was paying cold-JIT cost that later ones did not, so the run
        // measured warmup and reported it as scaling. Warmup must cost the same as the measurement at
        // every checkpoint or the earliest point is penalised and the slope inverts.
        for (int i = 0; i < LOOKUPS; i++) {
            client().prepareGet(DESCRIPTORS, nameOf((i * (population / LOOKUPS)) % population)).get();
        }
        long startedAt = System.nanoTime();
        for (int i = 0; i < LOOKUPS; i++) {
            client().prepareGet(DESCRIPTORS, nameOf((i * (population / LOOKUPS)) % population)).get();
        }
        return (System.nanoTime() - startedAt) / 1_000_000.0 / LOOKUPS;
    }

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
        double elapsed = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        assertEquals("no descriptor creation may fail, or the rate is measuring rejections", 0, failures.get());
        return count / elapsed;
    }

    private static String nameOf(int i) {
        return String.format(Locale.ROOT, "idx-%08d", i);
    }
}
