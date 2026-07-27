/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * G2a. What an index population actually costs to build, before committing to the plan's 1M.
 *
 * <p>G2 asks for a synthetic 1M-index cluster exercising create, search, wildcard, scale and node
 * failure, and picked 1M so it could run in CI. Nothing has measured whether it can. C11 measured
 * metadata at 698 B/index deferred and 2,944 B/index materialized at three shards, so the population
 * alone is somewhere between 0.7 and 2.9 GB at a million, before any cost of creating it. The creation
 * rate is unmeasured entirely.
 *
 * <p>So this measures the rate at counts small enough to run, and reports what a million would take at
 * that rate. A G2 built on an unreachable number is worth less than a smaller G2 that runs, and which of
 * those is on offer is a measurement rather than a judgement.
 *
 * <p>Creations are issued concurrently and unacknowledged shard counts are tolerated, because what is
 * being measured is the cluster's ability to absorb the population rather than any single request's
 * latency. The assertion that every index exists at the end is what stops that tolerance from turning
 * into a measurement of requests that quietly failed.
 */
public class IndexPopulationScaleIT extends OpenSearchIntegTestCase {

    /**
     * Raised because the default stops this measurement long before the interesting part.
     *
     * <p>{@code cluster.max_shards_per_node} defaults to 1000, so a three-data-node test cluster refuses
     * the 3001st single-shard index with "this cluster currently has [3000]/[3000] maximum shards open".
     * That is the ceiling S12 says computed placement removes, met here as a hard validation error, and
     * it is worth naming: reaching a million indices is not only a question of time and heap, it requires
     * this limit to be raised by three orders of magnitude.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("cluster.max_shards_per_node", 100_000).build();
    }

    private static final String DESCRIPTOR_INDEX = "descriptors";

    /**
     * The Area H claim, measured against the thing it replaces in the same harness.
     *
     * <p>Index creation writes an entry into the cluster state metadata map, and H0a measured that
     * rebuild at 107 ms for one index once a hundred thousand exist. Area H proposes writing a descriptor
     * document instead, with {@code op_type=create} giving put-if-absent from the single shard owning the
     * id, and no cluster state update at all.
     *
     * <p>If that claim holds, descriptor creation is flat in the population where index creation is not,
     * and the two arms of this measurement diverge. If it does not hold, Area H is attacking the wrong
     * thing and should stop before any production code depends on it.
     *
     * <p>The population here is descriptors in one index rather than indices in a cluster, which is
     * exactly the substitution Area H proposes. Uniqueness is asserted rather than assumed: a create that
     * silently overwrote an existing id would look flat and prove nothing.
     */
    public void testDescriptorCreationRateAgainstPopulation() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(DESCRIPTOR_INDEX)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
        );

        StringBuilder table = new StringBuilder("\nH-spike descriptor creation cost, ").append(internalCluster().size()).append(" nodes\n");
        int created = 0;
        for (int population : POPULATIONS) {
            int toCreate = population - created;
            long startedAt = System.nanoTime();
            createDescriptors(created, toCreate);
            long elapsedNanos = System.nanoTime() - startedAt;
            created = population;

            double perDescriptorMillis = (elapsedNanos / 1_000_000.0) / toCreate;
            table.append(
                String.format(
                    Locale.ROOT,
                    "  population=%,7d  batch=%,6d  %6.3f ms/descriptor  projected 1M = %5.1f min%n",
                    population,
                    toCreate,
                    perDescriptorMillis,
                    perDescriptorMillis * 1_000_000 / 60_000
                )
            );
        }
        logger.warn(table.toString());

        // Uniqueness, which is what makes a descriptor write a legitimate replacement for the cluster
        // state entry rather than merely a faster one.
        org.opensearch.action.index.IndexRequest duplicate = new org.opensearch.action.index.IndexRequest(DESCRIPTOR_INDEX).id("scale-0")
            .source("name", "scale-0")
            .create(true);
        expectThrows(org.opensearch.index.engine.VersionConflictEngineException.class, () -> client().index(duplicate).actionGet());
    }

    private void createDescriptors(int from, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(IN_FLIGHT);
        for (int i = 0; i < count; i++) {
            inFlight.acquire();
            String name = "scale-" + (from + i);
            client().index(
                new org.opensearch.action.index.IndexRequest(DESCRIPTOR_INDEX).id(name)
                    .source("name", name, "uuid", name + "-uuid", "shards", 1)
                    .create(true),
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
        assertTrue("descriptor creation must finish within the budget", done.await(10, TimeUnit.MINUTES));
        assertEquals("no descriptor write may fail, or the rate is meaningless", 0, failures.get());
    }

    /** Small enough to finish, large enough for the rate to be meaningful rather than startup noise. */
    private static final int[] POPULATIONS = { 200, 1_000, 3_000, 6_000 };

    public void testIndexCreationRateAndCost() throws Exception {
        StringBuilder table = new StringBuilder("\nG2a index population cost, ").append(internalCluster().size()).append(" nodes\n");

        int created = 0;
        for (int population : POPULATIONS) {
            int toCreate = population - created;
            long startedAt = System.nanoTime();
            createIndices(created, toCreate);
            long elapsedNanos = System.nanoTime() - startedAt;
            created = population;

            ClusterState state = client().admin().cluster().prepareState().get().getState();
            int inMetadata = state.metadata().indices().size();
            assertEquals("every index asked for must exist, or the rate is of requests that failed", population, inMetadata);

            double elapsedMillis = elapsedNanos / 1_000_000.0;
            double perIndexMillis = elapsedMillis / toCreate;
            table.append(
                String.format(
                    Locale.ROOT,
                    "  population=%,7d  batch=%,6d took %8.1f ms  %6.2f ms/index  projected 1M = %6.1f min%n",
                    population,
                    toCreate,
                    elapsedMillis,
                    perIndexMillis,
                    perIndexMillis * 1_000_000 / 60_000
                )
            );
        }
        logger.warn(table.toString());
    }

    /**
     * Bounded in-flight requests, and the bound is a correction rather than a tuning knob.
     *
     * <p>An earlier version issued the whole batch at once and the cluster fell over at two thousand
     * concurrent creations, with nodes disconnecting. That measured the harness's tolerance for
     * simultaneous requests, not its capacity to hold a population, and reporting it as the latter would
     * have been wrong in the direction that makes the architecture look worse than it is.
     */
    private static final int IN_FLIGHT = 100;

    private void createIndices(int from, int count) throws Exception {
        CountDownLatch done = new CountDownLatch(count);
        AtomicInteger failures = new AtomicInteger();
        java.util.concurrent.Semaphore inFlight = new java.util.concurrent.Semaphore(IN_FLIGHT);
        StringBuilder firstFailure = new StringBuilder();
        for (int i = 0; i < count; i++) {
            inFlight.acquire();
            client().admin()
                .indices()
                .prepareCreate("scale-" + (from + i))
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
                // Waiting for shards would measure allocation rather than the metadata path, and G2's
                // question is about the population.
                .setWaitForActiveShards(ActiveShardCount.NONE)
                .setTimeout(TimeValue.timeValueMinutes(2))
                .execute(new ActionListener<CreateIndexResponse>() {
                    @Override
                    public void onResponse(CreateIndexResponse response) {
                        inFlight.release();
                        done.countDown();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        failures.incrementAndGet();
                        synchronized (firstFailure) {
                            if (firstFailure.length() == 0) {
                                firstFailure.append(e.toString());
                            }
                        }
                        inFlight.release();
                        done.countDown();
                    }
                });
        }
        assertTrue("index creation must finish within the budget", done.await(10, TimeUnit.MINUTES));
        assertEquals("no creation may fail, or the rate is meaningless. First failure: " + firstFailure, 0, failures.get());
    }
}
