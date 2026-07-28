/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * C11. The claim the whole area exists to make, as a number rather than an argument.
 *
 * <p>S6 measured OpenSearch's allocator: 4.6 s of cold allocation at 40,000 shards, superlinear, with
 * the 100,000-shard tier exceeding a twenty-minute suite timeout. That curve is what makes 10 billion
 * shards unreachable, and it is not a constant factor that tuning improves.
 *
 * <p>Computed placement should be flat per shard, because nothing is global: each entry is a function of
 * one index's descriptor and the node list, and no pass considers other indices. This measures whether
 * that holds, by building entries for a growing population and checking the per-shard cost does not
 * climb.
 *
 * <p>Two things this deliberately does <b>not</b> claim. It does not measure a running cluster, so it is
 * evidence about the algorithm rather than about the system. And the honest comparison against S6 is not
 * "faster": it is that the superlinear pass does not happen at all, because under this design a
 * serverless index publishes no routing entry and there is nothing to allocate. Building N entries here
 * is the pessimistic case where every index is touched at once, which a real cluster never does.
 */
public class ComputedPlacementCostTests extends OpenSearchTestCase {

    private static final int[] INDEX_COUNTS = { 1_000, 4_000, 16_000 };
    private static final int SHARDS_PER_INDEX = 10;

    public void testPerShardCostDoesNotClimbWithPopulation() {
        List<String> nodes = nodes(50);
        double firstPerShard = 0;
        double lastPerShard = 0;

        for (int i = 0; i < INDEX_COUNTS.length; i++) {
            int indexCount = INDEX_COUNTS[i];
            List<IndexMetadata> population = population(indexCount);

            // Warm the JIT on a slice so the first tier is not measuring class loading.
            for (int w = 0; w < Math.min(200, indexCount); w++) {
                ComputedRoutingTable.build(population.get(w), nodes, 3);
            }

            long start = System.nanoTime();
            for (IndexMetadata metadata : population) {
                ComputedRoutingTable.build(metadata, nodes, 3);
            }
            long elapsedNanos = System.nanoTime() - start;

            long shards = (long) indexCount * SHARDS_PER_INDEX;
            double perShardNanos = (double) elapsedNanos / shards;
            logger.info(
                "{} indices ({} shards): {} ms total, {} ns/shard",
                indexCount,
                shards,
                elapsedNanos / 1_000_000,
                String.format(Locale.ROOT, "%.0f", perShardNanos)
            );

            if (i == 0) {
                firstPerShard = perShardNanos;
            }
            lastPerShard = perShardNanos;
        }

        // The property, stated as a bound rather than a timing. S6's allocator went from 25 ms at 2,000
        // shards to 445 ms at 40,000, which is a 4.5x rise in per-shard cost over a 20x population
        // increase. Flat means the ratio stays near 1; the bound is loose because a unit test on a
        // shared machine is noisy, and anything under 2x already separates this from a superlinear curve.
        double ratio = lastPerShard / firstPerShard;
        logger.info("per-shard cost ratio across a 16x population increase: {}", String.format(Locale.ROOT, "%.2f", ratio));
        assertTrue("per-shard cost climbed " + ratio + "x across a 16x population increase", ratio < 2.0);
    }

    /**
     * The stronger property, and the one that actually removes the ceiling: a single index costs the same
     * regardless of how many other indices exist. The allocator cannot say this, because its pass is over
     * the whole cluster.
     */
    public void testOneIndexCostsTheSameRegardlessOfClusterSize() {
        List<String> nodes = nodes(50);
        IndexMetadata subject = index("subject", SHARDS_PER_INDEX);

        for (int i = 0; i < 500; i++) {
            ComputedRoutingTable.build(subject, nodes, 3);
        }

        long alone = timeOne(subject, nodes);
        // Build a large population first, so anything that scaled with cluster size would show up.
        List<IndexMetadata> crowd = population(16_000);
        for (IndexMetadata metadata : crowd) {
            ComputedRoutingTable.build(metadata, nodes, 3);
        }
        long crowded = timeOne(subject, nodes);

        logger.info("one index: {} ns alone, {} ns after 16,000 others", alone, crowded);
        assertTrue("cost of one index rose from " + alone + " to " + crowded + " ns", crowded < alone * 3);
    }

    private static long timeOne(IndexMetadata metadata, List<String> nodes) {
        long start = System.nanoTime();
        for (int i = 0; i < 1_000; i++) {
            ComputedRoutingTable.build(metadata, nodes, 3);
        }
        return (System.nanoTime() - start) / 1_000;
    }

    private static List<IndexMetadata> population(int count) {
        List<IndexMetadata> population = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            population.add(index("tenant-" + String.format(Locale.ROOT, "%07d", i), SHARDS_PER_INDEX));
        }
        return population;
    }

    private static IndexMetadata index(String name, int shards) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(shards)
            .numberOfReplicas(0)
            .build();
    }

    private static List<String> nodes(int count) {
        List<String> nodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            nodes.add("node-" + i);
        }
        return nodes;
    }
}
