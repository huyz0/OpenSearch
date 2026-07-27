/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

/**
 * H0b. Where the cost of a cluster state change actually goes, since {@code Metadata.build()} does not
 * account for it.
 *
 * <p>H0a measured {@code Metadata.build()} at 0.67 ms per added index at a population of a thousand and
 * 8.74 ms at ten thousand, so at G2a's largest population of six thousand it is roughly 4 to 5 ms. G2a
 * measured 91.66 ms per index end to end at that same population. Calling {@code build()} the cause was
 * too strong: it is a cause and a small one.
 *
 * <p>The two figures are not perfectly comparable, since G2a runs a hundred creations in flight through
 * a batching executor while H0a is single threaded, but the gap is far too large to be only that. This
 * measures the other candidates against population so the terms can be ranked rather than guessed.
 *
 * <p>What is measured here is the per-change work that scales with the number of indices already
 * present, in the order a create pays it: building the metadata, building the routing table alongside it,
 * and the diffing that publication performs. Listener costs are named in the class javadoc of the audit
 * rather than measured here, because they need a running node.
 */
public class ClusterStateChangeCostTests extends OpenSearchTestCase {

    private static final int[] POPULATIONS = { 1_000, 10_000, 50_000 };
    private static final int REPEATS = 10;

    public void testWhereTheCostGoes() {
        StringBuilder table = new StringBuilder("\nH0b cost of adding one index to an existing population\n");
        table.append(
            String.format(Locale.ROOT, "  %9s %13s %14s %13s %13s%n", "indices", "metadata (ms)", "routing (ms)", "diff (ms)", "total (ms)")
        );

        for (int population : POPULATIONS) {
            ClusterState base = stateWith(population);

            double metadata = time(() -> {
                Metadata.Builder builder = Metadata.builder(base.metadata());
                builder.put(index("added-" + population), false);
                return builder.build();
            });

            double routing = time(() -> {
                RoutingTable.Builder builder = RoutingTable.builder(base.routingTable());
                builder.addAsNew(index("added-routing-" + population));
                return builder.build();
            });

            ClusterState next = ClusterState.builder(base)
                .metadata(Metadata.builder(base.metadata()).put(index("diff-target-" + population), false))
                .build();
            double diff = time(() -> next.diff(base));

            table.append(
                String.format(
                    Locale.ROOT,
                    "  %,9d %13.3f %14.3f %13.3f %13.3f%n",
                    population,
                    metadata,
                    routing,
                    diff,
                    metadata + routing + diff
                )
            );
        }
        logger.warn(table.toString());
    }

    /**
     * The assertion that stops this measuring nothing, and the finding it pins.
     *
     * <p>If every term were flat there would be no ceiling to remove. At least one must grow with
     * population, and naming which one is the point of the exercise.
     */
    public void testAtLeastOneTermGrowsWithPopulation() {
        ClusterState small = stateWith(1_000);
        ClusterState large = stateWith(50_000);

        double metadataSmall = time(() -> Metadata.builder(small.metadata()).put(index("a"), false).build());
        double metadataLarge = time(() -> Metadata.builder(large.metadata()).put(index("a"), false).build());
        double diffSmall = time(() -> stateAdding(small, "b").diff(small));
        double diffLarge = time(() -> stateAdding(large, "b").diff(large));

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH0b growth from 1,000 to 50,000 indices: metadata %.3f -> %.3f ms (%.1fx), diff %.3f -> %.3f ms (%.1fx)%n",
                metadataSmall,
                metadataLarge,
                metadataLarge / metadataSmall,
                diffSmall,
                diffLarge,
                diffLarge / diffSmall
            )
        );

        assertTrue("every measurement must be non-zero, or this measured nothing", metadataSmall > 0 && diffSmall > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "at least one per-change term must grow with population, or there is no ceiling to remove: " + "metadata %.1fx, diff %.1fx",
                metadataLarge / metadataSmall,
                diffLarge / diffSmall
            ),
            metadataLarge > metadataSmall * 5 || diffLarge > diffSmall * 5
        );
    }

    // ---------------------------------------------------------------- helpers

    private static ClusterState stateAdding(ClusterState base, String name) {
        return ClusterState.builder(base).metadata(Metadata.builder(base.metadata()).put(index(name), false)).build();
    }

    private static double time(java.util.function.Supplier<Object> work) {
        long best = Long.MAX_VALUE;
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            long startedAt = System.nanoTime();
            Object produced = work.get();
            best = Math.min(best, System.nanoTime() - startedAt);
            assertNotNull("the work must produce something, or the timing is of nothing", produced);
        }
        return best / 1_000_000.0;
    }

    private static ClusterState stateWith(int population) {
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routing = RoutingTable.builder();
        for (int i = 0; i < population; i++) {
            IndexMetadata index = index("idx-" + i);
            metadata.put(index, false);
            routing.addAsNew(index);
        }
        Metadata built = metadata.build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(built).routingTable(routing.build()).build();
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
