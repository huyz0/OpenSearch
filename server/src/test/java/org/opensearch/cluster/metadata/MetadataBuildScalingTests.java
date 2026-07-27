/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;

/**
 * H0a. Whether {@link Metadata.Builder#build()} is the superlinear term G2a measured.
 *
 * <p>G2a measured index creation degrading from 7.4 ms to 98.8 ms per index between populations of 200
 * and 6,000, and named this method as the likely cause: creating an index changes the indices map, so
 * {@code indices.equals(previousMetadata.indices)} is false and every create runs
 * {@code buildMetadataWithRecomputedIndicesLookups}, sweeping every index in the cluster to rebuild six
 * name arrays and the sorted {@code indicesLookup}.
 *
 * <p>That was a reading of the code, and readings in this project have been wrong repeatedly. This
 * measures the exact operation a create performs, {@code Metadata.builder(existing).put(one).build()},
 * against populations large enough for the shape to be unambiguous.
 *
 * <p>Three cases, because a single number would not distinguish the cause from its neighbours:
 *
 * <ul>
 *   <li><b>add one index</b>, which is what creation does and what should be flat if the architecture is
 *       to reach a hundred million</li>
 *   <li><b>rebuild with no change</b>, which takes the fast path and shows what that path costs, since it
 *       still copies six arrays of length N</li>
 *   <li><b>the sweep alone</b>, forcing the recompute without adding anything, which isolates the
 *       iteration from everything else the builder does</li>
 * </ul>
 *
 * <p>An in-JVM measurement rather than a cluster one, deliberately: G2a already measured the whole create
 * path and could not say which part grew. This can reach populations a test cluster cannot.
 */
public class MetadataBuildScalingTests extends OpenSearchTestCase {

    private static final int[] POPULATIONS = { 1_000, 10_000, 50_000, 100_000 };

    /** Enough repeats that JIT warmup is not what is being reported, few enough to finish. */
    private static final int REPEATS = 20;

    public void testBuildCostAgainstPopulation() {
        StringBuilder table = new StringBuilder("\nH0a Metadata.build() cost against population\n");
        table.append(String.format(Locale.ROOT, "  %9s %14s %14s %14s%n", "indices", "add one (ms)", "no change (ms)", "sweep only (ms)"));

        for (int population : POPULATIONS) {
            Metadata base = metadataWith(population);

            double addOne = timeAddingOneIndex(base, population);
            double noChange = timeRebuildWithoutChange(base);
            double sweepOnly = timeForcedSweep(base);

            table.append(String.format(Locale.ROOT, "  %,9d %14.3f %14.3f %14.3f%n", population, addOne, noChange, sweepOnly));
        }
        logger.warn(table.toString());
    }

    /**
     * The load-bearing assertion, so this cannot pass while measuring nothing.
     *
     * <p>Asserts on the ratio rather than an absolute time, since absolute times are machine-dependent
     * and would make this either flaky or meaningless. Adding one index to a population of 100,000 must
     * not cost dramatically more than adding one to a population of 1,000 <em>if</em> the operation is
     * flat. It is not flat, so this asserts the growth is real and would fail if someone made it flat,
     * at which point the assertion should be inverted and this class becomes the regression test for the
     * fix.
     */
    public void testAddingOneIndexGrowsWithPopulation() {
        double atThousand = timeAddingOneIndex(metadataWith(1_000), 1_000);
        double atHundredThousand = timeAddingOneIndex(metadataWith(100_000), 100_000);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH0a add-one cost: %.3f ms at 1,000 indices, %.3f ms at 100,000, ratio %.1fx%n",
                atThousand,
                atHundredThousand,
                atHundredThousand / atThousand
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", atThousand > 0 && atHundredThousand > 0);
        assertTrue(
            String.format(
                Locale.ROOT,
                "adding one index to a hundred thousand must cost more than adding one to a thousand, "
                    + "which is the finding this class exists to pin: %.3f vs %.3f ms",
                atHundredThousand,
                atThousand
            ),
            atHundredThousand > atThousand * 5
        );
    }

    // ---------------------------------------------------------------- measurement

    /** Exactly what index creation does to metadata: take the existing one, add an index, build. */
    private static double timeAddingOneIndex(Metadata base, int population) {
        long best = Long.MAX_VALUE;
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            Metadata.Builder builder = Metadata.builder(base);
            builder.put(index("added-" + population + "-" + repeat), false);
            long startedAt = System.nanoTime();
            Metadata built = builder.build();
            long elapsed = System.nanoTime() - startedAt;
            assertNotNull("the build must produce metadata, or the timing is of nothing", built);
            best = Math.min(best, elapsed);
        }
        return best / 1_000_000.0;
    }

    /** The fast path, which still copies six arrays of length N. */
    private static double timeRebuildWithoutChange(Metadata base) {
        long best = Long.MAX_VALUE;
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            Metadata.Builder builder = Metadata.builder(base);
            long startedAt = System.nanoTime();
            Metadata built = builder.build();
            best = Math.min(best, System.nanoTime() - startedAt);
            assertNotNull(built);
        }
        return best / 1_000_000.0;
    }

    /**
     * The sweep in isolation: remove an index and put it straight back, so the map differs from the
     * previous one by nothing yet the equality check fails and the recompute runs.
     */
    private static double timeForcedSweep(Metadata base) {
        String name = "idx-0";
        IndexMetadata removed = base.index(name);
        long best = Long.MAX_VALUE;
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            Metadata.Builder builder = Metadata.builder(base);
            builder.remove(name);
            builder.put(removed, false);
            long startedAt = System.nanoTime();
            Metadata built = builder.build();
            best = Math.min(best, System.nanoTime() - startedAt);
            assertNotNull(built);
        }
        return best / 1_000_000.0;
    }

    // ---------------------------------------------------------------- fixtures

    private static Metadata metadataWith(int population) {
        Metadata.Builder builder = Metadata.builder();
        for (int i = 0; i < population; i++) {
            builder.put(index("idx-" + i), false);
        }
        return builder.build();
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
