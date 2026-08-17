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
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * H3b. The kill criterion for Area H, measured against the same operation that condemned the old path.
 *
 * <p>S19 and S20 established that adding one index to an existing population costs work proportional to
 * that population: 1.61 ms of metadata rebuild at a thousand indices, 14.34 at ten thousand, 69.31 at
 * fifty thousand, because {@code Metadata.build()} recomputes derived structures over every index on every
 * change. That is ceiling 3, and it is what makes a hundred million indices unreachable.
 *
 * <p>This measures the same call with the gate open, where creation records a descriptor and writes
 * nothing to cluster state. The claim is that the cost stops depending on the population. If it does not,
 * something else is superlinear and Area H should stop rather than continue, which is why this is written
 * as a kill criterion rather than a regression test.
 *
 * <p>The comparison is against the gated path in the same run and on the same machine, so the two numbers
 * are directly comparable in a way S19's cluster measurement and S20's in-JVM measurement were not. That
 * mismatch produced a wrong conclusion once already and is worth not repeating.
 */
public class DescriptorOnlyCreationScalingTests extends OpenSearchTestCase {

    private static final int[] POPULATIONS = { 1_000, 10_000, 50_000 };
    private static final int REPEATS = 10;

    @Before
    public void registerCreationStrategyBridge() {
        // D2's final slice moved clusterStateCreateIndex's skipsClusterState consultation onto
        // IndexCreationStrategyRegistry -- see ServerlessNamespaceTests's own javadoc for the pre-existing
        // gap this same bridge closes.
        IndexCreationStrategyRegistry.register(new SupplierBackedIndexCreationStrategy());
    }

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.register(null);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
        IndexCreationStrategyRegistry.register(null);
    }

    public void testCreationCostAgainstPopulation() {
        StringBuilder table = new StringBuilder("\nH3b creation cost against existing population\n");
        table.append(
            String.format(Locale.ROOT, "  %9s %20s %20s %10s%n", "indices", "cluster state (ms)", "descriptor only (ms)", "ratio")
        );

        for (int population : POPULATIONS) {
            ClusterState base = stateWith(population);

            DescriptorOnlyCreation.register(null);
            IndexDescriptorPublisher.register(null);
            IndexDescriptorPublisher.registerCreator(null);
            double throughClusterState = time(base, "cs-" + population);

            AtomicInteger published = new AtomicInteger();
            IndexDescriptorPublisher.register(descriptor -> published.incrementAndGet());
            // T18 split recording from creating, and a gated index now goes through the creator only. The
            // publisher still records ordinary indices from Metadata.Builder, so counting there would count
            // zero for a gated population. The count moves to where the writes actually happen.
            IndexDescriptorPublisher.registerCreator(descriptor -> {
                published.incrementAndGet();
                return java.util.concurrent.CompletableFuture.completedFuture(Boolean.TRUE);
            });
            DescriptorOnlyCreation.register(indexMetadata -> true);
            double throughDescriptor = time(base, "desc-" + population);

            assertTrue("the gated path must actually have published descriptors, or it measured nothing", published.get() > 0);

            table.append(
                String.format(
                    Locale.ROOT,
                    "  %,9d %20.3f %20.3f %9.1fx%n",
                    population,
                    throughClusterState,
                    throughDescriptor,
                    throughClusterState / throughDescriptor
                )
            );
        }
        logger.warn(table.toString());
    }

    /**
     * The kill criterion, asserted on what creation does rather than on how long it takes.
     *
     * <p>Creation through the descriptor must not scale with the number of indices already present, which
     * is the single property Area H exists to deliver. This asserted that with a stopwatch and a 3x bound
     * on a few microseconds per operation, and produced a false failure in two consecutive cycles: it fails
     * whenever the box is busy and passes alone, so what it was measuring was the machine.
     *
     * <p>The property has a deterministic witness. A gated creation returns the caller's {@code ClusterState}
     * instance unchanged -- it writes a descriptor and rebuilds no metadata -- and an operation that does not
     * touch the population cannot scale with it. Identity is that statement, exactly, with no clock in it.
     *
     * <p>The timing is kept and still asserted, at a bound loose enough to survive a loaded machine, because
     * identity alone would not catch a population-dependent scan that happened to leave the state alone. It
     * is a backstop against a regression of that shape rather than the measurement, and the figures are
     * logged either way. {@link #testCreationCostAgainstPopulation} is where the numbers are for
     * reading.
     */
    public void testDescriptorOnlyCreationIsFlatAgainstPopulation() {
        IndexDescriptorPublisher.register(descriptor -> {});
        // T18 split recording from creating: a gated index is created by the creator, and its
        // future is what the acknowledgement waits on. Registering only a publisher would leave
        // createGated returning null, which creation now treats as "no record anywhere".
        IndexDescriptorPublisher.registerCreator(descriptor -> java.util.concurrent.CompletableFuture.completedFuture(Boolean.TRUE));
        DescriptorOnlyCreation.register(indexMetadata -> true);

        double atThousand = time(stateWith(1_000), "flat-small");
        double atFiftyThousand = time(stateWith(50_000), "flat-large");

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH3b descriptor-only creation: %.4f ms at 1,000 indices, %.4f ms at 50,000, ratio %.2fx%n",
                atThousand,
                atFiftyThousand,
                atFiftyThousand / atThousand
            )
        );

        assertTrue("both measurements must be non-zero, or this measured nothing", atThousand > 0 && atFiftyThousand > 0);

        // The deterministic half: creation returns the state it was given, at either population, so it
        // rebuilt no metadata and therefore did no work proportional to the population.
        for (int population : new int[] { 1_000, 50_000 }) {
            ClusterState base = stateWith(population);
            ClusterState result = MetadataCreateIndexService.clusterStateCreateIndex(
                base,
                Set.of(),
                index("identity-" + population),
                (state, reason) -> state,
                null,
                write -> {}
            );
            assertSame(
                "a gated creation must return the cluster state it was given at population "
                    + population
                    + ". A new instance means metadata was rebuilt, which is the cost that scales and the "
                    + "one thing this path exists to avoid",
                base,
                result
            );
        }

        // The backstop: loose enough that only a regression in the shape of the cost, rather than a busy
        // machine, can trip it. A tight bound here is what made this test a contention detector.
        assertTrue(
            String.format(
                Locale.ROOT,
                "creation must stop scaling with population, which is the whole claim of Area H: "
                    + "%.4f ms at 1,000 against %.4f ms at 50,000",
                atThousand,
                atFiftyThousand
            ),
            atFiftyThousand < atThousand * 20
        );
    }

    /**
     * The safety property that makes the gate shippable. An index that skips cluster state with no
     * publisher installed would exist nowhere, so creation must refuse rather than succeed silently.
     */
    public void testCreationRefusesWhenNothingWouldRecordTheIndex() {
        DescriptorOnlyCreation.register(indexMetadata -> true);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);

        IllegalStateException failure = expectThrows(
            IllegalStateException.class,
            () -> MetadataCreateIndexService.clusterStateCreateIndex(
                ClusterState.builder(ClusterName.DEFAULT).build(),
                Set.of(),
                index("nowhere"),
                (state, reason) -> state,
                null
            )
        );

        assertTrue(
            "the refusal must say the index would have no record: " + failure.getMessage(),
            failure.getMessage().contains("no record of it anywhere")
        );
    }

    /** With the gate closed, creation must behave exactly as it always has. */
    public void testWithTheGateClosedCreationIsUnchanged() {
        ClusterState created = MetadataCreateIndexService.clusterStateCreateIndex(
            ClusterState.builder(ClusterName.DEFAULT).build(),
            Set.of(),
            index("ordinary"),
            (state, reason) -> state,
            null,
            write -> {}
        );

        assertTrue("an ungated index must still get its cluster state entry", created.metadata().hasIndex("ordinary"));
    }

    // ---------------------------------------------------------------- helpers

    private static double time(ClusterState base, String prefix) {
        long best = Long.MAX_VALUE;
        for (int repeat = 0; repeat < REPEATS; repeat++) {
            IndexMetadata created = index(prefix + "-" + repeat);
            long startedAt = System.nanoTime();
            ClusterState result = MetadataCreateIndexService.clusterStateCreateIndex(
                base,
                Set.of(),
                created,
                (state, reason) -> state,
                null,
                write -> {}
            );
            best = Math.min(best, System.nanoTime() - startedAt);
            assertNotNull("creation must return a state, or the timing is of nothing", result);
        }
        return best / 1_000_000.0;
    }

    private static ClusterState stateWith(int population) {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < population; i++) {
            metadata.put(index("existing-" + i), false);
        }
        return ClusterState.builder(ClusterName.DEFAULT).metadata(metadata.build()).build();
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
