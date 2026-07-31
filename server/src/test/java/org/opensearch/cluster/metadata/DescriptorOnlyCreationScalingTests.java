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

    @After
    public void clearRegistrations() {
        DescriptorOnlyCreation.register(null);
        IndexDescriptorPublisher.register(null);
        IndexDescriptorPublisher.registerCreator(null);
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
     * The kill criterion itself. Creation through the descriptor must not scale with the number of
     * indices already present, which is the single property Area H exists to deliver.
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
        assertTrue(
            String.format(
                Locale.ROOT,
                "creation must stop scaling with population, which is the whole claim of Area H: "
                    + "%.4f ms at 1,000 against %.4f ms at 50,000",
                atThousand,
                atFiftyThousand
            ),
            atFiftyThousand < atThousand * 3
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
