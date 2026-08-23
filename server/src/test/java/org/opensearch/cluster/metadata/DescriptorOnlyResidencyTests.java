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
 * H5. Whether an index created through the gate leaves anything behind in cluster state.
 *
 * <p>This is ceiling 2, which H3 did not address. H3 measured that creation stops <em>costing</em> what
 * the population costs; this measures that it stops <em>adding</em> to the population. The two are
 * separate claims and the first does not imply the second: creation could be fast and still leave an
 * entry, in which case the seventy gigabytes at a hundred million indices would arrive just as surely,
 * only more quickly.
 *
 * <p>The measurement is the count of entries in cluster state after creating many gated indices, which is
 * zero or it is not. That is a stronger and cheaper assertion than measuring retained heap, and it does
 * not depend on a heap estimator being right about what it is walking.
 *
 * <p>C11 measured 698 B per index deferred and 2,944 B materialized. At a hundred million those are 70
 * and 294 GB, on every cluster-manager-eligible node. Zero entries is the only number that makes a
 * hundred million reachable, which is why this asserts zero rather than an improvement.
 */
public class DescriptorOnlyResidencyTests extends OpenSearchTestCase {

    private final TestIndexCreationStrategy strategy = new TestIndexCreationStrategy();

    @Before
    public void registerStrategy() {
        // Phase D3 of core-pluggability-refactor-plan.md: a test-local IndexCreationStrategy, not
        // DescriptorOnlyCreation (relocated into plugins/serverless-storage) -- see TestIndexCreationStrategy's
        // own javadoc.
        IndexCreationStrategyRegistry.register(strategy);
    }

    @After
    public void clearRegistrations() {
        strategy.deactivate();
        TestClaimedIndexLifecycle.uninstall();
        IndexCreationStrategyRegistry.register(null);
    }

    /** The load-bearing one: gated indices must leave no cluster state entry at all. */
    public void testGatedIndicesAddNothingToClusterState() {
        AtomicInteger published = new AtomicInteger();
        TestClaimedIndexLifecycle.install().recording(descriptor -> published.incrementAndGet());
        // T18 split recording from creating, and a gated index now goes through the creator only. The
        // publisher still records ordinary indices from Metadata.Builder, so counting there would count
        // zero for a gated population. The count moves to where the writes actually happen.
        TestClaimedIndexLifecycle.install().creating(descriptor -> {
            published.incrementAndGet();
            return java.util.concurrent.CompletableFuture.completedFuture(Boolean.TRUE);
        });
        strategy.activate(indexMetadata -> true);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        for (int i = 0; i < 1_000; i++) {
            state = MetadataCreateIndexService.clusterStateCreateIndex(
                state,
                Set.of(),
                index("gated-" + i),
                (current, reason) -> current,
                null,
                write -> {}
            );
        }

        assertEquals("a thousand gated creations must leave no metadata entries", 0, state.metadata().indices().size());
        assertEquals("and no routing entries either", 0, state.routingTable().indicesRouting().size());
        assertEquals("while every one of them was recorded as a descriptor", 1_000, published.get());
    }

    /**
     * The contrast, in the same test rather than across runs, since the point is the difference. Ungated
     * indices still accumulate exactly as they always have, which is what makes coexistence real rather
     * than aspirational.
     */
    public void testUngatedIndicesStillAccumulate() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        for (int i = 0; i < 100; i++) {
            state = MetadataCreateIndexService.clusterStateCreateIndex(
                state,
                Set.of(),
                index("ordinary-" + i),
                (current, reason) -> current,
                null,
                write -> {}
            );
        }

        assertEquals("an ordinary index must still get its entry", 100, state.metadata().indices().size());
    }

    /**
     * Mixed, which is the configuration a migration actually runs in. The gate is per index, so both
     * kinds must coexist in one cluster state with only the ungated ones resident.
     */
    public void testGatedAndUngatedCoexist() {
        TestClaimedIndexLifecycle.install().recording(descriptor -> {});
        // T18 split recording from creating: a gated index is created by the creator, and its
        // future is what the acknowledgement waits on. Registering only a publisher would leave
        // createGated returning null, which creation now treats as "no record anywhere".
        TestClaimedIndexLifecycle.install().creating(descriptor -> java.util.concurrent.CompletableFuture.completedFuture(Boolean.TRUE));
        strategy.activate(indexMetadata -> indexMetadata.getIndex().getName().startsWith("serverless-"));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();
        for (int i = 0; i < 50; i++) {
            state = MetadataCreateIndexService.clusterStateCreateIndex(
                state,
                Set.of(),
                index("serverless-" + i),
                (current, reason) -> current,
                null,
                write -> {}
            );
            state = MetadataCreateIndexService.clusterStateCreateIndex(
                state,
                Set.of(),
                index("ordinary-" + i),
                (current, reason) -> current,
                null,
                write -> {}
            );
        }

        assertEquals("only the ungated indices may be resident", 50, state.metadata().indices().size());
        for (int i = 0; i < 50; i++) {
            assertTrue("the ordinary index must be present", state.metadata().hasIndex("ordinary-" + i));
            assertFalse("the serverless one must not be", state.metadata().hasIndex("serverless-" + i));
        }
    }

    /**
     * What this buys, stated as the arithmetic rather than left implied. Reported rather than asserted,
     * because the per-index figures come from C11 and this test does not re-measure them.
     */
    public void testReportTheResidencyArithmetic() {
        long budgetBytes = 8L * 1024 * 1024 * 1024;
        logger.warn(
            String.format(
                Locale.ROOT,
                "%nH5 residency at an 8 GB metadata budget:%n"
                    + "  materialized  2,944 B/index -> %,d indices%n"
                    + "  deferred        698 B/index -> %,d indices%n"
                    + "  gated             0 B/index -> unbounded, descriptors live in an index on disk%n",
                budgetBytes / 2944,
                budgetBytes / 698
            )
        );
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
