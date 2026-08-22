/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * Plan item G3 (plan-100m-index-implementation.md, Area G; Part 0's own "Ceiling 1: Placement" row):
 * S6 measured the ordinary allocator's cold-allocation cost as superlinear -- 4.6 s at 40,000 shards,
 * the figure that should size a cell. The plan's own claim is that under computed placement (Area C)
 * this becomes flat, and states plainly that the claim needs a number. S14/C11's {@code
 * ComputedPlacementCostTests} (deleted at T40, its finding already recorded in
 * SCALABLE_METADATA_SPIKE_RESULTS.md) measured exactly that -- 35,277 ns to build one index's routing
 * table alone, 32,723 ns after 16,000 others exist -- but only the routing-table computation in
 * isolation, called directly, no cluster, no publication, no consensus round trip. This measures the
 * same question against the real path a client's create request actually takes: {@code
 * TransportCreateIndexAction} through cluster-state publication and application, the same thing S6
 * itself measured for the allocator it replaces.
 *
 * <p>Deliberately not asserting on any elapsed-time threshold or cross-tier comparison -- see T40's own
 * "a test asserting on elapsed time is deleted, a test asserting on a count is kept" rule, adopted after
 * wall-clock assertions on shared hardware turned out to be measuring the hardware more often than the
 * code. This class asserts only that each timed create actually completed and is non-zero (the same
 * guard-against-measuring-nothing discipline {@code PublicationLatencyVsClusterSizeIT}, T14, already
 * established for the identical reason); the real numbers are reported via {@code logger.warn} --
 * visible on a passing run, unlike plain {@code logger.info}, since gradle's test runner otherwise only
 * surfaces captured output on failure -- for a human to read and record as a finding, not for the test
 * itself to judge pass or fail by.
 *
 * <p>Deliberately not at S6's own 40,000-shard, or the plan's 100M-index, scale: real index creation is
 * a real cluster-state publication and application, not S14's free in-memory routing computation, so an
 * integration test reaches a much smaller population in reasonable time. Population checkpoints below
 * are two to three orders of magnitude short of the plan's own target -- a real number at this scale is
 * a genuine data point, not proof flatness holds all the way to 100M, the same "measured at small
 * scale, not fully closed" honesty T14 and E8 already applied to this plan.
 *
 * <p>Also scoped narrower than G3's own "time from cluster start to serving" framing: this times the
 * create call's own acknowledgement, the point at which {@code ComputedRoutingTable} has already marked
 * the new index's shard started as part of the very cluster-state computation the create performs (see
 * that class's own "shards are built already started" javadoc) -- matching what S6 measured for the
 * allocator it replaces (a routing decision, not a real shard physically finishing local construction
 * on a data node afterward). A genuinely first real search or write against the new index, and how long
 * that additionally waits, is not measured here.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ComputedPlacementColdStartIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /** Cumulative background population in place before each timed probe creation. */
    private static final int[] POPULATION_CHECKPOINTS = { 0, 300, 1200 };

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), randomRepoPath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    /** One shard, no replicas, computed placement -- the same shape D1's own IT already uses, and no remote store needed to create it. */
    private static Settings computedIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }

    private static String backgroundName(int i) {
        return String.format(Locale.ROOT, "serverless_cold-start-background-%06d", i);
    }

    private static String probeName(int i) {
        return String.format(Locale.ROOT, "serverless_cold-start-probe-%02d", i);
    }

    public void testColdStartLatencyAgainstBackgroundPopulation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        StringBuilder report = new StringBuilder(
            String.format(
                Locale.ROOT,
                "%nG3: cold-start latency under computed placement, real create-to-acknowledged, against a real "
                    + "background population (not S14/C11's routing-table-only microbenchmark)%n%12s  %14s%n",
                "population",
                "probe create"
            )
        );

        int created = 0;
        int probeIndex = 0;
        for (int checkpoint : POPULATION_CHECKPOINTS) {
            while (created < checkpoint) {
                client().admin().indices().prepareCreate(backgroundName(created)).setSettings(computedIndexSettings()).get();
                created++;
            }

            String probeName = probeName(probeIndex++);
            long startNanos = System.nanoTime();
            client().admin().indices().prepareCreate(probeName).setSettings(computedIndexSettings()).get();
            long elapsedNanos = System.nanoTime() - startNanos;

            assertTrue(
                "a real create acknowledgement must take measurable, non-zero time -- a zero reading means "
                    + "this measured nothing rather than something fast",
                elapsedNanos > 0
            );
            assertTrue(
                "the probe index must actually exist after a successful create acknowledgement",
                getClusterState().metadata().hasIndex(probeName)
            );

            report.append(String.format(Locale.ROOT, "%,12d  %,10d ms%n", checkpoint, elapsedNanos / 1_000_000));
        }

        logger.warn(report.toString());
    }
}
