/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * How fast a node gives gated indices back, and what CPU pressure does to that.
 *
 * <h2>The question</h2>
 *
 * {@code GatedIdleEvictionIT} measures the resident peak, and found it exactly where the model predicts on
 * a quiet machine -- arrival rate times the idle window, 20 of 200 -- and at 148 of 200 on a box under an
 * unrelated build. Eviction had not stopped, it had fallen behind.
 *
 * <p>That is the wrong direction for a safety property. The residency ceiling is the whole argument for
 * reaching ten billion shards, and if it weakens exactly when a node is busiest then it is weakest when it
 * matters most. Before designing around that, it is worth knowing which of two things is happening: the
 * sweep not running, or the sweep running and being unable to close fast enough.
 *
 * <h2>Why this measures a drain rather than a peak</h2>
 *
 * A peak conflates arrival rate, idle window and eviction rate. Filling first and then stopping separates
 * them: nothing arrives during the drain, so the time to go from a full population to zero is eviction
 * throughput and nothing else. Running that with and without CPU pressure gives the ratio directly.
 *
 * <p>Opt-in, because it deliberately saturates the machine and would make every test running beside it slow
 * and flaky:
 *
 * <pre>
 * ./gradlew :plugins:serverless-storage:internalClusterTest \
 *     --tests '*GatedEvictionUnderPressureIT*' -Dtests.pressure=true
 * </pre>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedEvictionUnderPressureIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int POPULATION = 100;

    /**
     * Long enough that the whole population is still resident when the fill finishes.
     *
     * <p>The first version set this to two seconds and asserted the population was fully open before
     * timing the drain. It was not: filling a hundred indices takes far longer than two seconds, so the
     * earliest were evicted before the latest were created and the assertion read zero of a hundred. The
     * window has to outlast the fill or there is no full population to drain.
     */
    private static final TimeValue IDLE_AFTER = TimeValue.timeValueSeconds(90);

    private static final TimeValue SWEEP_EVERY = TimeValue.timeValueSeconds(1);

    /** How long a drain may take before the measurement gives up and reports how far it got. */
    private static final long DRAIN_DEADLINE_SECONDS = 240;

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .put(org.opensearch.serverless.storage.ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .put(IndicesClusterStateService.GATED_SHARD_SWEEP_INTERVAL_SETTING.getKey(), SWEEP_EVERY)
            .put(IndicesClusterStateService.GATED_SHARD_IDLE_EVICTION_SETTING.getKey(), IDLE_AFTER)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testHowFastAQuietNodeDrains() throws Exception {
        assumeTrue(
            "set -Dtests.pressure=true; this test saturates the machine",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.pressure", "false"))
        );
        long millis = measureDrain(0);
        logger.warn(
            String.format(Locale.ROOT, "%nDRAIN quiet: %,d indices in %,d ms, %,.1f per second%n", POPULATION, millis, rate(millis))
        );
    }

    public void testHowFastALoadedNodeDrains() throws Exception {
        assumeTrue(
            "set -Dtests.pressure=true; this test saturates the machine",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.pressure", "false"))
        );
        int burners = Math.max(1, Runtime.getRuntime().availableProcessors());
        long millis = measureDrain(burners);
        logger.warn(
            String.format(
                Locale.ROOT,
                "%nDRAIN under %d burner threads: %,d indices in %,d ms, %,.1f per second%n",
                burners,
                POPULATION,
                millis,
                rate(millis)
            )
        );
    }

    private static double rate(long millis) throws Exception {
        return POPULATION / Math.max(0.001, millis / 1000.0);
    }

    /**
     * Fills, stops, and times the drain to zero, optionally against {@code burners} busy threads.
     *
     * <p>The fill is not timed and the burners start only once it is done, so the number reported is
     * eviction throughput rather than a mixture of eviction and arrival.
     */
    private long measureDrain(int burners) throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        IndicesService indices = internalCluster().getInstance(IndicesService.class, dataNode);

        for (int i = 0; i < POPULATION; i++) {
            String name = String.format(Locale.ROOT, "serverless_drain-%04d", i);
            client().admin().indices().create(new CreateIndexRequest(name).settings(gated())).actionGet();
            client().prepareIndex(name).setId("1").setSource("tenant", name).get();
        }
        assertBusy(
            () -> assertEquals("the whole population must be open before the drain is timed", POPULATION, gatedOpen(indices)),
            120,
            TimeUnit.SECONDS
        );

        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> load = new java.util.ArrayList<>();
        for (int i = 0; i < burners; i++) {
            Thread t = new Thread(() -> {
                // Deliberately branch-unpredictable arithmetic, so the JIT cannot fold it away and leave a
                // thread that reports as busy while consuming nothing.
                long x = 1;
                while (stop.get() == false) {
                    x = x * 6364136223846793005L + 1442695040888963407L;
                    if (x == 42) {
                        Thread.yield();
                    }
                }
            }, "eviction-pressure-" + i);
            t.setDaemon(true);
            load.add(t);
            t.start();
        }

        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DRAIN_DEADLINE_SECONDS);

            // Timed from the first eviction rather than from here, because the idle window has to outlast
            // the fill and waiting it out is not part of eviction throughput. What is being measured is how
            // fast the node closes indices once it has decided to, not how long it waits before deciding.
            int remaining = gatedOpen(indices);
            while (remaining == POPULATION && System.nanoTime() < deadline) {
                Thread.sleep(100);
                remaining = gatedOpen(indices);
            }
            long firstEviction = System.nanoTime();

            while (remaining > 0 && System.nanoTime() < deadline) {
                Thread.sleep(100);
                remaining = gatedOpen(indices);
            }
            long elapsed = (System.nanoTime() - firstEviction) / 1_000_000;
            if (remaining > 0) {
                logger.warn("drain did not finish: {} of {} still resident after {} ms", remaining, POPULATION, elapsed);
            }
            return elapsed;
        } finally {
            stop.set(true);
            for (Thread t : load) {
                t.join(5_000);
            }
        }
    }

    private static int gatedOpen(IndicesService indices) throws Exception {
        int count = 0;
        for (IndexService indexService : indices) {
            if (indexService.getIndexSettings().getSettings().getAsBoolean("index.serverless_storage.enabled", false)) {
                count++;
            }
        }
        return count;
    }

    private static Settings gated() throws Exception {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
