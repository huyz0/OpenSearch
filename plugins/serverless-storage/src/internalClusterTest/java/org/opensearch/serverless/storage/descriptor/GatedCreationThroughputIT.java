/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * P9. What creating a gated index actually costs, through the API that creates it.
 *
 * <p>S26 reported 20,577 creations per second and S30 about 26,500. Neither called {@code prepareCreate} for
 * the indices it counted: both wrote descriptor documents with {@code client.index} and timed that. So both
 * measured descriptor write throughput and neither measured index creation.
 *
 * <p>That is the same defect S22 already had, where 0.0005 ms turned out to be the cost of <em>not</em>
 * publishing rather than the cost of creating, and it recurred in the number written to replace it. Twice in
 * one area, so this test creates indices.
 *
 * <p><b>What the gate does and does not remove.</b> A gated creation returns the cluster state unchanged, so
 * it skips the {@code Metadata} rebuild that S20 measured at 69 ms per change at fifty thousand indices, and
 * it skips the publication. It does not skip the queue: the skip happens inside {@code CreateIndexTask},
 * which is an {@code AckedClusterStateUpdateTask} submitted to the single-threaded cluster state executor,
 * and the whole settings and template pipeline runs before the result is discarded.
 *
 * <p>Both arms run in the same cluster so the comparison is not across runs, which is the mistake S19 and
 * S20 made once already.
 *
 * <p><b>P10 used this to test a hypothesis that turned out to be wrong.</b> The gate returns the cluster
 * state unchanged from inside a cluster state task, so queueing looked like the cap. Skipping the queue
 * entirely, verified firing with twenty hits and no misses, moved gated creation from 235 to 245 per
 * second, which is noise. The queue is not the bottleneck; the creation pipeline is, and the change was
 * reverted rather than kept for a benefit that could not be measured on the most safety-critical path in
 * the system.
 */
public class GatedCreationThroughputIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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

    private static final int BATCH = 20;
    private static final int IN_FLIGHT = 5;

    @Override
    protected boolean addMockInternalEngine() {
        // A serverless index installs its own engine factory, and the framework's mock engine collides
        // with it: "multiple engine factories provided".
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // The base class does not register it, and without the plugin the node rejects both
        // serverless_storage.base_path and index.serverless_storage.enabled as unknown settings.
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // A serverless index needs a container to resolve, or every gated creation fails on the shard
        // rather than on the path being measured. A shared local directory stands in for the object store,
        // matching what the other serverless integration tests do.
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Serverless storage requires remote cluster state cluster-wide, and a container to resolve.
            // Without both, every gated creation fails validation and the run measures rejections.
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            .build();
    }

    @After
    public void clearGate() {
        DescriptorGate.uninstall();
    }

    public void testGatedCreationAgainstOrdinaryCreation() throws Exception {
        DescriptorGate.install(
            new DescriptorStore(client(), 1),
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );

        // Ordinary first, so the gated arm cannot benefit from a warmer JVM. S30 is the reason: an
        // under-warmed first arm inverted a whole curve.
        double ordinary = createPerSecond("ordinary", false);
        double gated = createPerSecond("gated", true);
        double ordinaryAgain = createPerSecond("ordinary2", false);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nP9 creation through the real API, %d indices per arm%n"
                    + "  ordinary       %,10.0f per second%n"
                    + "  gated          %,10.0f per second%n"
                    + "  ordinary again %,10.0f per second   (warmup control)%n"
                    + "  gated is %.2fx ordinary%n"
                    + "  S26 and S30 reported ~20,000 to ~26,500 per second, measuring descriptor document%n"
                    + "  writes rather than index creation.%n",
                BATCH,
                ordinary,
                gated,
                ordinaryAgain,
                gated / ordinary
            )
        );

        assertTrue("every arm must be non-zero, or this measured nothing", ordinary > 0 && gated > 0 && ordinaryAgain > 0);
    }

    /**
     * T14. Whether gated creation scales with concurrency, which decides whether 235 per second is a real
     * ceiling or an artefact of how P9 drove it.
     *
     * <p>P9 measured 235 per second with five requests in flight and the plan has quoted it ever since,
     * including as the reason a hundred million indices takes 4.9 days. That figure is only a ceiling if
     * creation is serialised somewhere.
     *
     * <p>Two measurements say it may not be. P10 removed the cluster state queue entirely and throughput did
     * not move, and T13 found a cluster state publication costs 14 to 27 ms while a gated creation costs
     * 4.3 ms, so gating is skipping the round trip rather than queueing behind it. What remains is
     * per-request pipeline work, and per-request work should scale with concurrent requests until something
     * shared saturates.
     *
     * <p>So this sweeps concurrency at a fixed batch. Flat means 235 is a real serialisation point and the
     * 4.9 day figure stands. Rising means the ceiling was the harness, and the number the plan quotes is
     * measuring how hard P9 pushed rather than what the system does.
     *
     * <p><b>Measured, two runs:</b>
     *
     * <pre>
     *   in flight    run A    run B
     *           1      189      145 per second
     *           5      222      317 per second
     *          20      443      386 per second
     *          50      499      492 per second
     *         100        -      520 per second
     *         200        -      566 per second
     * </pre>
     *
     * <p><b>It rises, so 235 was not a ceiling.</b> The five in flight row reproduces P9, which is what
     * makes the rest credible: the harness is the same, only the pressure changed. Throughput saturates
     * around 500 to 570 per second, with four times the concurrency from fifty to two hundred buying 1.15x,
     * so that is a real asymptote rather than a point on a line.
     *
     * <p>Individual figures are soft, since run to run they move by a third at the same concurrency. The
     * asymptote and the shape are what survive repetition.
     *
     * <p><b>What this changes.</b> A hundred million indices is about 2.1 days at 550 per second rather than
     * 4.9 days at 235, so the plan's figure overstates the cost by roughly 2.3x. That is worth correcting
     * and it is not a reprieve: two days is still a bulk migration, and whether the asymptote rises with
     * cluster size is a separate question this single cluster cannot answer.
     */
    public void testGatedCreationAgainstConcurrency() throws Exception {
        DescriptorGate.install(
            new DescriptorStore(client(), 1),
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );

        int[] concurrencies = { 1, 5, 20, 50, 100, 200 };
        int batch = 100;
        StringBuilder table = new StringBuilder("\nT14 gated creation against concurrency, " + batch + " indices per arm\n");
        table.append(String.format(Locale.ROOT, "  %12s %18s %14s%n", "in flight", "creations/sec", "vs 1"));

        // Warmed with the same work, since an under-warmed first arm is how S30 inverted a whole curve.
        createPerSecond("warm", true, 20, 5);

        double atOne = 0;
        for (int concurrency : concurrencies) {
            double rate = createPerSecond("conc-" + concurrency, true, batch, concurrency);
            if (concurrency == concurrencies[0]) {
                atOne = rate;
            }
            table.append(String.format(Locale.ROOT, "  %12d %18.0f %13.2fx%n", concurrency, rate, rate / atOne));
        }

        table.append("\n  P9 measured 235/sec at five in flight and the plan quotes it as the ceiling.\n");
        logger.warn(table.toString());

        assertTrue("the measurement must be non-zero, or this measured nothing", atOne > 0);
    }

    private double createPerSecond(String prefix, boolean serverless) throws Exception {
        return createPerSecond(prefix, serverless, BATCH, IN_FLIGHT);
    }

    private double createPerSecond(String prefix, boolean serverless, int batch, int concurrency) throws Exception {
        CountDownLatch done = new CountDownLatch(batch);
        AtomicInteger failures = new AtomicInteger();
        // Capture the first failure rather than only counting. Counting told me 300 creations failed and
        // nothing about why, which cost a whole run.
        java.util.concurrent.atomic.AtomicReference<Exception> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
        Semaphore inFlight = new Semaphore(concurrency);

        long startedAt = System.nanoTime();
        for (int i = 0; i < batch; i++) {
            inFlight.acquire();
            Settings.Builder settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
            if (serverless) {
                settings.put("index.serverless_storage.enabled", true);
            }
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(String.format(Locale.ROOT, "%s-%05d", prefix, i)).settings(settings.build()),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(CreateIndexResponse response) {
                            inFlight.release();
                            done.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            firstFailure.compareAndSet(null, e);
                            failures.incrementAndGet();
                            inFlight.release();
                            done.countDown();
                        }
                    }
                );
        }
        assertTrue("creation must finish", done.await(10, TimeUnit.MINUTES));
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        if (failures.get() > 0) {
            logger.warn("P9 first creation failure for arm [{}]", prefix, firstFailure.get());
            throw new AssertionError(
                "no creation may fail, or the rate is measuring rejections: " + failures.get() + " failed, first was",
                firstFailure.get()
            );
        }
        return batch / seconds;
    }
}
