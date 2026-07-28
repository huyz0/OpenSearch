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

    private double createPerSecond(String prefix, boolean serverless) throws Exception {
        CountDownLatch done = new CountDownLatch(BATCH);
        AtomicInteger failures = new AtomicInteger();
        // Capture the first failure rather than only counting. Counting told me 300 creations failed and
        // nothing about why, which cost a whole run.
        java.util.concurrent.atomic.AtomicReference<Exception> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);

        long startedAt = System.nanoTime();
        for (int i = 0; i < BATCH; i++) {
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
        return BATCH / seconds;
    }
}
