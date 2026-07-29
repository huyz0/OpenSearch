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
 * T15. A sustained creation load, long enough for a sampling profiler to see where the time goes.
 *
 * <p>S34 established that gated creation saturates near 550 per second, so about 1.8 ms per index. S26
 * measured a descriptor document write at 20,577 per second, about 0.049 ms, which is 2.7 percent of that.
 * So the storage write is not the cost and batching it cannot be the answer, which is worth knowing before
 * building a batcher rather than after.
 *
 * <p>S32 already removed the other obvious candidate: skipping the cluster state queue entirely moved
 * throughput by nothing. S33 removed the third, since publication costs 14 to 27 ms and a gated creation
 * costs 1.8 ms, so creation is not waiting on one.
 *
 * <p>That leaves the pipeline, and reading it turns up a strong suspect.
 * {@code MetadataCreateIndexService.applyCreateIndexWithTemporaryService} calls
 * {@code indicesService.withTempIndexService}, whose own comment says it exists to "create the index here
 * (on the master) to validate it can be created, as well as adding the mapping". A real {@code IndexService}
 * with a {@code MapperService}, analysis registry and query shard context, constructed and thrown away per
 * creation.
 *
 * <p><b>This class does not test that hypothesis, it only provides the load to profile.</b> P10 is the
 * reason for the distinction: it had an equally strong reading of the code, built the fix, watched it fire,
 * measured no change and was reverted. A suspect identified by reading is a place to point the profiler,
 * not a conclusion.
 *
 * <p>Run under a recorder, for example:
 *
 * <pre>{@code
 * ./gradlew :plugins:serverless-storage:internalClusterTest \
 *   --tests "*GatedCreationProfileIT" -Dbuild.docker=false \
 *   -Dtests.jvm.argline="-XX:StartFlightRecording=settings=profile,filename=/tmp/creation.jfr"
 * }</pre>
 *
 * <p><b>The profile's first finding was about the measurement, not the system.</b> Of 1,045 execution
 * samples, 18.7 percent touch the Java security manager and 8.6 percent touch the jacoco coverage agent.
 * The single hottest leaf frame in the whole recording is {@code ArraysSupport.mismatch}, at 11 percent,
 * which is file permission path comparison. Neither agent exists in production.
 *
 * <p>Removing them, at three thousand indices and a hundred in flight:
 *
 * <pre>
 *   jacoco + security manager      536 per second     what every earlier figure was measured under
 *   security manager off           690 per second     1.29x
 *   both off                       859 per second     1.60x
 * </pre>
 *
 * <p>So S26, S31, S34 and this class's own first run all understate creation by about 1.6x. A hundred
 * million indices is roughly <b>1.35 days</b> at 859 per second, against the 4.9 days the plan quoted from
 * S31 and the 2.1 days S34 corrected it to. Two corrections to the same number, and neither of the earlier
 * ones was looking at the JVM it ran on.
 *
 * <p><b>What the profile says about the system, once that is subtracted.</b> The busiest thread by a wide
 * margin is {@code clusterManagerService#updateTask} at 27.2 percent of all samples, and there is exactly
 * one of it. Within that thread, index metadata and settings construction is 13.4 percent and
 * {@code withTempIndexService} is only 4.6 percent, which kills the hypothesis this class was written to
 * test: the temporary {@code IndexService} is real but it is not the cost.
 *
 * <p>That is the second hypothesis about this path to die on contact with a profiler, after S32, and for the
 * same reason both times. Reading the code makes the expensive-looking thing look expensive.
 *
 * <p><b>An open contradiction worth stating rather than smoothing over.</b> S32 removed the cluster state
 * queue entirely and throughput did not move, which says the queue is not the limiter. This profile says
 * the single cluster manager task thread is the busiest thing in the system by a factor of two.
 *
 * <p><b>Resolved by re-profiling with both agents off</b>, which is the only profile worth optimising
 * against. 523 samples, zero infrastructure contamination, and {@code jdk.ThreadCPULoad} answers the
 * question directly: the cluster manager task thread runs at <b>3.76 percent of total CPU on a twenty core
 * machine, where one fully busy thread reads 5.0 percent</b>. So it is about 75 percent busy. It is the
 * leading constraint by a factor of two over anything else, and it is not saturated.
 *
 * <p>That reconciles S32 without needing the concurrency explanation: removing work from a thread with
 * headroom gives sub-linear gains, so S32's null result is what a 75 percent busy serialisation point
 * predicts. The clean concurrency sweep agrees, saturating near 850 per second between fifty and a hundred
 * in flight and declining slightly at two hundred.
 *
 * <p><b>Where the cluster manager thread's time goes, cleanly measured:</b>
 *
 * <pre>
 *   settings subsystem                      ~20%   Setting.get, getRaw, exists, AbstractScopedSettings
 *   temporary IndexService and analysis     ~15-20%  createIndexService, newIndexService, AnalysisRegistry
 *   descriptor write submission             ~10%   TransportBulkAction, IngestService.resolvePipelines
 * </pre>
 *
 * <p>Note the first entry, because the earlier contaminated profile hid it and the second entry is the one
 * everybody guesses. Validating index settings against the scoped settings registry costs more than
 * building the throwaway {@code IndexService} that the code makes so visible.
 *
 * <p><b>Why nothing here was optimised.</b> Each of the three has a problem that measurement will not
 * solve. Skipping the temporary {@code IndexService} for a gated index is defensible, since its mappings go
 * to the mapping store rather than cluster state and T7 already refuses to gate an index with alias
 * filters, but it moves mapping validation from creation time to first use, which is a semantic trade
 * rather than an optimisation. Caching built analyzers is core OpenSearch behaviour affecting every index
 * creation, not just gated ones. Moving the descriptor write submission off this thread is safe and worth
 * perhaps five percent, and needs a {@code ThreadPool} plumbed through {@code DescriptorGate.install} to
 * get it.
 *
 * <p>At 850 per second a hundred million indices is 1.35 days. The honest position is that the remaining
 * wins are a semantic decision rather than an engineering one, and that decision belongs to whoever owns
 * the mapping validation contract.
 */
public class GatedCreationProfileIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /** Enough work that a sampling profiler gets thousands of samples rather than dozens. */
    private static final int INDICES = 3_000;

    /** Past the knee S34 measured, so the profile is of a saturated system rather than an idle one. */
    private static final int IN_FLIGHT = 100;

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
            .build();
    }

    @After
    public void clearGate() {
        DescriptorGate.uninstall();
    }

    public void testSustainedGatedCreation() throws Exception {
        DescriptorGate.install(
            new DescriptorStore(client(), 1),
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );

        CountDownLatch done = new CountDownLatch(INDICES);
        AtomicInteger failures = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<Exception> firstFailure = new java.util.concurrent.atomic.AtomicReference<>();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);

        long startedAt = System.nanoTime();
        for (int i = 0; i < INDICES; i++) {
            inFlight.acquire();
            client().admin()
                .indices()
                .create(
                    new CreateIndexRequest(String.format(Locale.ROOT, "profile-%06d", i)).settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put("index.serverless_storage.enabled", true)
                            .build()
                    ),
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
        assertTrue("creation must finish", done.await(30, TimeUnit.MINUTES));
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT15 sustained gated creation%n  %,d indices in %.1f s at %d in flight%n  %.0f per second%n",
                INDICES,
                seconds,
                IN_FLIGHT,
                INDICES / seconds
            )
        );

        if (failures.get() > 0) {
            logger.warn("T15 first failure", firstFailure.get());
            throw new AssertionError("no creation may fail, or the profile is of rejections: " + failures.get(), firstFailure.get());
        }
    }
}
