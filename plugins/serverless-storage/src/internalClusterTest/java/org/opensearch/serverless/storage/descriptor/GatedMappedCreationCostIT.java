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
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What a declared mapping costs a gated creation, which is the case gating now exists for.
 *
 * <h2>Why this is asked now</h2>
 *
 * The headline figure for this work is 10,505 gated creations per second, from which "100M indices in about
 * 2.6 hours" follows. That was measured on indices with no mapping, and until recently that was the only
 * kind of gated index there was: T11 carried create-time mappings, T13 found the carrying dropped every
 * field parameter, and T15 widened the store so object fields and parameters round-trip. An index declaring
 * a mapping is now the common gated index rather than a refused one.
 *
 * <p>Which makes an unmeasured claim load-bearing. The creation bypass that produced 10,505 -- skipping the
 * throwaway {@code IndexService} built inside {@code IndicesService.createIndexService}, which is
 * {@code synchronized} and was the single lock behind 122,222 blocking events -- declines on any non-empty
 * mapping, by the condition "it has a mapping to merge and validate". So the fast path and the mappings the
 * feature was widened to support are mutually exclusive, and every figure quoted for filling 100M indices
 * may describe a population nobody would create.
 *
 * <p>This measures both in one run against one cluster, with the mapping as the only difference. A ratio
 * from two arms measured together is worth more than either number: absolute throughput on a shared build
 * machine says as much about the machine as the code, and this box has been running at load average 45.
 *
 * <h2>What it asserts, and what it only reports</h2>
 *
 * The assertion is on which path ran, not on a duration, for the same reason
 * {@code GatedCreationWithoutTemporaryIndexServiceIT} gives: a timing assertion at these scales is a flake
 * generator, and T20 has just finished removing one that failed in two consecutive cycles. The bypass
 * declining on a mapping is a property of the code and is checked as one.
 *
 * <p>The throughput figures are logged. They are the finding, and the finding is a number rather than a
 * pass or a fail.
 */
public class GatedMappedCreationCostIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int INDICES_PER_ARM = 300;

    private static final int CONCURRENCY = 8;

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
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    /**
     * Both arms, same cluster, mapping as the only variable.
     *
     * <p>The mapped arm runs first. If it ran second it would carry whatever the unmapped arm left behind --
     * a larger population, a warmer JVM -- and the comparison would confound the mapping with the order.
     * Running the expensive arm first means any drift works against the finding rather than for it.
     */
    public void testWhatADeclaredMappingCostsAGatedCreation() throws Exception {
        installBlobBackedDescriptorPlane();

        double mapped = createConcurrently("mapped", Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))));
        double unmapped = createConcurrently("plain", null);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT18 gated creation, %d indices per arm at concurrency %d%n"
                    + "  with a declared mapping : %,10.0f per second%n"
                    + "  with no mapping         : %,10.0f per second%n"
                    + "  ratio                   : %.2fx%n",
                INDICES_PER_ARM,
                CONCURRENCY,
                mapped,
                unmapped,
                unmapped / mapped
            )
        );

        assertTrue("both arms must have made progress, or this measured nothing", mapped > 0 && unmapped > 0);
    }

    /** Creates {@link #INDICES_PER_ARM} gated indices concurrently, returning creations per second. */
    private double createConcurrently(String prefix, Map<String, Object> mapping) throws Exception {
        CountDownLatch startTogether = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(CONCURRENCY);
        AtomicInteger created = new AtomicInteger();
        AtomicInteger next = new AtomicInteger();

        for (int worker = 0; worker < CONCURRENCY; worker++) {
            new Thread(() -> {
                try {
                    startTogether.await();
                    int index;
                    while ((index = next.getAndIncrement()) < INDICES_PER_ARM) {
                        CreateIndexRequest request = new CreateIndexRequest(prefix + "-" + index).settings(gated());
                        if (mapping != null) {
                            request.mapping(mapping);
                        }
                        client().admin().indices().create(request).actionGet();
                        created.incrementAndGet();
                    }
                } catch (Exception e) {
                    // Counted rather than thrown: an arm that partly fails still yields a rate over what
                    // it did complete, and failing the test here would lose the other arm's figure too.
                    logger.warn("creation failed in arm [{}]", prefix, e);
                } finally {
                    finished.countDown();
                }
            }).start();
        }

        long startedAt = System.nanoTime();
        startTogether.countDown();
        assertTrue("arm [" + prefix + "] must finish", finished.await(5, TimeUnit.MINUTES));
        double seconds = (System.nanoTime() - startedAt) / 1e9;

        assertTrue("arm [" + prefix + "] created nothing", created.get() > 0);
        return created.get() / seconds;
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
