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

/**
 * T32. Whether batching has anything left to amortise on the gated creation path.
 *
 * <p>S36 closed the batching question by arithmetic: the cluster manager thread runs about 75 percent busy,
 * so there is a third more headroom on it rather than a multiple. That is an inference from a headroom
 * figure, and the question deserves a direct answer, because "will batching help" was asked again and
 * because reasoning about this area has been wrong often enough to be worth distrusting.
 *
 * <p><b>What batching actually buys, stated before measuring.</b> A batching executor collapses N cluster
 * state tasks into one cluster state publication. S15 measured that as worth one to two orders of magnitude
 * for wake and sleep, because each of those tasks published. So the value of batching is entirely in
 * publications avoided, and the way to know whether any is left is to count publications rather than to
 * reason about CPU.
 *
 * <p>The cluster state version advances once per publication. So this creates indices and watches the
 * version:
 *
 * <ul>
 *   <li>if a gated creation advances the version, each one publishes, and batching has something to
 *       collapse,</li>
 *   <li>if it does not, gated creations already cost zero publications and batching cannot help however it
 *       is arranged, because the thing it removes is already absent.</li>
 * </ul>
 *
 * <p>The ordinary arm is the control and the calibration together: it must advance the version, or the
 * measurement is not sensitive to publications at all and the gated zero would mean nothing.
 */
public class GatedCreationBatchingHeadroomIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int INDICES = 25;

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

    public void testHowManyPublicationsEachKindOfCreationCosts() throws Exception {
        installBlobBackedDescriptorPlane();

        long ordinaryVersions = createAndCountVersions("plain-batch", false);
        long gatedVersions = createAndCountVersions("serverless_gated-batch", true);

        logger.warn(
            String.format(
                Locale.ROOT,
                "%nT32 cluster state versions advanced by %d index creations%n"
                    + "  ordinary  %4d versions   %.2f per index%n"
                    + "  gated     %4d versions   %.2f per index%n",
                INDICES,
                ordinaryVersions,
                ordinaryVersions / (double) INDICES,
                gatedVersions,
                gatedVersions / (double) INDICES
            )
        );

        assertTrue(
            "the control must publish, or this measurement cannot see publications at all and the gated " + "figure would mean nothing",
            ordinaryVersions >= INDICES
        );
        assertEquals(
            "a gated creation must cost no publication. If it costs one, batching has something to collapse "
                + "and S36 closed the question on the wrong grounds",
            0,
            gatedVersions
        );
    }

    /** Creates {@link #INDICES} indices and returns how far the cluster state version moved. */
    private long createAndCountVersions(String prefix, boolean serverless) throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (serverless) {
            settings.put("index.serverless_storage.enabled", true);
        }

        long before = clusterVersion();
        for (int i = 0; i < INDICES; i++) {
            assertTrue(
                client().admin()
                    .indices()
                    .create(new CreateIndexRequest(String.format(Locale.ROOT, "%s-%03d", prefix, i)).settings(settings.build()))
                    .actionGet()
                    .isAcknowledged()
            );
        }
        return clusterVersion() - before;
    }

    private long clusterVersion() throws Exception {
        return client().admin().cluster().prepareState().get().getState().version();
    }
}
