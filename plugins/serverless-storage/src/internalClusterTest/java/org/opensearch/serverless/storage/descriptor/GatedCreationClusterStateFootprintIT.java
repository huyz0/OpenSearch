/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * What a gated index costs the cluster state, in bytes.
 *
 * <h2>Why bytes and not just versions</h2>
 *
 * {@code GatedCreationBatchingHeadroomIT} establishes that a gated creation advances the cluster state
 * version zero times, which is what makes batching pointless on that path. That is a claim about
 * <em>publications</em>, and it is the right claim for the batching question, but it is not the claim R6
 * rests on.
 *
 * <p>R6 is that per-index cluster state cost goes to zero rather than getting smaller. A design could
 * publish nothing on creation and still accumulate state: entries added to a map that rides along in some
 * later, unrelated publication would cost zero versions per creation and still put 100M entries in every
 * node's heap and in every full cluster state transfer. Version count cannot see that. Bytes can.
 *
 * <p>So this serialises the whole cluster state before and after, and asserts the gated arm adds nothing
 * while the ordinary arm adds something. The ordinary arm is the calibration: if it did not grow, the
 * measurement would not be sensitive to index metadata at all and the gated zero would mean nothing, which
 * is the same reasoning the batching test uses for its control.
 *
 * <h2>What "nothing" has to allow for</h2>
 *
 * Not literally zero. A live cluster writes to its own state for reasons unrelated to these creations, and
 * this runs against a real node rather than a mock. The assertion is therefore that the gated arm's growth
 * is a small constant, unrelated to how many indices were created, while the ordinary arm's growth is
 * proportional to them. Stated as: gated growth must be below what a single ordinary index costs. That
 * comparison is derived from the same run on the same cluster, so it needs no absolute threshold and no
 * tuning as index metadata changes size.
 */
public class GatedCreationClusterStateFootprintIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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

    public void testWhatEachKindOfCreationAddsToTheClusterState() throws Exception {
        installBlobBackedDescriptorPlane();

        long ordinaryGrowth = createAndMeasureGrowth("plain-footprint", false);
        long gatedGrowth = createAndMeasureGrowth("gated-footprint", true);

        long perOrdinaryIndex = ordinaryGrowth / INDICES;

        logger.warn(
            String.format(
                Locale.ROOT,
                "%ncluster state bytes added by %d index creations%n"
                    + "  ordinary  %,10d bytes   %,8d per index%n"
                    + "  gated     %,10d bytes%n",
                INDICES,
                ordinaryGrowth,
                perOrdinaryIndex,
                gatedGrowth
            )
        );

        assertTrue(
            "the control must grow the cluster state, or this measurement cannot see index metadata at all "
                + "and the gated figure would mean nothing",
            perOrdinaryIndex > 0
        );
        assertTrue(
            "a gated index must not accumulate cluster state. "
                + INDICES
                + " of them added "
                + gatedGrowth
                + " bytes, which is at least what one ordinary index costs ("
                + perOrdinaryIndex
                + "), so something is being retained per gated index even though nothing is published per "
                + "gated index",
            gatedGrowth < perOrdinaryIndex
        );
    }

    /** Creates {@link #INDICES} indices and returns how many bytes the serialised cluster state grew by. */
    private long createAndMeasureGrowth(String prefix, boolean serverless) throws Exception {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (serverless) {
            settings.put("index.serverless_storage.enabled", true);
        }

        long before = clusterStateBytes();
        for (int i = 0; i < INDICES; i++) {
            assertTrue(
                client().admin()
                    .indices()
                    .create(new CreateIndexRequest(String.format(Locale.ROOT, "%s-%03d", prefix, i)).settings(settings.build()))
                    .actionGet()
                    .isAcknowledged()
            );
        }
        return clusterStateBytes() - before;
    }

    /**
     * The serialised size of the whole cluster state.
     *
     * <p>Serialised rather than estimated from a heap walk: this is the quantity that is published to every
     * node and written to the remote store, so it is the one that has to stay flat at 100M indices.
     */
    private long clusterStateBytes() throws Exception {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            state.writeTo(out);
            return out.bytes().length();
        }
    }
}
