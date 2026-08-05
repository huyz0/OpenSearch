/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A gated deletion must not queue behind the cluster manager's state update thread.
 *
 * <h2>The same defect as creation, one path over</h2>
 *
 * {@code MetadataDeleteIndexService} already knew a gated deletion writes nothing to cluster state: for an
 * all-gated request its transform publishes tombstones and returns {@code currentState} unchanged, so there
 * is no diff, no publication and no reroute. What it could not avoid was <em>being</em> a cluster state
 * update task -- submitted at URGENT, queued with every other one, folded one at a time on the single
 * {@code clusterManagerService#updateTask} thread.
 *
 * <p>That is exactly what creation looked like before it was admitted off that thread, and it produced the
 * same shape of number: with creation at several thousand per second, deletion sat at a few hundred and
 * consumed the population soak's entire clock.
 *
 * <h2>Why occupancy, and not a rate</h2>
 *
 * A throughput assertion is a flake generator on a shared machine, and it measures the wrong thing anyway.
 * The A/B that justified this change had to be run with creation and resolution as controls precisely
 * because raw rates moved by a factor of three between runs on machine load alone.
 *
 * <p>What can be asserted without ambiguity is occupancy: hold the update thread, and see whether the work
 * still happens. Under the old arrangement a gated deletion queues behind the block and cannot complete;
 * under the new one it never wants the thread.
 *
 * <p><b>The ordinary deletion is the control and is not decoration.</b> Without it, a mistake in the
 * blocking setup would leave the gated assertion passing for the wrong reason, which is the class of false
 * confidence this area keeps producing. The control proves an ordinary deletion genuinely cannot finish
 * while the block is held, so the gated one finishing means something.
 */
public class GatedDeletionOffClusterStateThreadIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /** How long the control is given to prove it is stuck. */
    private static final long CONTROL_WAIT_SECONDS = 3;

    /** How long a gated deletion may take while the thread is held before this counts as queued behind it. */
    private static final long GATED_DEADLINE_SECONDS = 30;

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

    public void testAGatedDeletionCompletesWhileTheClusterStateThreadIsHeld() throws Exception {
        installBlobBackedDescriptorPlane();

        // Created before the thread is blocked, since creating them is what this test is not about.
        client().admin().indices().create(new CreateIndexRequest("gated-doomed").settings(gated())).actionGet();
        client().admin().indices().create(new CreateIndexRequest("plain-doomed").settings(ordinary())).actionGet();

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch held = new CountDownLatch(1);
        blockClusterStateThread(release, held);
        assertTrue("the update thread never reached the blocking task, so this test proves nothing", held.await(30, TimeUnit.SECONDS));

        try {
            // The control. An ordinary deletion needs the thread that is currently held.
            PlainActionFuture<AcknowledgedResponse> ordinary = PlainActionFuture.newFuture();
            client().admin().indices().delete(new DeleteIndexRequest("plain-doomed"), ordinary);
            assertFalse(
                "an ordinary deletion completed while the cluster state update thread was blocked, so the "
                    + "block is not working and the gated assertion below would pass for the wrong reason",
                ordinary.isDone() || awaitQuietly(ordinary)
            );

            // The measurement.
            long started = System.nanoTime();
            PlainActionFuture<AcknowledgedResponse> gated = PlainActionFuture.newFuture();
            client().admin().indices().delete(new DeleteIndexRequest("gated-doomed"), gated);
            AcknowledgedResponse response = gated.actionGet(GATED_DEADLINE_SECONDS, TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

            assertTrue("a gated deletion must be acknowledged on its own terms", response.isAcknowledged());
            logger.warn("gated deletion completed in {} ms with the cluster state update thread held", elapsedMillis);
        } finally {
            release.countDown();
        }
    }

    /**
     * A request naming a gated index and an ordinary one keeps the old path.
     *
     * <p>The bypass fires only when every named index is gated. Taking it for a mixed request would delete
     * the gated half through the descriptor store and drop the rest, since nothing downstream would remove
     * the ordinary index from cluster state -- an acknowledged deletion that half happened.
     */
    public void testAMixedDeletionStillRemovesBoth() throws Exception {
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("mixed-gated").settings(gated())).actionGet();
        client().admin().indices().create(new CreateIndexRequest("mixed-plain").settings(ordinary())).actionGet();

        client().admin().indices().delete(new DeleteIndexRequest("mixed-gated", "mixed-plain")).actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNull("the ordinary half of a mixed deletion must actually be deleted", state.metadata().index("mixed-plain"));

        // Tombstoned, not erased. A deletion has to be remembered rather than represented by absence: a node
        // partitioned during the delete cannot tell "never existed" from "have not looked yet", and would
        // adopt its dangling shard data on rejoin. Asserting the record is gone would have been asserting
        // for the resurrection bug rather than against it.
        org.opensearch.cluster.metadata.IndexDescriptor gated = store().get("mixed-gated");
        assertNotNull("a deleted gated index must leave a durable tombstone behind", gated);
        assertFalse("and that tombstone must say the index no longer exists", gated.exists());
    }

    private BlobDescriptorBackend storeRef;

    private BlobDescriptorBackend store() throws Exception {
        return storeRef;
    }

    @Override
    protected InstalledDescriptorPlane installBlobBackedDescriptorPlane() throws java.io.IOException {
        InstalledDescriptorPlane plane = super.installBlobBackedDescriptorPlane();
        storeRef = plane.points();
        return plane;
    }

    private static Settings ordinary() throws Exception {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }

    private static Settings gated() throws Exception {
        return Settings.builder().put(ordinary()).put("index.serverless_storage.enabled", true).build();
    }

    /**
     * Occupies the elected cluster manager's state update thread until released.
     *
     * <p>Submitted at URGENT so it cannot be overtaken by the deletion under test, which is also URGENT. A
     * lower priority here would let the deletion run ahead of the block and the test would pass without
     * demonstrating anything.
     */
    private void blockClusterStateThread(CountDownLatch release, CountDownLatch held) throws Exception {
        ClusterService clusterService = internalCluster().getCurrentClusterManagerNodeInstance(ClusterService.class);
        clusterService.submitStateUpdateTask("block the cluster state update thread", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                held.countDown();
                release.await();
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                held.countDown();
            }
        });
    }

    /** Whether the future completed within the control window. Any outcome counts as completion. */
    private static boolean awaitQuietly(PlainActionFuture<AcknowledgedResponse> future) throws Exception {
        try {
            future.actionGet(CONTROL_WAIT_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return future.isDone();
        }
    }
}
