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
import org.opensearch.action.support.PlainActionFuture;
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
 * A gated creation must not queue behind the cluster manager's state update thread.
 *
 * <h2>What was wrong, and why nothing caught it</h2>
 *
 * Gating was implemented as a branch at the bottom of the ordinary creation path. A gated index reached
 * {@code MetadataCreateIndexService.clusterStateCreateIndex}, took the branch, wrote a descriptor and returned
 * the cluster state unchanged. Every claim made about it was true: zero cluster state versions per creation,
 * zero bytes, measured and asserted by {@code GatedCreationBatchingHeadroomIT} and
 * {@code GatedCreationClusterStateFootprintIT}.
 *
 * <p>All of those measure what a gated creation <em>writes</em>. None of them measures what it <em>occupies</em>.
 * The request was still submitted as an URGENT cluster state update task and still did all of its validation
 * on the single {@code clusterManagerService#updateTask} thread before reaching the branch that decided there
 * was nothing to publish. Profiling a three node cluster put twenty-seven percent of all on-CPU samples on
 * that one thread, roughly 3.9 ms of single-threaded CPU per creation, which is a hard ceiling that no amount
 * of client concurrency can move -- the batch executor folds tasks one at a time, so batching amortises the
 * publication that gating had already made free and not the validation that remained.
 *
 * <p>So the metadata was externalised and the admission was not, and every existing test agreed the design was
 * working because every existing test asked about bytes and versions.
 *
 * <h2>Why this test is shaped as an occupancy test</h2>
 *
 * The only way to observe "did not use that thread" is to make the thread unavailable and see whether the work
 * still happens. So the update thread is deliberately blocked by a task that parks in {@code execute}, and a
 * gated index is created while it is held. Under the old arrangement that creation queues behind the block and
 * cannot complete; under the new one it never wants the thread at all.
 *
 * <p><b>The ordinary creation is the control, and it is not decoration.</b> Without it a bug in the blocking
 * setup -- a task that never reached the executor, a latch released early -- would leave the gated assertion
 * passing for the wrong reason, which is exactly the class of false confidence this whole area keeps
 * producing. The control asserts that an ordinary creation genuinely cannot finish while the block is held, so
 * the gated one finishing means something.
 */
public class GatedCreationOffClusterStateThreadIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /** How long the control is given to prove it is stuck. Long enough to be convincing, short enough to run. */
    private static final long CONTROL_WAIT_SECONDS = 3;

    /** How long a gated creation may take while the thread is blocked before this counts as queued behind it. */
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

    public void testAGatedCreationCompletesWhileTheClusterStateThreadIsHeld() throws Exception {
        installGate();

        // The mapping projection index is created by the first gated creation that carries a mapping, and
        // creating it is itself an ordinary cluster state update. Doing that here, before the thread is
        // blocked, keeps the test measuring the steady state rather than the bootstrap -- otherwise the
        // first gated creation would be waiting on an index creation that genuinely does need the thread,
        // and would fail for a reason that has nothing to do with what this is asserting.
        createGated("gated-warmup");

        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch held = new CountDownLatch(1);
        blockClusterStateThread(release, held);
        assertTrue("the update thread never reached the blocking task, so this test proves nothing", held.await(30, TimeUnit.SECONDS));

        try {
            // The control. An ordinary creation needs the thread that is currently held, so it must not get
            // anywhere while the block is in place.
            PlainActionFuture<CreateIndexResponse> ordinary = PlainActionFuture.newFuture();
            client().admin().indices().create(new CreateIndexRequest("plain-blocked").settings(plainSettings()), ordinary);
            assertFalse(
                "an ordinary creation completed while the cluster state update thread was blocked, so the "
                    + "block is not working and the gated assertion below would pass for the wrong reason",
                ordinary.isDone() || awaitQuietly(ordinary)
            );

            // The measurement.
            long started = System.nanoTime();
            PlainActionFuture<CreateIndexResponse> gated = PlainActionFuture.newFuture();
            client().admin().indices().create(new CreateIndexRequest("gated-blocked").settings(gatedSettings()), gated);
            CreateIndexResponse response = gated.actionGet(GATED_DEADLINE_SECONDS, TimeUnit.SECONDS);
            long elapsedMillis = (System.nanoTime() - started) / 1_000_000;

            assertTrue("a gated creation must be acknowledged on its own terms", response.isAcknowledged());
            logger.warn("gated creation completed in {} ms with the cluster state update thread held", elapsedMillis);
        } finally {
            release.countDown();
        }

        // And the index is real once the block is gone: resolvable by exact name, which for a gated index is
        // the descriptor being readable rather than a metadata entry existing.
        assertTrue(
            "the gated index created off the cluster state thread must exist afterwards, or it was fast " + "because it did nothing",
            store.get("gated-blocked") != null
        );
    }

    /**
     * An index that asks for gating but cannot be represented by a descriptor still gets created.
     *
     * <p>This is the fallback the admission check makes possible and would otherwise make dangerous. The check
     * reads request settings, so this request is admitted off the cluster state thread; the real gate then
     * reads the finished metadata, sees an alias with a filter, and declines. Nothing has been written at that
     * point, so the request has to start again on the ordinary path.
     *
     * <p>Worth a test of its own because the failure it guards against is silent: an index admitted for the
     * fast path and then dropped when the gate declined would be a creation that returned success and produced
     * nothing, which is the exact shape of defect this branch has already found twice.
     */
    public void testAnIndexThatAsksForGatingButCannotBeRepresentedIsStillCreated() throws Exception {
        installGate();

        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-refused").settings(gatedSettings())
                    .alias(new org.opensearch.action.admin.indices.alias.Alias("filtered").filter("{\"term\":{\"tenant\":\"a\"}}"))
            )
            .actionGet();

        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertNotNull(
            "an index the descriptor cannot represent must keep its cluster state entry rather than falling " + "between the two paths",
            state.metadata().index("gated-refused")
        );
        assertTrue(
            "and its alias filter must have survived the round trip",
            state.metadata().index("gated-refused").getAliases().containsKey("filtered")
        );
    }

    /** The store the gate was installed against, so this can read a descriptor without going through an API. */
    private BlobDescriptorBackend store;

    private void installGate() throws Exception {
        store = installBlobBackedDescriptorPlane().points();
    }

    private static Settings plainSettings() throws Exception {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }

    private static Settings gatedSettings() throws Exception {
        return Settings.builder().put(plainSettings()).put("index.serverless_storage.enabled", true).build();
    }

    private void createGated(String name) throws Exception {
        client().admin().indices().create(new CreateIndexRequest(name).settings(gatedSettings())).actionGet();
    }

    /**
     * Occupies the elected cluster manager's state update thread until the latch is released.
     *
     * <p>Submitted at URGENT so it cannot be overtaken by the creation under test, which is also URGENT. A
     * lower priority here would let the creation run ahead of the block and the test would pass without ever
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
    private static boolean awaitQuietly(PlainActionFuture<CreateIndexResponse> future) throws Exception {
        try {
            future.actionGet(CONTROL_WAIT_SECONDS, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return future.isDone();
        }
    }
}
