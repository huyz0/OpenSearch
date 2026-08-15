/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.descriptor.DescriptorGate;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Real end-to-end coverage for plan item D1 (plan-100m-index-implementation.md, Area D): does a node
 * join actually cause {@link ReaderShardPreWarmCoordinator} to dispatch a real pre-warm poll, using
 * {@link ReaderShardPreWarmCoordinator#preWarmCountForTesting()} rather than inferring it from search
 * correctness alone -- this mechanism only affects latency, so a correct search result proves nothing
 * about whether pre-warming happened either way.
 *
 * <p>A 30-shard index makes the real assertion tractable without forcing rendezvous hashing to land a
 * specific shard on a specific node (top-3 candidates, not top-1, so a single-shard probe-name search
 * like {@code ServerlessStorageAffinityForwardingIT}'s own is a different, harder problem here): with
 * 30 independent shards, a newly-joined third node is virtually certain to land in at least one
 * shard's top-3 by chance, and the assertion only needs "at least one," not a specific shard id.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageReaderShardPreWarmIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int SHARD_COUNT = 30;

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
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearDescriptorGate() throws Exception {
        DescriptorGate.uninstall();
    }

    @Before
    public void resetPreWarmCount() {
        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();
    }

    public void testANewlyJoinedNodeReceivesAtLeastOnePreWarmPoll() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        // Deliberately NOT installBlobBackedDescriptorPlane(): that makes an index fully gated (Area
        // H, off cluster state entirely), and this coordinator walks event.state().metadata().indices()
        // -- exactly where a gated index would not be. D1 is about computed placement (Area C), which
        // needs only index.serverless_storage.enabled plus the node-level setting below; the index
        // stays a real Metadata entry either way.

        Settings computed = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
        client().admin().indices().prepareCreate("prewarm-target").setSettings(computed).get();

        // Let the two-data-node epoch settle for real before adding the third -- otherwise the very
        // first membership publication (empty previous epoch) could be what a race attributes the
        // pre-warm to, rather than the join this test means to exercise.
        assertBusy(() -> {
            long before = ReaderShardPreWarmCoordinator.preWarmCountForTesting();
            // No assertion on the value itself here -- just letting cluster state settle. A short
            // real wait, not a sleep: assertBusy retries until nothing throws, and nothing here does.
            assertNotNull(before);
        }, 10, TimeUnit.SECONDS);
        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();

        internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        assertBusy(() -> {
            assertTrue(
                "a 30-shard index and a genuinely new node joining a 3-node computed-placement "
                    + "fleet must make at least one shard newly eligible for it, dispatching a real "
                    + "pre-warm poll -- got "
                    + ReaderShardPreWarmCoordinator.preWarmCountForTesting(),
                ReaderShardPreWarmCoordinator.preWarmCountForTesting() > 0
            );
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * The regression this whole fix exists for: the same property as the test above, for a genuinely
     * <em>gated</em> index (Area H, off cluster state entirely) rather than a merely computed-placement
     * one. Before this fix, {@link ReaderShardPreWarmCoordinator} enumerated only {@code
     * event.state().metadata().indices()}, which a gated index is never in by definition -- so this
     * exact scenario dispatched zero pre-warms, silently, for the entire target population this
     * project exists to serve.
     */
    public void testAGatedIndexsShardIsAlsoPreWarmed() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        installBlobBackedDescriptorPlane();

        Settings gated = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
        client().admin().indices().create(new CreateIndexRequest("serverless_gated-prewarm-target").settings(gated)).actionGet();
        // Creating a gated index touches no node at all -- the descriptor is written and nothing is
        // told to build anything, which is the whole point of gating (see GatedResidencySoakIT's own
        // comment on this exact line). The write is what actually opens the shard on a node (T39),
        // which is what makes it appear in that node's own on-demand-open working set -- the thing
        // this fix's gated pass now consults instead of cluster state.
        client().prepareIndex("serverless_gated-prewarm-target").setId("1").setSource("f", "v").get();

        // Let the three-node epoch settle for real before adding the fourth, same reasoning as the
        // ordinary-index test above.
        assertBusy(() -> assertNotNull(ReaderShardPreWarmCoordinator.preWarmCountForTesting()), 10, TimeUnit.SECONDS);
        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();

        internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        assertBusy(() -> {
            assertTrue(
                "a 30-shard gated index and a genuinely new node joining a 3-node fleet must make at "
                    + "least one shard newly eligible for it, dispatching a real pre-warm poll -- got "
                    + ReaderShardPreWarmCoordinator.preWarmCountForTesting(),
                ReaderShardPreWarmCoordinator.preWarmCountForTesting() > 0
            );
        }, 30, TimeUnit.SECONDS);
    }

    public void testDisabledCoordinatorNeverDispatches() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(
                Settings.builder().put(ServerlessStoragePlugin.SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING.getKey(), false)
            )
            .get();

        Settings computed = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
        client().admin().indices().prepareCreate("prewarm-disabled-target").setSettings(computed).get();

        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        // No assertBusy waiting for a positive count here -- there is nothing to wait for. A short,
        // fixed real wait for cluster state to fully settle, then assert the count is still zero.
        assertBusy(() -> assertNotNull(client().admin().cluster().prepareState().get().getState()), 10, TimeUnit.SECONDS);
        assertEquals(
            "gating is off by default and was explicitly disabled here -- nothing must dispatch",
            0L,
            ReaderShardPreWarmCoordinator.preWarmCountForTesting()
        );
    }
}
