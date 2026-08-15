/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Plan item G4 (plan-100m-index-implementation.md, Area G): "Chaos. Node loss during pre-warm, LB and
 * coordinator disagreement about the node set, and a partitioned name index tier. N3 claims graceful
 * degradation and that claim needs evidence." Before this class there was no coverage at all: no test
 * in this plugin exercised a cluster-manager failover happening between two pre-warm-triggering epoch
 * changes.
 *
 * <p>Covers the second of G4's three named disruptions -- coordinator ("cluster-manager" in this
 * architecture's own vocabulary, see {@link ReaderShardPreWarmCoordinator}'s own "why only the elected
 * cluster-manager acts" javadoc) change. The sibling class {@code ServerlessStoragePreWarmChaosIT}
 * covers the first, node loss, for a real gated index. The third disruption, "a partitioned name index
 * tier," and "LB disagreement about the node set" specifically (as opposed to a coordinator failover,
 * which this class does cover) are deliberately left out of both: neither has a concrete mechanism in
 * this codebase to disrupt yet (no simulated LB, and Area A's name index tier has no partition-injection
 * seam this plugin owns), so building either now would be guessing at a scenario rather than testing
 * one, the same discipline E7/D5/H1d's {@code InPlaceMergeTriggerCoordinator} deferral already applied
 * this cycle.
 *
 * <h2>Why this is a separate top-level class, not a second method on the sibling class</h2>
 *
 * The first version of this test was a second {@code public void test...} method on {@code
 * ServerlessStoragePreWarmChaosIT} itself. Both methods passed individually (run one at a time via a
 * {@code --tests "Class.method"} filter) but the gated node-loss test failed, fast and reproducibly,
 * whenever the two ran together as one class -- OpenSearch's IT runner forks one JVM per test
 * <em>class</em> and reuses it across that class's methods ("All tests run in this JVM: [...]" in the
 * test log names the class, not the method), and {@code DescriptorGate} -- the component the node-loss
 * test's real bootstrap path installs -- is a JVM-wide static singleton (see this plugin's Area H notes
 * on why that singleton shape exists). Two test methods that both boot real {@code
 * ServerlessStoragePlugin} nodes in the same JVM, one of which installs real gated-index machinery,
 * is exactly the "components correct in isolation, never proven integrated" trap this plugin has hit
 * more than once this cycle (see {@code TransportMigrateShardAction}'s and {@code
 * ReaderCacheAffinityRecorder}'s own H1d history) -- except here the trap was in the test harness itself,
 * not the production code. Splitting the two disruptions into separate top-level classes gives each its
 * own JVM fork, which is what a shared-singleton production component actually requires from anything
 * that boots it for real more than once in a suite; matching each test method to its own class is simpler
 * and more robust than trying to make {@code DescriptorGate} cooperate across methods it was never
 * designed to be reset between.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageClusterManagerFailoverChaosIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int SHARD_COUNT = 30;

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    /**
     * One shared object store directory for every node this test starts. See {@code
     * ServerlessStoragePreWarmChaosIT#basePath()}'s own javadoc for why this is a lazily-initialized
     * plain instance field rather than a static one: this class, too, is {@code Scope.TEST}.
     */
    private volatile Path basePath;

    private Path basePath() {
        if (basePath == null) {
            synchronized (this) {
                if (basePath == null) {
                    basePath = randomRepoPath();
                }
            }
        }
        return basePath;
    }

    /**
     * Deliberately does NOT set {@link ServerlessStoragePlugin#SERVERLESS_STORAGE_NODE_ENABLED_SETTING}
     * -- this test's index is ordinary (non-gated), and {@code ServerlessStorageReaderShardPreWarmIT}'s
     * own ordinary-index baseline never sets it either. Turning it on unconditionally in an earlier
     * version of this test (when it was still a second method sharing a fixture with the gated
     * node-loss test) is what made this test's baseline round consistently dispatch zero pre-warm
     * polls -- the one setting difference from that reference test's own proven-working shape.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_READER_PRE_WARM_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @Before
    public void resetPreWarmCount() {
        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();
    }

    /**
     * See {@code ServerlessStoragePreWarmChaosIT#drainClusterStateQueue()}'s own javadoc for why this
     * real completion signal replaces the vacuous {@code assertBusy(() -> assertNotNull(count))} idiom
     * {@code ServerlessStorageReaderShardPreWarmIT} uses for the same purpose.
     */
    private void drainClusterStateQueue() {
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();
    }

    /**
     * Coordinator change, the second of G4's three named disruptions. The ordinary (non-gated) pass
     * only ever dispatches from whichever node {@link org.opensearch.cluster.node.DiscoveryNodes#isLocalNodeElectedClusterManager}
     * answers true for at the moment a cluster-state event is applied -- see {@code
     * ReaderShardPreWarmCoordinator}'s own "why only the elected cluster-manager acts" javadoc. That
     * restriction exists to avoid a request storm, not because any state carries over between
     * elections, so nothing in the mechanism should need to be told a new cluster-manager has taken
     * over. This proves that empirically instead of trusting the reasoning.
     *
     * <p>Deliberately staged rather than starting all three cluster-manager-eligible nodes at once: the
     * first version of this test did exactly that and the baseline round -- proven correct by {@code
     * ServerlessStorageReaderShardPreWarmIT}'s own single-manager version of the same assertion --
     * consistently dispatched zero. Three master-eligible nodes bootstrapping and electing amongst
     * themselves from cold adds churn this test does not need answered before its own question even
     * starts. So this proves the known-good single-manager baseline first, exactly as that reference
     * test does, and only adds the second and third cluster-manager-eligible node -- and asks anything
     * about failover -- afterward.
     */
    public void testANewClusterManagerResumesPreWarmDispatchAfterFailover() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        Settings computed = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
            .build();
        client().admin().indices().prepareCreate("serverless_chaos-failover-target").setSettings(computed).get();

        drainClusterStateQueue();
        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();

        // Baseline round, under the original (and, so far, only) cluster-manager -- proves the fixture
        // itself dispatches before asking anything about failover, the same known-good shape
        // ServerlessStorageReaderShardPreWarmIT's own single-manager test already establishes.
        internalCluster().startDataOnlyNode();
        assertBusy(
            () -> assertTrue(
                "the original cluster-manager must dispatch at least one pre-warm poll before this test "
                    + "asks anything about failover -- got "
                    + ReaderShardPreWarmCoordinator.preWarmCountForTesting(),
                ReaderShardPreWarmCoordinator.preWarmCountForTesting() > 0
            ),
            30,
            TimeUnit.SECONDS
        );

        // Only now add failover redundancy -- two more cluster-manager-eligible nodes, so killing the
        // current one still leaves a majority (2 of 3) to elect a real replacement.
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startClusterManagerOnlyNode();
        ensureStableCluster(6);
        drainClusterStateQueue();

        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(5);
        drainClusterStateQueue();

        // The failover round: strictly after a new cluster-manager is confirmed elected and the
        // cluster is stable again, one more growth round must still produce a real dispatch.
        ReaderShardPreWarmCoordinator.resetPreWarmCountForTesting();
        internalCluster().startDataOnlyNode();
        assertBusy(
            () -> assertTrue(
                "the new cluster-manager elected after failover must resume pre-warm dispatch with no "
                    + "special hand-off -- got "
                    + ReaderShardPreWarmCoordinator.preWarmCountForTesting(),
                ReaderShardPreWarmCoordinator.preWarmCountForTesting() > 0
            ),
            30,
            TimeUnit.SECONDS
        );
    }
}
