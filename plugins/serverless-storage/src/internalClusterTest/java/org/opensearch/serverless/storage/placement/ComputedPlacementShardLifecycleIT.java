/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * C18. Whether a computed index has a shard to receive a document.
 *
 * <p>C17 made index creation complete for an index whose routing is not published, and the reachability
 * suite asserts that much. It deliberately claims nothing about the index being usable, and this is the
 * test that asks.
 *
 * <p>The reason to expect it to fail is structural rather than incidental.
 * {@link RoutingNodes#localRoutingNode} builds the local node's shard list by looping the published
 * routing table, and every phase of {@code IndicesClusterStateService} takes its work list from there.
 * An index with no published entry contributes nothing, so no {@code IndexService} and no
 * {@code IndexShard} is created on any node. Coordinators resolve the index through the supplier and
 * route to nodes that were never told to open the shard.
 *
 * <p>Written before the fix rather than after it, because the previous two passes through this area both
 * shipped mechanisms that no test could notice were dead.
 */
public class ComputedPlacementShardLifecycleIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "computed-lifecycle";

    /**
     * Only the membership machinery, not the full serverless plugin, whose gate and resolver would
     * collide with this suite's own fake supplier registrations -- see {@link MembershipOnlyTestPlugin}.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MembershipOnlyTestPlugin.class);
    }

    /**
     * A five second replication retry timeout instead of the sixty second default.
     *
     * <p>Not a convenience. At sixty seconds the write test takes a minute whether it passes or fails and
     * passes about two runs in three, so it is both unusable for iteration and dishonest as a signal: a
     * write that only succeeds by winning a race at the timeout boundary is not a working write. Five
     * seconds makes the failure immediate and the success meaningful, since a correctly routed write to a
     * shard that is already in primary mode needs milliseconds.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5))
            .build();
    }

    @Before
    public void registerComputedPlacementBeforeEachTest() {
        registerComputedPlacement();
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
    }

    /** The claim: a data node opens the shards a computed index says it owns. */
    public void testADataNodeOpensTheComputedShard() throws Exception {
        createComputedIndex();

        assertBusy(() -> {
            int open = 0;
            for (IndicesService indices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                if (indices.hasIndex(resolveIndex(INDEX))) {
                    open++;
                }
            }
            assertTrue("some data node must have opened the computed index", open > 0);
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * The question underneath the write, asked directly.
     *
     * <p>A write that races the replication retry timeout tells you almost nothing: it takes sixty
     * seconds whether it passes or fails, so the signal is buried in a race. Primary mode is what the
     * write actually needs, the shard knows it locally, and asking the shard is both immediate and
     * unambiguous. This is the assertion to fix C18 against.
     */
    public void testTheComputedShardEntersPrimaryMode() throws Exception {
        createComputedIndex();

        awaitPrimaryMode();
    }

    /** Waits for the node holding the computed shard to have it STARTED and in primary mode. */
    private void awaitPrimaryMode() throws Exception {
        assertBusy(() -> {
            IndexShard shard = null;
            for (IndicesService indices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                IndexService indexService = indices.indexService(resolveIndex(INDEX));
                if (indexService != null && indexService.hasShard(0)) {
                    shard = indexService.getShard(0);
                }
            }
            assertNotNull("no data node holds the computed shard", shard);
            assertEquals("the shard must reach STARTED", IndexShardState.STARTED, shard.state());
            assertTrue("the shard must be in primary mode, or every write is rejected and retried", shard.isPrimaryMode());
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * The consequence: once the shard is genuinely ready, a write reaches it and comes back.
     *
     * <p>The wait is the point of the test rather than noise in it. Index creation returns as soon as
     * the required shards are active, and for a computed index the active count comes from the computed
     * table, which reports STARTED from the moment the placement can be derived. That is before any node
     * has opened the shard. So "created" is a weaker promise for a computed index than for an allocated
     * one, and a write issued immediately after creation retries until the shard is really there. It
     * takes over a second and under five, measured.
     *
     * <p>Waiting for primary mode first, then writing with a one second budget, asserts the thing that
     * matters: the write path itself is not slow, and it is not winning a race. A write that needed the
     * retry loop would fail at one second, which is exactly what happens when the wait is removed.
     */
    public void testADocumentCanBeIndexedAndRead() throws Exception {
        createComputedIndex();
        awaitPrimaryMode();

        IndexResponse response = client().prepareIndex(INDEX)
            .setId("1")
            .setSource("field", "value")
            .setTimeout(TimeValue.timeValueSeconds(1))
            .get();

        assertEquals(RestStatus.CREATED, response.status());
        client().admin().indices().prepareRefresh(INDEX).get();
        assertTrue("the document must be readable back from the computed shard", client().prepareGet(INDEX, "1").get().isExists());
    }

    /**
     * C2. The membership must actually be published in a running cluster.
     *
     * <p>An assertion rather than a log probe, because a probe only prints when a test fails and this
     * area has three times shipped a mechanism that was correct and never invoked. Asserting it makes
     * the reachability permanent instead of something checked once by hand.
     */
    public void testPlacementMembershipIsPublished() throws Exception {
        createComputedIndex();
        awaitPrimaryMode();

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            ComputedPlacementMembership membership = ComputedPlacementMembershipService.get(state);

            assertFalse("the membership must be published, or placement is still reading the live view", membership.isEmpty());
            for (String dataNodeId : state.nodes().getDataNodes().keySet()) {
                assertTrue("every live data node must be a member: " + membership, membership.contains(dataNodeId));
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void createComputedIndex() {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).setWaitForActiveShards(ActiveShardCount.ALL).setTimeout(TimeValue.timeValueSeconds(30))
        );

        // The premise of every test in this class, asserted rather than assumed. Without the
        // registrations this creates an ordinary index that happens to be called "computed-", and every
        // test then passes while exercising nothing. That is not hypothetical: an edit removed the
        // per-test registration calls and three tests kept passing green for exactly that reason, which
        // is the "unit tests construct the state they assert against" trap arriving at integration level.
        assertFalse(
            "the index under test must have no published routing entry, or this suite is testing an "
                + "ordinary index and proving nothing",
            client().admin().cluster().prepareState().get().getState().routingTable().hasIndex(INDEX)
        );
    }

    private static void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));
        AbsentIndexRoutingSuppliers.register(ComputedPlacementShardLifecycleIT::compute);
    }

    private static IndexRoutingTable compute(ClusterState state, IndexMetadata indexMetadata) {
        List<String> dataNodes = sortedDataNodes(state);
        if (dataNodes.isEmpty()) {
            return null;
        }
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            builder.addIndexShard(new IndexShardRoutingTable.Builder(shard).addShard(started(shard, owner(dataNodes, shardId))).build());
        }
        return builder.build();
    }

    private static String owner(List<String> dataNodes, int shardId) {
        return dataNodes.get(shardId % dataNodes.size());
    }

    private static List<String> sortedDataNodes(ClusterState state) {
        List<String> dataNodes = new ArrayList<>(state.nodes().getDataNodes().keySet());
        Collections.sort(dataNodes);
        return dataNodes;
    }

    /**
     * The recovery source is stated rather than derived. The A5 trap is that a source taken from absent
     * inSyncAllocationIds silently becomes EmptyStoreRecoverySource and a live index recovers blank.
     * Empty is correct here and only here: this index is being created, so there is nothing to recover.
     * A restart is C13, and it has to make the opposite choice.
     */
    private static ShardRouting started(ShardId shard, String nodeId) {
        return ComputedShardRouting.started(shard, nodeId, RecoverySource.EmptyStoreRecoverySource.INSTANCE);
    }
}
