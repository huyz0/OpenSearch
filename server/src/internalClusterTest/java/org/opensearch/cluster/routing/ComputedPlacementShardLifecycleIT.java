/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
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
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
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
     * C13. Whether a computed index survives a restart of the node holding it.
     *
     * <p>This is the A5 trap's home ground. A recovery source derived from absent
     * {@code inSyncAllocationIds} silently becomes {@code EmptyStoreRecoverySource}, and a live index
     * comes back blank rather than failing loudly. Computed placement recreates the exact condition,
     * because a computed index has no published in-sync ids by construction.
     *
     * <p>Recovery for a computed index has been reasoned about and never observed, and A5 is what
     * reasoning about recovery cost last time. So the assertion is the document, not the shard state: a
     * blank index is STARTED, green, and empty, and every structural check passes while the data is
     * gone.
     *
     * <p><b>It fails, and the A5 trap is the reason.</b> The documents do not survive. The local view
     * hands back {@code EmptyStoreRecoverySource} because that is what the placement function states,
     * and on restart that is exactly the wrong answer: the shard recovers from an empty store and the
     * index comes back blank while looking perfectly healthy.
     *
     * <p>The fix is a design question rather than a patch, which is why this is disabled rather than
     * hacked. The recovery source cannot come from the placement function, because placement is computed
     * identically on every node and only the node holding the data knows whether it has any. So either
     * the node overrides the source at shard creation by looking at its own disk, or the hook is given
     * enough context to decide per node. Deciding that is the next task.
     *
     * <p>This test passed for three runs before the harness bug below was found, because without the
     * registrations it was restarting an ordinary index. That is the second time in this area a green
     * test was measuring nothing.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "C13: a computed index recovers blank after a restart. The A5 trap, live. See this "
        + "method's javadoc and plan-area-c-computed-placement.md.")
    public void testAComputedIndexSurvivesANodeRestart() throws Exception {
        createComputedIndex();
        awaitPrimaryMode();

        client().prepareIndex(INDEX).setId("1").setSource("field", "value").setTimeout(TimeValue.timeValueSeconds(1)).get();
        client().admin().indices().prepareRefresh(INDEX).get();
        assertTrue(client().prepareGet(INDEX, "1").get().isExists());

        RecoverySource.Type beforeRestart = recoverySourceOfComputedShard();
        assertEquals("PROBE before-restart source", RecoverySource.Type.EMPTY_STORE, beforeRestart);

        internalCluster().fullRestart();
        ensureStableCluster(internalCluster().size());
        awaitPrimaryMode();

        client().admin().indices().prepareRefresh(INDEX).get();
        assertTrue(
            "the document must survive the restart, or the shard recovered from an empty store and the " + "index came back blank",
            client().prepareGet(INDEX, "1").get().isExists()
        );

        // Assert what it recovered from, not only that the data is there. A5's lesson is that an empty
        // store recovery is silent: the index is STARTED, green and blank, so a data assertion alone can
        // pass for the wrong reason on a different day. Naming the source makes the guarantee explicit.
        RecoverySource.Type afterRestart = recoverySourceOfComputedShard();
        logger.info("DIAGC13 recovery source after restart={}", afterRestart);
        assertEquals("a restarted computed shard must recover from its existing store", RecoverySource.Type.EXISTING_STORE, afterRestart);
    }

    private RecoverySource.Type recoverySourceOfComputedShard() {
        for (IndicesService indices : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexService indexService = indices.indexService(resolveIndex(INDEX));
            if (indexService != null && indexService.hasShard(0)) {
                return indexService.getShard(0).recoveryState().getRecoverySource().getType();
            }
        }
        throw new AssertionError("no data node holds the computed shard after restart");
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
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedPlacementShardLifecycleIT::localShards);
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

    /**
     * The inverse lookup. Enumerating every index in metadata is exactly the cost this area exists to
     * avoid, and a real implementation would not do it; a test cluster with a handful of indices can,
     * and what is under test here is the seam rather than how a plugin answers through it.
     */
    private static List<ShardRouting> localShards(ClusterState state, String nodeId) {
        List<String> dataNodes = sortedDataNodes(state);
        List<ShardRouting> mine = new ArrayList<>();
        if (dataNodes.isEmpty()) {
            return mine;
        }
        for (IndexMetadata indexMetadata : state.metadata()) {
            if (AbsentIndexRoutingSuppliers.shouldPublishRouting(indexMetadata)) {
                continue;
            }
            for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
                if (nodeId.equals(owner(dataNodes, shardId)) == false) {
                    continue;
                }
                // INITIALIZING, not STARTED. failMissingShards fails any shard the local view calls
                // active that the node does not already have, and createIndices then skips it because it
                // is in failedShardsCache, so an already-started local view can never open a shard. That
                // was measured, not reasoned: the first run of this test failed with no node-side error
                // at all.
                mine.add(
                    ComputedShardRouting.initializing(
                        new ShardId(indexMetadata.getIndex(), shardId),
                        nodeId,
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE
                    )
                );
            }
        }
        return mine;
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
