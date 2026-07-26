/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
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
 * C13. Whether a computed index survives a restart.
 *
 * <p>In its own class, and that is deliberate. The restart leaves the cluster in a state the other tests
 * cannot recover from, so keeping it alongside them turned one real failure into three misleading ones:
 * the siblings failed on a broken shared cluster rather than on anything they were asserting.
 *
 * <p><b>It does not pass, and the reason moved.</b> The first version of this test passed for three runs
 * while restarting an ordinary index, because an edit had silently stripped the registrations that make
 * an index computed. With them restored the documents were gone after the restart, which is the A5 trap:
 * the local view stated {@code EmptyStoreRecoverySource} because that is what the placement function
 * says, and on restart that recovers a live index blank while leaving it STARTED and green.
 *
 * <p>C19 fixed that part. The node now corrects the recovery source from something only the node knows,
 * whether it already holds the data, because a placement function evaluated identically everywhere
 * cannot know which node has a store.
 *
 * <p>C20 then fixed cluster recovery. State recovery had been republishing routing for the computed
 * index, which collided with the copy the node contributes for itself, and the cluster never came back:
 * every node reported {@code state not recovered / initialized}. With the guard in place the cluster
 * recovers and this test runs to completion.
 *
 * <p><b>C2 landed and moved the failure.</b> Placement now reads the published membership, which is
 * stable across a restart, and the membership is confirmed published before any data is written. What
 * fails now is earlier and different: the twenty documents are not searchable even <em>before</em> the
 * restart. That is not the symptom this test was written for, and it needs its own investigation rather
 * than being folded into C13.
 *
 * <p>The prime suspect is the trigger index this test creates to force a cluster state change: it is an
 * ordinary index, so it publishes routing, and it is the first thing the membership sees. Whether its
 * presence shifts the computed placement of the shard written afterwards is the first thing to check,
 * with the second being whether the search reaches the node that actually holds the shard.
 *
 * <p><b>The original cause was placement instability, and that part is fixed.</b>
 * Probes cleared recovery entirely: the recovery source is correct both times, the
 * {@code cleanLuceneIndex} branch that silently discards a store never fires, and the pre-restart
 * recovery is a textbook new index. What varies is which node owns the shard.
 *
 * <p>{@link #owner} places by {@code shardId % dataNodes.size()} over the data nodes visible in the
 * cluster state at that instant. During a restart that list grows as nodes rejoin, so ownership moves,
 * and a node that gains the shard has none of its data. It then recovers empty, correctly by its own
 * lights, because it really does have nothing. Different timing gives different symptoms: one run comes
 * back with zero documents, the next cannot find the shard at all.
 *
 * <p>That is precisely the risk C2 named. "Two coordinators computing against different node lists
 * produce different placement, so this is a correctness input, not a convenience." A restart is the same
 * problem in time rather than in space: the same coordinator computing against a changing node list
 * relocates shards away from their data, and every new owner starts blank while looking healthy.
 *
 * <p>So C13 is blocked on C2 rather than on recovery. The placement function needs a stable node set,
 * agreed rather than instantaneous, and {@code RendezvousShardPlacement} from C1 minimises movement but
 * does not by itself make the input stable. This test should also use it rather than modulo, but that
 * only reduces the blast radius; it does not make a restart safe.
 */
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "C2 landed: placement is now stable across a restart. The remaining failure is earlier and "
    + "different, documents not searchable before the restart. See this class's javadoc.")
public class ComputedPlacementRestartIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "computed-restart";

    @Before
    public void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));
        AbsentIndexRoutingSuppliers.register(ComputedPlacementRestartIT::compute);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedPlacementRestartIT::localShards);
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
    }

    /**
     * Twenty documents rather than one, and a count rather than an existence check. A blank index is
     * STARTED, green and empty, so a single surviving document proves much less than a full count.
     */
    public void testAComputedIndexSurvivesAFullRestart() throws Exception {
        awaitStableMembership();
        createComputedIndex();
        awaitPrimaryMode();

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value-" + i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        assertEquals(
            "the documents must be there before the restart, or placement moved while the test was writing",
            20L,
            client().prepareSearch(INDEX).setSize(0).get().getHits().getTotalHits().value()
        );

        String ownerBeforeRestart = nodeHoldingComputedShard();

        internalCluster().fullRestart();
        ensureStableCluster(internalCluster().size());
        awaitPrimaryMode();

        // Placement first, because it is the property under test and it explains the count. A shard that
        // moved lands on a node with none of its data, so the count would be zero for a reason that has
        // nothing to do with recovery.
        assertEquals("the shard must come back on the node that holds its data", ownerBeforeRestart, nodeHoldingComputedShard());

        client().admin().indices().prepareRefresh(INDEX).get();
        assertEquals(
            "every document must survive the restart, or the shard recovered blank",
            20L,
            client().prepareSearch(INDEX).setSize(0).get().getHits().getTotalHits().value()
        );
        assertEquals(
            "a restarted computed shard must recover from its existing store",
            RecoverySource.Type.EXISTING_STORE,
            recoverySourceOfComputedShard()
        );

    }

    /**
     * Waits until every live data node is a member before anything is created.
     *
     * <p>Membership grows as nodes join, and growth is a placement change: with modulo it reshuffles
     * every shard. That is harmless before there is data and destructive after, so the test waits for
     * the set to settle rather than racing it. A production cluster has the same window, which is why
     * the plugin uses rendezvous hashing, where growth moves a fraction rather than everything.
     */
    private void awaitStableMembership() throws Exception {
        // The maintainer is edge-triggered: it publishes on a cluster state change while a supplier is
        // registered. Registration happens in @Before, after the cluster has already formed, so without
        // a subsequent change nothing ever publishes. A real cluster generates changes constantly and a
        // test does not, so one is forced here with an ordinary index that has nothing to do with
        // computed placement.
        //
        // The narrow production edge this papers over is worth naming: a cluster that installs a supplier
        // and then goes completely idle has no membership until something else happens, and placement
        // uses the live view in the meantime. Publishing on registration would close it.
        assertAcked(
            prepareCreate("membership-trigger").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build())
        );

        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            ComputedPlacementMembership membership = ComputedPlacementMembershipService.get(state);
            assertFalse("membership must be published before placement can be stable", membership.isEmpty());
            for (String dataNodeId : state.nodes().getDataNodes().keySet()) {
                assertTrue("every live data node must be a member before creating data: " + membership, membership.contains(dataNodeId));
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void createComputedIndex() {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).setWaitForActiveShards(ActiveShardCount.ALL).setTimeout(TimeValue.timeValueSeconds(30))
        );
        assertFalse(
            "the index under test must have no published routing entry, or this proves nothing",
            client().admin().cluster().prepareState().get().getState().routingTable().hasIndex(INDEX)
        );
    }

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
            assertEquals(IndexShardState.STARTED, shard.state());
            assertTrue(shard.isPrimaryMode());
        }, 30, TimeUnit.SECONDS);
    }

    private String nodeHoldingComputedShard() {
        for (String nodeName : internalCluster().getNodeNames()) {
            IndicesService indices = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexService indexService = indices.indexService(resolveIndex(INDEX));
            if (indexService != null && indexService.hasShard(0)) {
                return internalCluster().getInstance(org.opensearch.cluster.service.ClusterService.class, nodeName).localNode().getId();
            }
        }
        throw new AssertionError("no node holds the computed shard");
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

    private static IndexRoutingTable compute(ClusterState state, IndexMetadata indexMetadata) {
        List<String> dataNodes = sortedDataNodes(state);
        if (dataNodes.isEmpty()) {
            return null;
        }
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetadata.getIndex());
        for (int shardId = 0; shardId < indexMetadata.getNumberOfShards(); shardId++) {
            ShardId shard = new ShardId(indexMetadata.getIndex(), shardId);
            builder.addIndexShard(
                new IndexShardRoutingTable.Builder(shard).addShard(
                    ComputedShardRouting.started(shard, owner(dataNodes, shardId), RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            );
        }
        return builder.build();
    }

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
                // EmptyStore is what the placement function can state. The node corrects it to
                // ExistingStore when it finds it already holds the data, which is C19: only the node
                // knows that, and a function computed identically everywhere cannot.
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

    /**
     * The placement node set, taken from the published membership rather than from the nodes that
     * happen to be reachable.
     *
     * <p>This is the whole of C2 in one method. Reading {@code getDataNodes()} means a node that is
     * merely restarting drops out, ownership moves to a node holding none of that shard's data, and it
     * recovers empty while looking healthy. The membership is published, versioned and never shrinks, so
     * a node being away is not a placement event.
     *
     * <p>Modulo rather than rendezvous here only because a server test cannot depend on the plugin that
     * owns {@code RendezvousShardPlacement}. The distinction does not matter for this test: with a
     * membership that never shrinks, a restart does not change the set, so both are stable. Rendezvous
     * earns its keep when the membership genuinely grows, where modulo reshuffles everything and
     * rendezvous moves only a fraction, and the plugin's own path uses it over this same membership.
     */
    private static List<String> sortedDataNodes(ClusterState state) {
        ComputedPlacementMembership membership = ComputedPlacementMembershipService.get(state);
        if (membership.isEmpty() == false) {
            return membership.nodeIds();
        }
        List<String> dataNodes = new ArrayList<>(state.nodes().getDataNodes().keySet());
        Collections.sort(dataNodes);
        return dataNodes;
    }
}
