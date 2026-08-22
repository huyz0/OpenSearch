/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * C13. Whether a computed index survives a restart.
 *
 * <p>In its own class, and that is deliberate. The restart leaves the cluster in a state the other tests
 * cannot recover from, so keeping it alongside them turned one real failure into three misleading ones:
 * the siblings failed on a broken shared cluster rather than on anything they were asserting.
 *
 * <p>It took four separate fixes to pass, and the sequence is worth keeping because each one moved the
 * failure somewhere new rather than removing it.
 *
 * <p>C19 came first. The local view stated {@code EmptyStoreRecoverySource}, because that is all a
 * placement function evaluated identically on every node can state, and on restart that recovers a live
 * index blank while leaving it STARTED and green. The node now corrects the recovery source from the one
 * thing only it knows, whether it already holds the data.
 *
 * <p>C20 fixed cluster recovery. State recovery republished routing for the computed index, colliding
 * with the copy the node contributes for itself, and the cluster never came back at all.
 *
 * <p>C2 fixed placement stability. Ownership had been computed against the data nodes visible at that
 * instant, so a restart moved shards away from their data and every new owner started blank while
 * looking healthy. Membership is now published, versioned and never shrinks, so a node being away is not
 * a placement event.
 *
 * <p>C21 was the last and the least visible. Refresh resolved its shards with a direct routing lookup,
 * found nothing for a computed index, and reported success having touched no shards at all. Since
 * {@code Engine.docStats()} reads the internal searcher, which only advances on refresh, the twenty
 * documents sat in the engine while both the shard's own count and the search read zero. Every earlier
 * theory about this test, including blank recovery and lost writes, was downstream of that.
 *
 * <p>The pattern across all four is one thing: a read of the routing table that should have been a
 * resolve. C21 is the one that survived six passes over that seam, because unlike the others it failed
 * upward, returning success rather than an error.
 */
public class ComputedPlacementRestartIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "computed-restart";

    /**
     * Only the membership machinery, not the full serverless plugin. This suite registers its own fake
     * placement suppliers, and the real plugin's gate and resolver would collide with them; see
     * {@link MembershipOnlyTestPlugin}'s own javadoc.
     */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MembershipOnlyTestPlugin.class);
    }

    @Before
    public void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));
        AbsentIndexRoutingSuppliers.register(ComputedPlacementRestartIT::compute);
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
    }

    /**
     * Twenty documents rather than one, and a count rather than an existence check. A blank index is
     * STARTED, green and empty, so a single surviving document proves much less than a full count.
     */
    public void testAComputedIndexSurvivesAFullRestart() throws Exception {
        awaitStableMembership();
        createComputedIndex();
        awaitPrimaryMode();

        // Before writing anything, check that the node the coordinator will route writes to is the node
        // that actually built the shard. If these differ every write goes to a shard nobody reads, and
        // the resulting empty count would look exactly like a recovery failure.
        assertEquals(
            "placement must name the node that actually materialized the shard",
            nodeComputedPlacementAssignsTheShardTo(),
            nodeHoldingComputedShard()
        );

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value-" + i).get();
        }
        // The refresh is asserted rather than fired and forgotten. A broadcast that reaches no shards
        // reports success having touched nothing, so an index that was never refreshed is
        // indistinguishable from a working one at the call site, and every count after it reads stale.
        assertEquals(
            "the refresh must reach the computed shard, or every count after it reads stale",
            1,
            client().admin().indices().prepareRefresh(INDEX).get().getSuccessfulShards()
        );

        // Ask the shard before asking the search. These two disagreeing is the difference between
        // "the documents never landed" and "they landed somewhere the search does not look", and a
        // search count alone cannot tell them apart.
        Map<String, Long> perNode = documentCountPerNodeHoldingTheShard();
        assertEquals(
            "exactly one node must hold the shard and it must hold every document written to it, but the "
                + "counts per node were "
                + perNode,
            "[20]",
            new ArrayList<>(perNode.values()).toString()
        );
        assertEquals(
            "the search must find the documents the shard holds, before any restart",
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

        // Retried rather than asked once, and the reason is the design rather than test flakiness. A
        // membership that never shrinks means a node that is away is still assigned its shards, so
        // requests to it fail and retry until it is back. That is the deliberate trade in C2: unavailable
        // for a moment beats relocated onto a node with none of the data. Immediately after a full
        // restart the cluster is still settling, and one run in three saw the query phase fail outright
        // with every shard unavailable.
        //
        // This does not weaken the assertion. A shard that recovered blank stays blank, so retrying can
        // only wait out an unavailable window, never turn a real zero into twenty.
        assertBusy(() -> {
            // The conversion is the load-bearing part. assertBusy retries on AssertionError and lets any
            // other exception through, and an unavailable shard surfaces as SearchPhaseExecutionException
            // rather than as a failed assertion. Wrapping the block without this changes nothing, which is
            // how the first attempt at this retry still failed one run in three.
            try {
                assertEquals(
                    "the refresh must reach the computed shard after the restart too",
                    1,
                    client().admin().indices().prepareRefresh(INDEX).get().getSuccessfulShards()
                );
                assertEquals(
                    "every document must survive the restart, or the shard recovered blank",
                    20L,
                    client().prepareSearch(INDEX).setSize(0).get().getHits().getTotalHits().value()
                );
            } catch (OpenSearchException e) {
                throw new AssertionError("the restarted cluster has not finished settling yet", e);
            }
        }, 30, TimeUnit.SECONDS);
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
        // Nothing is created here to force a cluster state change, and that absence is the assertion. The
        // maintainer used to publish only on a state change, so registering a supplier into an idle
        // cluster published nothing and placement quietly used the live node list until something
        // unrelated happened. This test papered over that by creating a throwaway index; the maintainer
        // now publishes on registration instead, so the workaround is gone and its removal is what proves
        // the fix.
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

    /**
     * What every node that materialized the shard reports for its own document count.
     *
     * <p>Every node rather than the first one found, because the two failures this has to tell apart look
     * identical from a single count. If one node reports zero the writes never landed; if two nodes hold
     * the shard and only one has the documents, placement disagreed with itself and the search is reading
     * a copy that was never written to. An empty map means nothing materialized at all.
     */
    private Map<String, Long> documentCountPerNodeHoldingTheShard() {
        Map<String, Long> perNode = new TreeMap<>();
        for (String nodeName : internalCluster().getNodeNames()) {
            IndicesService indices = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexService indexService = indices.indexService(resolveIndex(INDEX));
            if (indexService != null && indexService.hasShard(0)) {
                perNode.put(nodeName, indexService.getShard(0).docStats().getCount());
            }
        }
        return perNode;
    }

    /** The node the published placement names as holding shard 0, which is where requests will go. */
    private String nodeComputedPlacementAssignsTheShardTo() {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexRoutingTable routing = state.getIndexRoutingTable(INDEX);
        assertNotNull("placement must resolve an entry for a computed index", routing);
        return routing.shard(0).primaryShard().currentNodeId();
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
     * <p>Modulo rather than rendezvous here even though this suite now lives beside
     * {@code RendezvousShardPlacement}: the suite deliberately runs with {@link MembershipOnlyTestPlugin}
     * rather than the full plugin, keeping the environment it had when it lived in core. The distinction
     * does not matter for this test: with a membership that never shrinks, a restart does not change the
     * set, so both are stable. Rendezvous earns its keep when the membership genuinely grows, where
     * modulo reshuffles everything and rendezvous moves only a fraction, and the plugin's own path uses
     * it over this same membership.
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
