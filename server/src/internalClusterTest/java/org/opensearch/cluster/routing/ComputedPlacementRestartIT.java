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
 * <p>What remains is larger. After a full restart with a computed index present, the cluster does not
 * recover at all: every node reports {@code state not recovered / initialized} and no cluster manager is
 * discovered. That is a gateway-level question rather than a shard-level one, and it is the next task.
 * The likely suspect is state recovery expecting each index in metadata to have routing.
 */
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "C13: after a full restart with a computed index, the cluster does not recover and no "
    + "cluster manager is discovered. See this class's javadoc and plan-area-c-computed-placement.md.")
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
        createComputedIndex();
        awaitPrimaryMode();

        for (int i = 0; i < 20; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("field", "value-" + i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        assertEquals(20L, client().prepareSearch(INDEX).setSize(0).get().getHits().getTotalHits().value());

        internalCluster().fullRestart();
        ensureStableCluster(internalCluster().size());
        awaitPrimaryMode();

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

    private static List<String> sortedDataNodes(ClusterState state) {
        List<String> dataNodes = new ArrayList<>(state.nodes().getDataNodes().keySet());
        Collections.sort(dataNodes);
        return dataNodes;
    }
}
