/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.TransportReplicationAction;
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
 * C23. Whether the operations that sweep every shard can see a computed one.
 *
 * <p>C21 and C22 each found this seam through a single caller. This suite asks the question the other
 * way round, because the shape is a family rather than a list of call sites:
 * {@code RoutingTable.allShards} and its neighbours iterate published entries only, so an index that
 * publishes none contributes nothing and every caller silently sees a smaller cluster than exists.
 *
 * <p><b>These fail by succeeding.</b> Not one of them throws. Stats reports an index with no shards,
 * segments reports no segments, recovery reports nothing recovering, and a force merge reports success
 * having merged nothing. That is the same signature as the refresh in C21 and the field mappings in C22,
 * and it is why every assertion here is a count rather than a check that no exception escaped. An
 * operator reading these responses would conclude the index is empty, and an automated system consuming
 * them would act on it.
 *
 * <p>Written before the fix, so what it reports is a finding rather than a confirmation.
 */
public class ComputedPlacementBroadcastIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "computed-broadcast";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), TimeValue.timeValueSeconds(5))
            .build();
    }

    @Before
    public void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));
        AbsentIndexRoutingSuppliers.register(ComputedPlacementBroadcastIT::compute);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedPlacementBroadcastIT::localShards);
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
    }

    /**
     * Stats is the one an operator is most likely to look at, and the most misleading when wrong: a
     * document count of zero for an index holding documents reads as an empty index rather than as an
     * unreachable one.
     */
    public void testIndicesStatsSeesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        indexOneDocument();

        IndicesStatsResponse response = client().admin().indices().prepareStats(INDEX).get();

        assertEquals("indices stats must reach the computed shard", 1, response.getSuccessfulShards());
        assertEquals("indices stats must count the document the shard holds", 1L, response.getPrimaries().getDocs().getCount());
    }

    public void testIndicesSegmentsSeesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        indexOneDocument();

        IndicesSegmentResponse response = client().admin().indices().prepareSegments(INDEX).get();

        assertEquals("indices segments must reach the computed shard", 1, response.getSuccessfulShards());
    }

    /**
     * Force merge is the dangerous one. The others report a wrong answer; this one accepts an
     * instruction, reports success, and does nothing, so a caller believes work happened that never did.
     */
    public void testForceMergeReachesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        indexOneDocument();

        ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        assertEquals(
            "force merge must reach the computed shard rather than succeed having merged nothing",
            1,
            response.getSuccessfulShards()
        );
    }

    public void testRecoveryReportsTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();

        RecoveryResponse response = client().admin().indices().prepareRecoveries(INDEX).get();

        assertEquals(
            "recovery must report the computed shard, since a shard nothing reports cannot be diagnosed",
            1,
            response.shardRecoveryStates().getOrDefault(INDEX, Collections.emptyList()).size()
        );
    }

    // ---------------------------------------------------------------- helpers

    private void indexOneDocument() {
        client().prepareIndex(INDEX).setId("1").setSource("field", "value").setTimeout(TimeValue.timeValueSeconds(10)).get();
        client().admin().indices().prepareRefresh(INDEX).get();
    }

    private void createComputedIndexAndWaitForPrimaryMode() throws Exception {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).setWaitForActiveShards(ActiveShardCount.ALL).setTimeout(TimeValue.timeValueSeconds(30))
        );

        assertFalse(
            "the index under test must have no published routing entry, or this suite proves nothing",
            client().admin().cluster().prepareState().get().getState().routingTable().hasIndex(INDEX)
        );

        assertBusy(() -> {
            IndexShard shard = null;
            for (IndicesService indices : internalCluster().getDataNodeInstances(IndicesService.class)) {
                IndexService indexService = indices.indexService(resolveIndex(INDEX));
                if (indexService != null && indexService.hasShard(0)) {
                    shard = indexService.getShard(0);
                }
            }
            assertNotNull("no data node holds the computed shard", shard);
            assertEquals("the computed shard must reach STARTED", IndexShardState.STARTED, shard.state());
            assertTrue("the computed shard must be in primary mode before any broadcast is tried", shard.isPrimaryMode());
        }, 30, TimeUnit.SECONDS);
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
        ComputedPlacementMembership membership = ComputedPlacementMembershipService.get(state);
        if (membership.isEmpty() == false) {
            return membership.nodeIds();
        }
        List<String> dataNodes = new ArrayList<>(state.nodes().getDataNodes().keySet());
        Collections.sort(dataNodes);
        return dataNodes;
    }
}
