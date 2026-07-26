/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.action.update.UpdateResponse;
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
 * C22. Which single-index request paths a computed index can still not reach.
 *
 * <p>C21 established the shape of the remaining work. A request path that reads the routing table
 * directly finds nothing for an index that publishes no entry, and each one degrades in the way Phase A
 * chose: an empty iterator, a null guard, no shards. That was right when absent meant no shards and is
 * wrong now that absent means computed.
 *
 * <p><b>Written to find out rather than to confirm.</b> Three call sites look unconverted by reading, and
 * reading is exactly what has been wrong every time in this area. The single-document write path looked
 * unconverted too and turns out to resolve already, which is why writes work at all. So this suite runs
 * before any production change and its first job is to say which seams a computed index actually
 * reaches. A seam nothing can reach is not a bug to fix, it is a note to write down, as C6 and C10 were.
 *
 * <p><b>Every assertion is a number, never the absence of an exception.</b> That is the whole lesson of
 * C21: a refresh that reached no shards returned success with {@code successful_shards} zero and hid for
 * two sessions behind a call site that never looked. Each test here names the count that is zero when
 * the seam is broken.
 *
 * <p>Every request is also bounded by a timeout, because the failure mode on the update path is a hang
 * rather than an error. An empty shard iterator makes the caller wait for an allocation that will never
 * be published, so an unbounded request would stall the suite instead of failing it.
 */
public class ComputedPlacementRequestPathsIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "computed-request-paths";

    /** As in the lifecycle suite: five seconds turns a hang into a prompt failure. */
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
        AbsentIndexRoutingSuppliers.register(ComputedPlacementRequestPathsIT::compute);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedPlacementRequestPathsIT::localShards);
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
    }

    /**
     * The plain update, which routes by document id.
     *
     * <p>Expected to pass already. {@code OperationRouting#shards} carries a supplier fallback for the
     * single-document path, so this is the control that tells the two update branches apart: if this
     * failed as well, the problem would be somewhere shared rather than in the branch under test.
     */
    public void testUpdateByIdReachesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        indexOneDocument();

        UpdateResponse response = client().prepareUpdate(INDEX, "1")
            .setDoc("field", "updated")
            .setTimeout(TimeValue.timeValueSeconds(10))
            .get();

        assertEquals("an update routed by document id must reach the computed shard", 2L, response.getVersion());
    }

    /**
     * The bulk update, which routes by an already-resolved shard id and takes the other branch.
     *
     * <p>{@code TransportUpdateAction#shards} looks the routing entry up directly when the request
     * carries a shard id, and returns an empty iterator when it finds nothing. The caller reads that as
     * "not allocated yet" and waits, so the symptom is a hang rather than an error, which is harder to
     * attribute than a failure. The bulk failure message is asserted rather than just the failure count,
     * because a bulk item can fail for reasons that have nothing to do with routing.
     */
    public void testBulkUpdateReachesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        indexOneDocument();

        BulkResponse response = client().prepareBulk()
            .add(client().prepareUpdate(INDEX, "1").setDoc("field", "bulk-updated"))
            .setTimeout(TimeValue.timeValueSeconds(10))
            .get();

        assertFalse(
            "a bulk update must reach the computed shard, but failed with: " + response.buildFailureMessage(),
            response.hasFailures()
        );
    }

    /**
     * Analyze against a concrete index, which needs a shard to run on.
     *
     * <p>Counted rather than merely called: the seam degrades to an empty shards iterator, and the
     * resulting NoShardAvailableActionException is the failure this asserts against.
     */
    public void testAnalyzeReachesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();

        AnalyzeAction.Response response = client().admin().indices().prepareAnalyze(INDEX, "one two three").get();

        assertEquals("analyze must run on the computed shard and return its tokens", 3, response.getTokens().size());
    }

    /** Field mappings, which balance across shards and so need at least one to exist. */
    public void testGetFieldMappingsReachesTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        indexOneDocument();

        GetFieldMappingsResponse response = client().admin().indices().prepareGetFieldMappings(INDEX).setFields("field").get();

        assertEquals(
            "get field mappings must reach the computed shard and report the mapped field",
            1,
            response.mappings().getOrDefault(INDEX, Collections.emptyMap()).size()
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

        // The premise, asserted rather than assumed. An edit once stripped the registrations and three
        // tests kept passing against an ordinary index, so this is the guard that stops a green suite
        // from measuring nothing.
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
            assertTrue("the computed shard must be in primary mode before any request path is tried", shard.isPrimaryMode());
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
