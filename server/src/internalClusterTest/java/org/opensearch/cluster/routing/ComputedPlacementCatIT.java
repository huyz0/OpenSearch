/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.action.admin.cluster.shards.CatShardsAction;
import org.opensearch.action.admin.cluster.shards.CatShardsRequest;
import org.opensearch.action.admin.cluster.shards.CatShardsResponse;
import org.opensearch.action.pagination.PageParams;
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
 * C26. Whether an operator can see a computed shard where they would look for it.
 *
 * <p>This is the gap C23 could not close. Its helper resolves a named list of indices, and the callers
 * here name none: {@code _cat/shards} and {@code _cat/allocation} both call the no-argument
 * {@code routingTable().allShards()}, which takes its index list from the routing table's own key set. A
 * computed index is not a key there, so it cannot be resolved and cannot even be named. There is no index
 * list to pass, which is why C26 is a design question rather than another conversion.
 *
 * <p>It matters more than its size suggests. Stats being wrong is a wrong answer to a question an
 * operator asked deliberately; cat being wrong means the shard is missing from the two places someone
 * looks when they are trying to find out where their data is, which is exactly the moment they are least
 * able to tell "not listed" apart from "not there".
 *
 * <p>The assertion is a row count rather than an absence of failure, because every instance of this seam
 * so far has returned success while describing an empty world.
 */
public class ComputedPlacementCatIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "computed-cat";

    @Before
    public void registerComputedPlacement() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));
        AbsentIndexRoutingSuppliers.register(ComputedPlacementCatIT::compute);
        AbsentIndexRoutingSuppliers.registerLocalShards(ComputedPlacementCatIT::localShards);
    }

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
    }

    /** The question an operator is really asking: is my shard listed, and on which node. */
    public void testCatShardsListsTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();
        String owner = nodeHoldingComputedShard();

        CatShardsRequest request = new CatShardsRequest();
        request.setIndices(new String[] { INDEX });
        request.setCancelAfterTimeInterval(TimeValue.timeValueSeconds(30));
        CatShardsResponse response = client().execute(CatShardsAction.INSTANCE, request).actionGet();

        List<ShardRouting> listed = new ArrayList<>();
        for (ShardRouting shardRouting : response.getResponseShards()) {
            if (INDEX.equals(shardRouting.getIndexName())) {
                listed.add(shardRouting);
            }
        }

        assertEquals("cat shards must list the computed shard, or an operator cannot find their data", 1, listed.size());
        assertEquals("cat shards must name the node that actually holds the computed shard", owner, listed.get(0).currentNodeId());
    }

    /**
     * The paginated path, which is a different failure and a worse one.
     *
     * <p>Pagination does not use the no-argument accessor. It enumerates <em>metadata</em>, which is the
     * one place a computed index does appear, and then looks each index up in the routing map with an
     * unguarded {@code get}. So where the unpaginated path omits the shard, this one is expected to throw
     * on it. Asserted rather than assumed, because reading a call site has been wrong repeatedly here.
     */
    public void testPaginatedCatShardsHandlesTheComputedIndex() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();

        CatShardsRequest request = new CatShardsRequest();
        request.setIndices(new String[] { INDEX });
        request.setCancelAfterTimeInterval(TimeValue.timeValueSeconds(30));
        request.setPageParams(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 100));

        CatShardsResponse response = client().execute(CatShardsAction.INSTANCE, request).actionGet();

        int listed = 0;
        for (ShardRouting shardRouting : response.getResponseShards()) {
            if (INDEX.equals(shardRouting.getIndexName())) {
                listed++;
            }
        }
        assertEquals("paginated cat shards must list the computed shard too", 1, listed);
    }

    /**
     * What cat allocation counts, asserted through the resolver it now uses.
     *
     * <p>The first version of this test asserted on {@code routingTable().allShards()} directly and so
     * could never pass: that accessor is the thing which structurally cannot see a computed index, which
     * is the entire premise of C26. Asserting on it was measuring the bug rather than the fix.
     *
     * <p>It also has to ask for metadata explicitly, for the same reason cat now does. A cluster state
     * response without metadata cannot name a computed index at all, and that is quiet rather than loud:
     * the resolver simply finds nothing to resolve and returns a shorter list.
     */
    public void testAllocationCountsTheComputedShard() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();

        ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).setMetadata(true).get().getState();

        int computedShards = 0;
        for (ShardRouting shardRouting : AbsentIndexRoutingSuppliers.allShards(state)) {
            if (INDEX.equals(shardRouting.getIndexName())) {
                computedShards++;
            }
        }

        assertEquals("what cat allocation counts must include the computed shard", 1, computedShards);
    }

    /**
     * The trap the fix itself introduces, pinned so it cannot come back.
     *
     * <p>The resolver reads the index list from metadata, so a cluster state fetched without metadata
     * yields no computed shards and no indication that anything was missing. Both cat callers therefore
     * request metadata when a supplier is installed. This asserts the failing shape directly, so that if
     * someone later trims the request back for payload reasons, this says why they cannot.
     */
    public void testWithoutMetadataTheResolverCannotSeeAComputedIndex() throws Exception {
        createComputedIndexAndWaitForPrimaryMode();

        ClusterState withoutMetadata = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();

        int computedShards = 0;
        for (ShardRouting shardRouting : AbsentIndexRoutingSuppliers.allShards(withoutMetadata)) {
            if (INDEX.equals(shardRouting.getIndexName())) {
                computedShards++;
            }
        }

        assertEquals("a state without metadata cannot name a computed index, which is why cat asks for it", 0, computedShards);
    }

    // ---------------------------------------------------------------- helpers

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
            assertTrue("the computed shard must be in primary mode before cat is asked about it", shard.isPrimaryMode());
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
