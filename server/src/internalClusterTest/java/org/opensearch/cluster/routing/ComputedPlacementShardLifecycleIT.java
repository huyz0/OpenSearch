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

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerLocalShards(null);
    }

    /** The claim: a data node opens the shards a computed index says it owns. */
    public void testADataNodeOpensTheComputedShard() throws Exception {
        registerComputedPlacement();
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
        registerComputedPlacement();
        createComputedIndex();

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
     * The consequence: a document can be written and read back. Not yet true, and the reason is worth
     * more than the assertion, so this is disabled rather than deleted.
     *
     * <p><b>How far the write gets, run by run.</b> Each fix moved the failure one layer down, which is
     * the argument for having written this before the code rather than after it.
     *
     * <ol>
     *   <li>No shard opened, and <b>no node-side error at all</b>. {@code failMissingShards} was failing
     *       the shard because the local view called it active while the node did not have it, and
     *       {@code createIndices} then skipped it through {@code failedShardsCache}. Fixed by having the
     *       local view report INITIALIZING.</li>
     *   <li>Shard opens. Write fails "primary shard is not active":
     *       {@code TransportReplicationAction} read the routing table directly and got null. Fixed by
     *       resolving through the supplier, the third site to make that same mistake.</li>
     *   <li>Write reaches the primary. "shard is not in primary mode", because
     *       {@code inSyncAllocationIds} is cluster-manager-maintained and a computed index never gets
     *       any. Partly addressed by reading the in-sync set from the placement.</li>
     *   <li>Still not in primary mode. A logging probe then showed the in-sync path was never called at
     *       all: {@code updateShard} runs only on a cluster state applied after the one that created the
     *       shard, and an idle cluster publishes no such state for a computed index. The transition was
     *       moved to recovery completion, where it is the tracker's first and only call.</li>
     *   <li>"primary term must be positive but was [0]". A term is bumped by the cluster manager when it
     *       assigns a primary, so an index the allocator never touches keeps zero, and
     *       {@code activatePrimaryMode} fails adding the peer recovery retention lease.</li>
     *   <li>"term is only increased as part of primary promotion", from substituting the term on the node:
     *       the shard is constructed from metadata, so a node-local term disagrees with its own shard.
     *       Setting it at creation instead, where for a computed index creation is the assignment, keeps
     *       every reader agreeing.</li>
     *   <li>"engine is closed", with the tracker's checkpoint invariant firing again.</li>
     *   <li>Current state, and the reason this is still disabled. The write now sometimes succeeds, but
     *       it takes sixty seconds either way. Measured over three runs: 60.16s pass, 60.38s fail,
     *       60.29s pass. Sixty seconds is the replication retry timeout almost exactly, so the shard is
     *       not being made writable by the recovery transition at all. The request retries for a minute
     *       and either wins the race at the boundary or does not. A test that passes by timing out into
     *       success is worse than one honestly marked broken, which is why the marker stays until a
     *       passing run is both fast and repeatable.</li>
     * </ol>
     *
     * <p>A measured non-result belongs here too: the version gate in
     * {@code ReplicationTracker.updateFromClusterManager}, which ignores an update unless the cluster
     * state version is strictly newer, looked like the cause and is not. Passing a higher version
     * changed nothing. That was a reasoned answer and it was wrong, which is the third time in this
     * project that reasoning lost to measurement.
     *
     * <p>Also recorded: driving the START transition from {@code handleRecoveryDone} rather than from
     * the next applied cluster state is <em>necessary</em> but not sufficient, and the attempt made
     * things worse rather than better. It tripped the tracker's invariant during recovery, which failed
     * and removed the shard, so the sibling test below regressed from passing to "no such shard". The
     * necessity stands: on an idle cluster nothing publishes a new state for a computed index, so a
     * transition that waits for one waits forever.
     */
    @org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "C18: a computed shard never enters primary mode. See this method's javadoc and "
        + "plan-area-c-computed-placement.md.")
    public void testADocumentCanBeIndexedAndRead() throws Exception {
        registerComputedPlacement();
        createComputedIndex();

        IndexResponse response = client().prepareIndex(INDEX).setId("1").setSource("field", "value").get();

        assertEquals(RestStatus.CREATED, response.status());
        client().admin().indices().prepareRefresh(INDEX).get();
        assertEquals(1, client().prepareGet(INDEX, "1").get().isExists() ? 1 : 0);
    }

    private void createComputedIndex() {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            ).setWaitForActiveShards(ActiveShardCount.ALL).setTimeout(TimeValue.timeValueSeconds(30))
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
