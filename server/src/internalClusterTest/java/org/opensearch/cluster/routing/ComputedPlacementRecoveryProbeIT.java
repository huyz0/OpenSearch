/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

/**
 * H15. Whether a computed shard costs anything before a request addresses it.
 *
 * <p>This decides H13, and the two answers point at opposite implementations, which is why it is a probe
 * rather than an argument. `IndexDescriptor` carries a suspended-shard set that nothing writes. If placement
 * is lazy, a cluster restarting with 99M sleeping indices builds a large cheap routing view and the first
 * scale-to-zero tick suspends them again, so suspension need not be durable and the field should be
 * deleted. If placement drives recovery, that same restart materializes 99M shards before anything can
 * suspend them, and the field is mandatory.
 *
 * <p>The measured quantity is an engine rather than a latency, because the question is about existence.
 * A shard whose {@code IndexService} was never created has opened no directory, read no segments and
 * allocated no engine, and no number is needed to say so.
 *
 * <p><b>The prediction, recorded before running so that being wrong is visible.</b> Computed placement is
 * supplied at read time and never published, and shard creation is driven by
 * {@code IndicesClusterStateService} applying the <em>published</em> routing table. A gated index publishes
 * nothing, so nothing should ever tell a node to open its shard. If that is right, placement is lazy and
 * H13's field can go. H9a is the reason for writing the prediction down: the last time this area predicted
 * a mechanism's behaviour without measuring it, the prediction was backwards.
 */
public class ComputedPlacementRecoveryProbeIT extends OpenSearchIntegTestCase {

    private static final String GATED = "gated-probe";
    private static final String GATED_UUID = "gated-probe-uuid";
    private static final int SHARDS = 3;

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
    }

    public void testComputedPlacementOpensNoEngine() throws Exception {
        // An existing data node rather than a new one. The framework already runs a cluster, so starting
        // another node and then demanding a stable cluster of one cannot be satisfied.
        String node = internalCluster().getDataNodeNames().iterator().next();

        AbsentIndexRoutingSuppliers.register((state, metadata) -> placementOn(node));

        ClusterState state = client().admin().cluster().prepareState().get().getState();

        // The premise: placement answers, and answers with started shards on a real node. Without this the
        // rest of the test would pass by measuring an index that does not exist in any sense.
        IndexRoutingTable computed = AbsentIndexRoutingSuppliers.resolve(state, GATED);
        assertNotNull("the premise: the gated index has computed placement", computed);
        assertEquals("with every shard placed", SHARDS, computed.shards().size());
        assertTrue(
            "and started, so this is not measuring an index that placement declined to place",
            computed.shard(0).primaryShard().started()
        );

        assertBusy(() -> {
            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
            IndexService indexService = indicesService.indexService(new Index(GATED, GATED_UUID));

            assertNull(
                "computed placement must open no engine before a request arrives. If this fails, a restart at a "
                    + "hundred million indices materializes every sleeping shard before the first scale-to-zero "
                    + "tick can suspend it, and suspension must be persisted durably on the descriptor (H13). "
                    + "If it passes, placement is lazy and the descriptor's suspended-shard field is dead weight",
                indexService
            );
            assertEquals("and no shards at all on this node for that index", 0, shardCountFor(indicesService));
        });
    }

    // ---------------------------------------------------------------- helpers

    private static int shardCountFor(IndicesService indicesService) {
        int count = 0;
        for (IndexService each : indicesService) {
            if (each.index().getName().equals(GATED)) {
                count += each.shardIds().size();
            }
        }
        return count;
    }

    /** Placement as Area C computes it: every shard started on a node, with no allocator involved. */
    private static IndexRoutingTable placementOn(String nodeName) {
        Index index = new Index(GATED, GATED_UUID);
        IndexRoutingTable.Builder placement = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < SHARDS; shard++) {
            ShardId shardId = new ShardId(index, shard);
            placement.addIndexShard(
                new IndexShardRoutingTable.Builder(shardId).addShard(
                    ComputedShardRouting.started(shardId, nodeName, RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            );
        }
        return placement.build();
    }
}
