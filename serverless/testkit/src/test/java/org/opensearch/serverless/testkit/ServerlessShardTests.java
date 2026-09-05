/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Phase 2: a shard is opened from a descriptor and serves a search, and the reconciler holds the
 * invariant S1 established.
 */
public class ServerlessShardTests extends OpenSearchTestCase {

    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-phase2")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    private static IndexDescriptor descriptor(String name, String uuid) {
        return new IndexDescriptor(name, uuid, 1, MAPPING, null);
    }

    /** S0 made durable: the same proof, now through production code and a descriptor. */
    public void testShardOpensFromADescriptorAndServesASearch() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("p2-a"))) {
            node.start();
            final IndexDescriptor alpha = descriptor("alpha", "uuid-alpha-00000000");
            final ShardAssignment assignment = new ShardAssignment("alpha", 0, 7L);

            final Set<ShardId> opened = node.applyTruth(List.of(alpha), List.of(assignment));
            assertEquals("exactly one shard should have opened", 1, opened.size());

            final ShardId shardId = opened.iterator().next();
            final IndexShard shard = node.reconciler().shard(shardId);
            assertEquals(IndexShardState.STARTED, shard.state());
            assertEquals("the shard-head term must reach the shard", 7L, shard.getOperationPrimaryTerm());

            ShardOps.indexDoc(shard, "1", "{\"msg\":\"hello serverless\",\"n\":1}");
            ShardOps.indexDoc(shard, "2", "{\"msg\":\"hello serverless\",\"n\":2}");
            shard.refresh("phase2");

            assertEquals("the shard served nothing", 2L, ShardOps.hits(node.searchService(), shardId, "msg", "hello"));
        }
    }

    /**
     * The invariant, through the production API: a view that omits a hosted index must not close it.
     *
     * <p>S1 measured this against test code modelling a diff rule. This asserts it of
     * {@code ShardReconciler}, which is the thing that will actually be in the path.
     */
    public void testAViewOmittingAHostedIndexDoesNotCloseIt() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("p2-b"))) {
            node.start();
            final IndexDescriptor alpha = descriptor("alpha", "uuid-alpha-00000000");
            final ShardId shardId = node.applyTruth(List.of(alpha), List.of(new ShardAssignment("alpha", 0, 1L))).iterator().next();
            final IndexShard shard = node.reconciler().shard(shardId);
            ShardOps.indexDoc(shard, "1", "{\"msg\":\"still here\",\"n\":1}");
            shard.refresh("phase2");
            assertEquals(1L, ShardOps.hits(node.searchService(), shardId, "msg", "still"));

            // A projection naming nothing at all. Truth has not changed; the view simply forgot.
            node.applyTruth(List.of(), List.of());

            assertFalse(
                "the view must omit the index for this test to mean anything",
                node.clusterService().state().metadata().hasIndex("alpha")
            );
            assertEquals("a projected view closed a live shard", IndexShardState.STARTED, shard.state());
            assertEquals(
                "the shard stopped serving after a partial view",
                1L,
                ShardOps.hits(node.searchService(), shardId, "msg", "still")
            );
            assertTrue("the reconciler forgot a shard it still holds", node.reconciler().openShards().contains(shardId));
        }
    }

    /** Ownership loss is the only thing that closes a shard, and it actually does. */
    public void testReleaseClosesTheShard() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("p2-c"))) {
            node.start();
            final IndexDescriptor alpha = descriptor("alpha", "uuid-alpha-00000000");
            final ShardId shardId = node.applyTruth(List.of(alpha), List.of(new ShardAssignment("alpha", 0, 1L))).iterator().next();
            assertEquals(1, node.reconciler().openShards().size());

            node.releaseShard(shardId, "shard-head says another node owns it now");

            assertTrue("release did not close the shard", node.reconciler().openShards().isEmpty());
            assertNull(node.reconciler().shard(shardId));
        }
    }

    /** Re-applying the same truth must not reopen, re-recover, or double-count. */
    public void testApplyTruthIsIdempotent() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("p2-d"))) {
            node.start();
            final IndexDescriptor alpha = descriptor("alpha", "uuid-alpha-00000000");
            final List<ShardAssignment> owned = List.of(new ShardAssignment("alpha", 0, 1L));

            assertEquals(1, node.applyTruth(List.of(alpha), owned).size());
            assertEquals("a second apply must open nothing", 0, node.applyTruth(List.of(alpha), owned).size());
            assertEquals("the node must still hold exactly one shard", 1, node.reconciler().openShards().size());
        }
    }

    /**
     * An assignment with no matching descriptor is a projector bug. It must throw rather than open
     * nothing and report success — the failure mode this branch keeps meeting.
     */
    public void testAnAssignmentWithoutADescriptorFailsLoudly() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("p2-e"))) {
            node.start();
            final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> node.applyTruth(List.of(), List.of(new ShardAssignment("ghost", 0, 1L)))
            );
            assertTrue(
                "the error must name the problem: " + e.getMessage(),
                e.getMessage().contains("no descriptor in the projected view")
            );
        }
    }

    /** Two nodes, disjoint descriptors, both serving — section 5 at N>1, now through production code. */
    public void testTwoNodesServeDisjointIndices() throws Exception {
        try (ServerlessNode a = new ServerlessNode(nodeSettings("p2-f1")); ServerlessNode b = new ServerlessNode(nodeSettings("p2-f2"))) {
            a.start();
            b.start();
            final IndexDescriptor alpha = descriptor("alpha", "uuid-alpha-00000000");
            final IndexDescriptor beta = descriptor("beta", "uuid-beta-000000000");

            final ShardId alphaShard = a.applyTruth(List.of(alpha), List.of(new ShardAssignment("alpha", 0, 1L))).iterator().next();
            final ShardId betaShard = b.applyTruth(List.of(beta), List.of(new ShardAssignment("beta", 0, 1L))).iterator().next();

            ShardOps.indexDoc(a.reconciler().shard(alphaShard), "1", "{\"msg\":\"alpha doc\",\"n\":1}");
            ShardOps.indexDoc(b.reconciler().shard(betaShard), "1", "{\"msg\":\"beta doc\",\"n\":1}");
            ShardOps.indexDoc(b.reconciler().shard(betaShard), "2", "{\"msg\":\"beta doc\",\"n\":2}");
            a.reconciler().shard(alphaShard).refresh("phase2");
            b.reconciler().shard(betaShard).refresh("phase2");

            assertEquals(1L, ShardOps.hits(a.searchService(), alphaShard, "msg", "alpha"));
            assertEquals(2L, ShardOps.hits(b.searchService(), betaShard, "msg", "beta"));
            assertFalse("node A's view leaked beta", a.clusterService().state().metadata().hasIndex("beta"));
            assertFalse("node B's view leaked alpha", b.clusterService().state().metadata().hasIndex("alpha"));
        }
    }
}
