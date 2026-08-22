/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * C3. The seam that lets a routing entry be computed rather than published.
 *
 * <p>The plan called for mirroring the {@code Metadata} holder change on {@link RoutingTable}: make its
 * internal map hold suppliers. That map has around forty internal references including the diff and both
 * serializers, and unlike {@code Metadata}'s holders -- which resolve before serialization --
 * {@link RoutingTable} is diffed and serialized on every cluster state publication. A lazy entry there
 * would have to define what a diff of an unresolved entry means, and getting that wrong yields either no
 * saving or two nodes reading one state differently.
 *
 * <p>The question turned out not to need answering. An entry computed identically on every node, from
 * inputs every node already has, does not need publishing. So a serverless index publishes nothing and
 * each node fills the gap locally, which is legal only because Phase A already taught the request paths
 * to tolerate an absent entry. The seam is therefore a hook at the point where Phase A degrades, not a
 * restructuring of the published state.
 */
public class AbsentIndexRoutingSuppliersTests extends OpenSearchTestCase {

    @After
    public void clearUnpublished() {
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    @After
    public void clearSupplier() {
        // A static registry leaks into unrelated tests if it is not cleared, and the failure that
        // produces is another suite's test failing for reasons nothing in it explains.
        AbsentIndexRoutingSuppliers.register(null);
    }

    public void testNothingIsRegisteredByDefault() {
        assertFalse(AbsentIndexRoutingSuppliers.isRegistered());
        assertNull(AbsentIndexRoutingSuppliers.supply(state(), index("idx")));
    }

    public void testARegisteredSupplierAnswers() {
        IndexMetadata metadata = index("idx");
        IndexRoutingTable computed = IndexRoutingTable.builder(metadata.getIndex())
            .addIndexShard(new IndexShardRoutingTable.Builder(new org.opensearch.core.index.shard.ShardId(metadata.getIndex(), 0)).build())
            .build();

        AbsentIndexRoutingSuppliers.register((state, index) -> computed);

        assertTrue(AbsentIndexRoutingSuppliers.isRegistered());
        assertSame(computed, AbsentIndexRoutingSuppliers.supply(state(), metadata));
    }

    /** Declining is how a supplier says "not my index" without disabling itself for every other one. */
    public void testASupplierMayDecline() {
        AbsentIndexRoutingSuppliers.register((state, index) -> null);

        assertTrue(AbsentIndexRoutingSuppliers.isRegistered());
        assertNull(AbsentIndexRoutingSuppliers.supply(state(), index("idx")));
    }

    /**
     * The property worth having a test for. This is already a degradation path, reached when an index
     * has no routing entry at all. A plugin bug here must not turn "no shard available" into a failed
     * request, or installing the hook makes the system less robust than not installing it.
     */
    public void testAThrowingSupplierDegradesRatherThanFailingTheRequest() {
        AbsentIndexRoutingSuppliers.register((state, index) -> { throw new IllegalStateException("plugin bug"); });

        assertNull("a throwing supplier must read as no answer", AbsentIndexRoutingSuppliers.supply(state(), index("idx")));
    }

    public void testRegisteringNullRestoresTheDefault() {
        AbsentIndexRoutingSuppliers.register((state, index) -> null);
        assertTrue(AbsentIndexRoutingSuppliers.isRegistered());

        AbsentIndexRoutingSuppliers.register(null);
        assertFalse(AbsentIndexRoutingSuppliers.isRegistered());
    }

    public void testLastRegistrationWins() {
        IndexMetadata metadata = index("idx");
        IndexRoutingTable first = IndexRoutingTable.builder(metadata.getIndex()).build();
        IndexRoutingTable second = IndexRoutingTable.builder(metadata.getIndex()).build();

        AbsentIndexRoutingSuppliers.register((state, index) -> first);
        AbsentIndexRoutingSuppliers.register((state, index) -> second);

        assertSame(second, AbsentIndexRoutingSuppliers.supply(state(), metadata));
    }

    /**
     * The gap the first pass through this area missed entirely. A supplier only runs when an index has no
     * published routing entry, and index creation publishes one for every index it creates -- so without
     * a way to opt an index out of publication, the supplier is installed and never invoked. The
     * mechanism looked wired and was dead, and only asking what an integration test would exercise
     * surfaced it.
     */
    public void testRoutingIsPublishedByDefault() {
        assertTrue(AbsentIndexRoutingSuppliers.shouldPublishRouting(index("idx")));
    }

    public void testAnIndexCanBeOptedOutOfPublication() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> metadata.getIndex().getName().startsWith("computed-"));

        assertFalse(AbsentIndexRoutingSuppliers.shouldPublishRouting(index("computed-idx")));
        assertTrue("an unclaimed index must still publish", AbsentIndexRoutingSuppliers.shouldPublishRouting(index("classic-idx")));
    }

    /**
     * Publishing routing that is then ignored is recoverable; not publishing routing that is then needed
     * is an index with nowhere to live. A broken predicate therefore fails towards publishing.
     */
    public void testAThrowingPredicateFailsTowardsPublishing() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> { throw new IllegalStateException("plugin bug"); });

        assertTrue(AbsentIndexRoutingSuppliers.shouldPublishRouting(index("idx")));
    }

    public void testNullMetadataPublishes() {
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> true);

        assertTrue(AbsentIndexRoutingSuppliers.shouldPublishRouting(null));
    }

    /**
     * The memo is the point of the cache, so the reuse it exists for is asserted before the invalidation
     * rules that constrain it. Same state, same index: one computation.
     */
    public void testTheSameStateReusesTheMemoisedPlacement() {
        IndexMetadata metadata = index("idx");
        java.util.concurrent.atomic.AtomicInteger computations = new java.util.concurrent.atomic.AtomicInteger();
        AbsentIndexRoutingSuppliers.register((state, index) -> {
            computations.incrementAndGet();
            return IndexRoutingTable.builder(index.getIndex()).build();
        });
        ClusterState state = state();

        IndexRoutingTable first = AbsentIndexRoutingSuppliers.supply(state, metadata);
        IndexRoutingTable second = AbsentIndexRoutingSuppliers.supply(state, metadata);

        assertSame("the second resolution of an unchanged state must come from the memo", first, second);
        assertEquals(1, computations.get());
    }

    /**
     * The bug this key exists to prevent, in the smallest shape that reproduces it.
     *
     * <p>A computed placement depends on the cluster's member list, which lives in a {@code
     * Metadata.Custom} and therefore changes the {@code Metadata} without touching the index's own {@code
     * IndexMetadata} -- and for a gated index the {@code IndexMetadata} is a deliberately identity-cached
     * synthesis, so it never changes at all. Keyed only on the index, the memo kept answering from the
     * member list of an earlier epoch indefinitely: after a node joined or was decommissioned, this node
     * went on routing computed shards against a node list nobody else had, which is how one coordinator's
     * derived allocation id stops matching the data node's and writes start failing outright.
     */
    public void testAChangeToClusterMetadataInvalidatesTheMemo() {
        IndexMetadata metadata = index("idx");
        java.util.concurrent.atomic.AtomicInteger computations = new java.util.concurrent.atomic.AtomicInteger();
        AbsentIndexRoutingSuppliers.register((state, index) -> {
            computations.incrementAndGet();
            return IndexRoutingTable.builder(index.getIndex()).build();
        });

        AbsentIndexRoutingSuppliers.supply(state(), metadata);
        // A new Metadata instance carrying the same indices, which is exactly what publishing a changed
        // membership custom produces: the index is untouched, the metadata is not.
        ClusterState epochBumped = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(org.opensearch.cluster.metadata.Metadata.builder().build())
            .build();
        AbsentIndexRoutingSuppliers.supply(epochBumped, metadata);

        assertEquals("a changed metadata must not be answered from a memo computed against the old one", 2, computations.get());
    }

    /**
     * The other half of the same key. While no membership is published -- a fresh cluster, before the
     * maintainer's first update -- placement falls back to the live node list, so a node joining or leaving
     * has to invalidate the memo just as a published change does.
     */
    public void testAChangeToTheNodeListInvalidatesTheMemo() {
        IndexMetadata metadata = index("idx");
        java.util.concurrent.atomic.AtomicInteger computations = new java.util.concurrent.atomic.AtomicInteger();
        AbsentIndexRoutingSuppliers.register((state, index) -> {
            computations.incrementAndGet();
            return IndexRoutingTable.builder(index.getIndex()).build();
        });
        ClusterState before = state();

        AbsentIndexRoutingSuppliers.supply(before, metadata);
        ClusterState afterJoin = ClusterState.builder(before)
            .nodes(
                org.opensearch.cluster.node.DiscoveryNodes.builder()
                    .add(
                        new org.opensearch.cluster.node.DiscoveryNode(
                            "node-1",
                            new org.opensearch.core.common.transport.TransportAddress(java.net.InetAddress.getLoopbackAddress(), 9300),
                            java.util.Map.of(),
                            java.util.Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
                            Version.CURRENT
                        )
                    )
                    .build()
            )
            .build();
        AbsentIndexRoutingSuppliers.supply(afterJoin, metadata);

        assertEquals("a node joining must not be answered from a memo computed without it", 2, computations.get());
    }

    /**
     * The memo is bounded, and the bound it used to claim was the unbounded quantity itself: "the number of
     * computed indices this node resolves" is not a bound at all for indices that have no routing entry to
     * hold them. A long-lived coordinator touching a large gated population pinned a synthesised {@code
     * IndexMetadata} and a routing table per index, forever.
     */
    public void testTheMemoDoesNotGrowWithTheNumberOfIndicesResolved() {
        AbsentIndexRoutingSuppliers.register((state, index) -> IndexRoutingTable.builder(index.getIndex()).build());
        ClusterState state = state();

        int resolved = 60_000;
        for (int i = 0; i < resolved; i++) {
            AbsentIndexRoutingSuppliers.supply(state, index("idx-" + i));
        }

        int memoised = AbsentIndexRoutingSuppliers.memoisedIndexCountForTesting();
        assertTrue("resolving " + resolved + " indices must not leave " + resolved + " memos: found " + memoised, memoised < resolved);
    }

    private static ClusterState state() {
        return ClusterState.builder(ClusterName.DEFAULT).build();
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
