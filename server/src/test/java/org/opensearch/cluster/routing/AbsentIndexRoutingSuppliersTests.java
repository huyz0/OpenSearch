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
