/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.cluster.metadata.IndexMetadataResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingResolver;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

/**
 * Phase C (final step) of core-pluggability-refactor-plan.md: this is the piece that gives a real node's
 * {@code Metadata}/{@code RoutingTable} their resolver in the first place, wired into {@code Node} as a
 * high-priority {@link ClusterStateApplier}. See the class's own javadoc for why high priority and why
 * attach-on-null is the right steady-state behavior; this covers both.
 *
 * <p>Every test here builds its {@link ClusterState} with an explicit, freshly-constructed {@link Metadata}
 * / {@link RoutingTable} rather than relying on {@link ClusterState.Builder}'s defaults -- those defaults
 * are {@link Metadata#EMPTY_METADATA} / {@link RoutingTable#EMPTY_ROUTING_TABLE}, shared JVM-wide
 * singletons that must never actually receive an attached resolver (see {@code
 * testDoesNotAttachToTheSharedEmptySingletons} below, and the attach methods' own javadoc) -- an earlier
 * version of this test suite built states without setting either explicitly and, as a direct result,
 * mutated those singletons and leaked a resolver across unrelated tests in the same JVM.
 */
public class ResolverAttachingClusterStateApplierTests extends OpenSearchTestCase {

    private static final IndexMetadataResolver METADATA_RESOLVER = (metadata, name) -> null;
    private static final IndexRoutingResolver ROUTING_RESOLVER = (state, meta) -> null;

    private static ClusterState freshEmptyState() {
        // Metadata.builder().build() / RoutingTable.builder().build() are distinct instances each call --
        // NOT the same reference as the shared EMPTY_METADATA/EMPTY_ROUTING_TABLE singletons -- so they are
        // safe to mutate and representative of what a real applied cluster state looks like.
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .build();
    }

    public void testAttachesBothResolversWhenNeitherIsSet() {
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(
            Optional.of(METADATA_RESOLVER),
            Optional.of(ROUTING_RESOLVER)
        );

        ClusterState state = freshEmptyState();
        applier.applyClusterState(new ClusterChangedEvent("test", state, state));

        assertSame(METADATA_RESOLVER, state.metadata().indexMetadataResolver());
        assertSame(ROUTING_RESOLVER, state.routingTable().indexRoutingResolver());
    }

    public void testDoesNotOverwriteAnAlreadyAttachedResolver() {
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(
            Optional.of(METADATA_RESOLVER),
            Optional.of(ROUTING_RESOLVER)
        );

        IndexMetadataResolver alreadyThere = (metadata, name) -> null;
        IndexRoutingResolver routingAlreadyThere = (state, meta) -> null;
        ClusterState state = freshEmptyState();
        state.metadata().attachIndexMetadataResolver(alreadyThere);
        state.routingTable().attachIndexRoutingResolver(routingAlreadyThere);

        applier.applyClusterState(new ClusterChangedEvent("test", state, state));

        assertSame(
            "must not clobber a resolver this same Metadata already carries forward from a prior apply",
            alreadyThere,
            state.metadata().indexMetadataResolver()
        );
        assertSame(
            "must not clobber a resolver this same RoutingTable already carries forward from a prior apply",
            routingAlreadyThere,
            state.routingTable().indexRoutingResolver()
        );
    }

    public void testIsANoOpWhenNoResolverIsConfigured() {
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(Optional.empty(), Optional.empty());

        ClusterState state = freshEmptyState();
        applier.applyClusterState(new ClusterChangedEvent("test", state, state));

        assertNull(state.metadata().indexMetadataResolver());
        assertNull(state.routingTable().indexRoutingResolver());
    }

    public void testOnlyMetadataResolverConfiguredLeavesRoutingUntouched() {
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(
            Optional.of(METADATA_RESOLVER),
            Optional.empty()
        );

        ClusterState state = freshEmptyState();
        applier.applyClusterState(new ClusterChangedEvent("test", state, state));

        assertSame(METADATA_RESOLVER, state.metadata().indexMetadataResolver());
        assertNull(state.routingTable().indexRoutingResolver());
    }

    public void testOnlyRoutingResolverConfiguredLeavesMetadataUntouched() {
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(
            Optional.empty(),
            Optional.of(ROUTING_RESOLVER)
        );

        ClusterState state = freshEmptyState();
        applier.applyClusterState(new ClusterChangedEvent("test", state, state));

        assertNull(state.metadata().indexMetadataResolver());
        assertSame(ROUTING_RESOLVER, state.routingTable().indexRoutingResolver());
    }

    public void testFreshFullStateSyncGetsTheResolverAttachedAgain() {
        // Simulates a full (non-diff) cluster state sync: a brand-new RoutingTable/Metadata instance that
        // never went through this node's own Builder-copy/diff-apply propagation path, so it starts out
        // with a null resolver even though this node has one configured -- see the class javadoc's "not
        // just the node's first" note.
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(
            Optional.of(METADATA_RESOLVER),
            Optional.of(ROUTING_RESOLVER)
        );

        ClusterState firstSync = freshEmptyState();
        applier.applyClusterState(new ClusterChangedEvent("first", firstSync, firstSync));
        assertSame(METADATA_RESOLVER, firstSync.metadata().indexMetadataResolver());

        // A brand new RoutingTable/Metadata, as a fresh full-state deserialization would produce.
        ClusterState freshFullState = freshEmptyState();
        assertNull(freshFullState.metadata().indexMetadataResolver());

        applier.applyClusterState(new ClusterChangedEvent("second", freshFullState, firstSync));

        assertSame(METADATA_RESOLVER, freshFullState.metadata().indexMetadataResolver());
        assertSame(ROUTING_RESOLVER, freshFullState.routingTable().indexRoutingResolver());
    }

    /**
     * The bug this test exists to pin down: {@link ClusterState.Builder}'s default metadata/routing table
     * -- used whenever a caller builds a {@code ClusterState} without setting them explicitly -- are the
     * literal shared singletons {@link Metadata#EMPTY_METADATA}/{@link RoutingTable#EMPTY_ROUTING_TABLE}.
     * If the applier (or the attach methods it calls) ever mutated those in place, one plugin's resolver
     * would leak into every other unrelated {@code ClusterState} on this node -- and in a test JVM, into
     * every other test that also builds a bare {@code ClusterState}, regardless of test order or class.
     */
    public void testDoesNotAttachToTheSharedEmptySingletons() {
        ResolverAttachingClusterStateApplier applier = new ResolverAttachingClusterStateApplier(
            Optional.of(METADATA_RESOLVER),
            Optional.of(ROUTING_RESOLVER)
        );

        // Deliberately NOT setting .metadata()/.routingTable() -- this is the exact shape of ClusterState
        // that leaked the bug: Builder falls back to the shared EMPTY_METADATA/EMPTY_ROUTING_TABLE.
        ClusterState bareState = ClusterState.builder(ClusterName.DEFAULT).build();
        assertSame(Metadata.EMPTY_METADATA, bareState.metadata());
        assertSame(RoutingTable.EMPTY_ROUTING_TABLE, bareState.routingTable());

        applier.applyClusterState(new ClusterChangedEvent("test", bareState, bareState));

        assertNull(
            "must never attach to the shared EMPTY_METADATA singleton -- doing so leaks across every "
                + "other ClusterState that defaults to it",
            Metadata.EMPTY_METADATA.indexMetadataResolver()
        );
        assertNull(
            "must never attach to the shared EMPTY_ROUTING_TABLE singleton -- doing so leaks across every "
                + "other ClusterState that defaults to it",
            RoutingTable.EMPTY_ROUTING_TABLE.indexRoutingResolver()
        );
    }
}
