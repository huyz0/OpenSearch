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
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Phase C3 of core-pluggability-refactor-plan.md.
 *
 * <p>Covers the same two things {@code IndexMetadataResolverPropagationTests} covers for {@code
 * Metadata}: that {@link RoutingTable}'s attached {@link IndexRoutingResolver} survives {@link
 * RoutingTable.Builder#Builder(RoutingTable)} and diff application (both the incremental and
 * non-incremental {@link Diff} implementations have their own {@code new RoutingTable(...)} call and
 * needed their own fix). Plus the actual consultation point this phase concluded belongs on {@link
 * ClusterState} rather than on bare {@code RoutingTable} -- see {@code RoutingTable#resolver}'s own
 * javadoc for why -- exercised here as {@link ClusterState#getIndexRoutingTable(String)}.
 */
public class IndexRoutingResolverPropagationTests extends OpenSearchTestCase {

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static IndexRoutingTable emptyRoutingTable(IndexMetadata indexMetadata) {
        return IndexRoutingTable.builder(indexMetadata.getIndex()).build();
    }

    public void testRoutingTableResolverPropagatesThroughBuilderMutation() {
        RoutingTable before = RoutingTable.builder().build();
        IndexRoutingResolver resolver = (state, meta) -> emptyRoutingTable(meta);
        before.attachIndexRoutingResolver(resolver);

        RoutingTable after = RoutingTable.builder(before).version(before.version() + 1).build();

        assertSame(resolver, after.indexRoutingResolver());
    }

    public void testRoutingTableResolverPropagatesThroughDiffApply() {
        RoutingTable before = RoutingTable.builder().build();
        IndexRoutingResolver resolver = (state, meta) -> emptyRoutingTable(meta);
        before.attachIndexRoutingResolver(resolver);

        RoutingTable after = RoutingTable.builder(before).version(before.version() + 1).build();
        Diff<RoutingTable> diff = after.diff(before);
        RoutingTable applied = diff.apply(before);

        assertSame(
            "a diff-applied RoutingTable must inherit the resolver from the pre-diff state on this node",
            resolver,
            applied.indexRoutingResolver()
        );
    }

    public void testRoutingTableResolverPropagatesThroughIncrementalDiffApply() {
        RoutingTable before = RoutingTable.builder().build();
        IndexRoutingResolver resolver = (state, meta) -> emptyRoutingTable(meta);
        before.attachIndexRoutingResolver(resolver);

        RoutingTable after = RoutingTable.builder(before).version(before.version() + 1).build();
        Diff<RoutingTable> diff = after.incrementalDiff(before);
        RoutingTable applied = diff.apply(before);

        assertSame(resolver, applied.indexRoutingResolver());
    }

    public void testClusterStateFallsThroughToRoutingResolverOnMiss() {
        IndexMetadata gated = indexMetadata("gated");
        Metadata metadata = Metadata.builder().build();
        metadata.attachIndexMetadataResolver((meta, name) -> "gated".equals(name) ? gated : null);

        IndexRoutingTable synthesized = emptyRoutingTable(gated);
        RoutingTable routingTable = RoutingTable.builder().build();
        routingTable.attachIndexRoutingResolver((state, meta) -> synthesized);

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        assertSame(
            "getIndexRoutingTable must compose the metadata resolver and the routing resolver together",
            synthesized,
            state.getIndexRoutingTable("gated")
        );
    }

    public void testClusterStateReturnsNullWhenNeitherRoutingNorMetadataResolves() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        assertNull(
            "with nothing published and nothing attached, this must behave like a plain miss -- not throw",
            state.getIndexRoutingTable("missing")
        );
    }

    public void testGetIndexRoutingTableIsNotConsultedOnAnUnsafeThread() throws InterruptedException {
        // Mirrors IndexMetadataResolverPropagationTests#testResolverIsNotConsultedOnAnUnsafeThread for the
        // routing-resolver half of the same guarantee.
        IndexMetadata gated = indexMetadata("gated");
        Metadata metadata = Metadata.builder().build();
        metadata.attachIndexMetadataResolver((meta, name) -> "gated".equals(name) ? gated : null);

        RoutingTable routingTable = RoutingTable.builder().build();
        routingTable.attachIndexRoutingResolver((state, meta) -> emptyRoutingTable(meta));

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        java.util.concurrent.atomic.AtomicReference<IndexRoutingTable> result = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        Thread clusterManagerThread = new Thread(
            () -> {
                try {
                    result.set(state.getIndexRoutingTable("gated"));
                } catch (Throwable t) {
                    failure.set(t);
                }
            },
            "opensearch[nodeA][clusterManagerService#updateTask][T#1]"
        );
        clusterManagerThread.start();
        clusterManagerThread.join();

        assertNull("a routing resolver must never be consulted from the cluster manager's own update thread", result.get());
        assertNull(failure.get());
    }

    public void testClusterStatePrefersPublishedRoutingOverTheResolver() {
        IndexMetadata real = indexMetadata("real");
        Metadata metadata = Metadata.builder().put(real, false).build();
        IndexRoutingTable published = emptyRoutingTable(real);
        RoutingTable routingTable = RoutingTable.builder().add(published).build();
        routingTable.attachIndexRoutingResolver(
            (state, meta) -> { throw new AssertionError("must not consult the resolver when routing is already published"); }
        );

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        assertSame(published, state.getIndexRoutingTable("real"));
    }
}
