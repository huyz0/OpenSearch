/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;

/**
 * H8a. Whether a gated index can be named by a request at all.
 *
 * <p>H2e built the seam that lets resolution answer for an index cluster state does not hold, and audited
 * exactly where it belongs: {@code aliasOrIndexExists} and the {@code concreteResolvedIndices} loop, both
 * of which read {@code metadata.getIndicesLookup()}. It was never wired into either.
 *
 * <p>That is the same shape as H7a, where the mapping mechanism existed with no caller and a test that
 * called it directly passed with the wiring disabled. Here the consequence is larger: with H5 removing the
 * cluster state entry, an index that can be created and can take a mapping update still cannot be
 * <em>named</em> by any request, because every path resolves through the lookup it is absent from.
 *
 * <p>H8a wired both sites. The gated name no longer raises {@code IndexNotFoundException}, which is what
 * made it unnameable by any request. Turning the accepted name into a concrete {@code Index} carrying the
 * descriptor's uuid is H8b, so a request can resolve the name today and not yet act on the shard.
 */
public class GatedIndexResolutionGapTests extends OpenSearchTestCase {

    @After
    public void clearRegistrations() {
        AbsentIndexDescriptorSuppliers.register(null);
    }

    /**
     * The gap. A descriptor exists and answers, and resolution still cannot find the index, because the
     * resolver never consults the seam.
     */
    public void testAGatedIndexCannotBeResolvedByName() {
        AbsentIndexDescriptorSuppliers.register(
            name -> new IndexDescriptor(
                name,
                name + "-uuid",
                1,
                0,
                true,
                IndexDescriptor.State.OPEN,
                List.of(),
                Version.CURRENT.id,
                false,
                false,
                false,
                false,
                0L,
                0L
            )
        );

        // The seam answers, which is the premise: the descriptor is available and says the index exists.
        ClusterState emptyState = ClusterState.builder(ClusterName.DEFAULT).build();
        assertTrue(
            "the premise: the descriptor seam resolves this name",
            AbsentIndexDescriptorSuppliers.exists(emptyState.metadata(), "gated-idx")
        );

        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(
            new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY)
        );

        // And resolution now succeeds, where before the wiring it raised IndexNotFoundException.
        String[] resolved = resolver.concreteIndexNames(emptyState, IndicesOptions.strictExpandOpen(), "gated-idx");

        assertEquals("a gated index must be nameable by a request", 1, resolved.length);
        assertEquals("gated-idx", resolved[0]);
    }

    /**
     * The control that keeps this from being a latency regression on every cluster. A name that is
     * genuinely missing, with no descriptor for it, must still raise rather than silently resolving, and
     * a name present in the lookup must never reach the seam at all.
     */
    public void testAGenuinelyMissingNameStillRaises() {
        AbsentIndexDescriptorSuppliers.register(name -> null);
        ClusterState emptyState = ClusterState.builder(ClusterName.DEFAULT).build();

        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(
            new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY)
        );

        expectThrows(
            IndexNotFoundException.class,
            () -> resolver.concreteIndexNames(emptyState, IndicesOptions.strictExpandOpen(), "never-existed")
        );
    }

    /**
     * The control. An index present in cluster state resolves, so the failure above is about the gate
     * rather than about the resolver being broken for everything.
     */
    public void testAnOrdinaryIndexStillResolves() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata("ordinary"), false).build())
            .build();

        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(
            new org.opensearch.common.util.concurrent.ThreadContext(Settings.EMPTY)
        );

        String[] resolved = resolver.concreteIndexNames(state, IndicesOptions.strictExpandOpen(), "ordinary");

        assertEquals("an ordinary index must still resolve", 1, resolved.length);
        assertEquals("ordinary", resolved[0]);
    }

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
}
