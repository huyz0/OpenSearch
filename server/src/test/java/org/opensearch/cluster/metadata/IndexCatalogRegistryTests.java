/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * The node-level holder that replaced the per-cluster-state resolver attachment, its propagation through
 * every {@code Builder}/diff, and the high-priority {@code ClusterStateApplier} that seeded from-scratch
 * instances (this class supersedes {@code ResolverAttachingClusterStateApplierTests} and the two
 * {@code *ResolverPropagationTests}).
 *
 * <p><b>What those three suites were really pinning, and what pins it now.</b> They existed because a
 * callback stored on a {@code Metadata}/{@code RoutingTable} instance is lost the moment a new instance is
 * produced -- by a {@code Builder} copy, by diff application, or by a full (non-diff) state sync that
 * deserializes fresh objects -- so "every node but the one that originally built a Metadata locally would
 * silently lose its resolver on the very next cluster state update". A node-scoped catalog cannot lose
 * itself that way: {@link #testAFromScratchStateResolvesWithNoAttachmentStep} is the direct replacement for
 * all of that machinery's tests, and it passes for the same reason the machinery is gone.
 *
 * <p>The other thing those suites pinned was a real bug they found: attaching to {@link
 * Metadata#EMPTY_METADATA}/{@link RoutingTable#EMPTY_ROUTING_TABLE} -- {@code ClusterState.Builder}'s
 * defaults, shared JVM-wide singletons -- leaked one plugin's resolver into every unrelated {@code
 * ClusterState} that also defaulted to them, and in a test JVM into every unrelated test. That entire class
 * of bug is gone by construction now, since nothing is attached to any instance; {@link
 * #testABareStateBuiltOnTheSharedSingletonsStillResolves} pins the flip side, which is that the singletons
 * no longer have to be special-cased to stay safe.
 *
 * <p>Since this is a static, process-wide registry, every test unregisters in {@code @After} -- a
 * registration left behind leaks into whichever unrelated test runs next in the same JVM.
 */
public class IndexCatalogRegistryTests extends OpenSearchTestCase {

    @After
    public void clearRegistry() {
        IndexCatalogRegistry.register(null);
    }

    public void testNothingRegisteredAnswersExactlyAsCoreDidBeforeTheSeamExisted() {
        assertFalse(IndexCatalogRegistry.isRegistered());
        assertFalse("with no catalog the feature is off, which all four isActive() sites need", IndexCatalogRegistry.isActive());
        assertNull(IndexCatalogRegistry.resolveMetadata(Metadata.builder().build(), "anything"));
        assertNull(IndexCatalogRegistry.resolveRouting(ClusterState.builder(ClusterName.DEFAULT).build(), indexMetadata("anything")));
        assertTrue("an ordinary cluster publishes routing for every index", IndexCatalogRegistry.shouldPublishRouting(indexMetadata("i")));
    }

    /**
     * {@code isActive()} is not {@code isRegistered()}: a plugin supplies its catalog unconditionally at
     * node startup and the feature underneath it toggles at runtime. This is the distinction the four
     * migrated call sites depend on, and the one an "is a resolver attached" check could not express.
     */
    public void testIsActiveIsIndependentOfIsRegistered() {
        boolean[] on = { false };
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public boolean isActive() {
                return on[0];
            }
        });

        assertTrue(IndexCatalogRegistry.isRegistered());
        assertFalse("registered but switched off must not read as active", IndexCatalogRegistry.isActive());

        on[0] = true;
        assertTrue("the answer must be read live on every call, not captured at registration", IndexCatalogRegistry.isActive());

        on[0] = false;
        assertFalse("and must follow the feature back off again", IndexCatalogRegistry.isActive());
    }

    /**
     * A catalog registered on the node answers for a {@code Metadata} it was never attached to, including
     * one built from scratch the way a full (non-diff) cluster state sync produces. This single test
     * replaces the whole propagation suite.
     */
    public void testAFromScratchStateResolvesWithNoAttachmentStep() {
        IndexMetadata gated = indexMetadata("gated");
        IndexRoutingTable computed = IndexRoutingTable.builder(gated.getIndex()).build();
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
                return "gated".equals(indexName) ? gated : null;
            }

            @Override
            public IndexRoutingTable resolveRouting(ClusterState state, IndexMetadata indexMetadata) {
                return computed;
            }

            @Override
            public boolean isActive() {
                return true;
            }
        });

        // Brand-new instances, exactly what a full-state deserialization hands a node: nothing on this
        // path ever passed through a Builder copy or a diff apply on this node.
        ClusterState freshFullState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .build();

        assertSame(gated, freshFullState.metadata().indexOrResolved("gated"));
        assertSame(computed, freshFullState.getIndexRoutingTable("gated"));
    }

    /** The shared-singleton case that used to need an explicit guard in two attach methods. */
    public void testABareStateBuiltOnTheSharedSingletonsStillResolves() {
        IndexMetadata gated = indexMetadata("gated");
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
                return "gated".equals(indexName) ? gated : null;
            }

            @Override
            public boolean isActive() {
                return true;
            }
        });

        ClusterState bare = ClusterState.builder(ClusterName.DEFAULT).build();
        assertSame(Metadata.EMPTY_METADATA, bare.metadata());
        assertSame(RoutingTable.EMPTY_ROUTING_TABLE, bare.routingTable());

        assertSame(
            "a bare state used to resolve nothing, because attachment skipped the singletons",
            gated,
            bare.metadata().indexOrResolved("gated")
        );
    }

    /**
     * The deliberately non-uniform exception policy: the predicates fall back to their safe direction, the
     * resolvers do not swallow. Collapsing a "temporarily unavailable" throw into {@code null} would turn it
     * into "does not exist", the one confusion this whole area exists to avoid.
     */
    public void testPredicatesSwallowAndResolversDoNot() {
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
                throw new DescriptorUnavailableException(indexName, new IllegalStateException("backend down"));
            }

            @Override
            public IndexRoutingTable resolveRouting(ClusterState state, IndexMetadata indexMetadata) {
                throw new IllegalStateException("boom");
            }

            @Override
            public boolean shouldPublishRouting(IndexMetadata indexMetadata) {
                throw new IllegalStateException("boom");
            }

            @Override
            public boolean isActive() {
                throw new IllegalStateException("boom");
            }
        });

        assertTrue(
            "a throwing catalog must not stop an ordinary index publishing routing",
            IndexCatalogRegistry.shouldPublishRouting(indexMetadata("i"))
        );
        assertFalse("a throwing catalog must read as off, not on", IndexCatalogRegistry.isActive());
        expectThrows(DescriptorUnavailableException.class, () -> IndexCatalogRegistry.resolveMetadata(Metadata.builder().build(), "gated"));
        expectThrows(
            IllegalStateException.class,
            () -> IndexCatalogRegistry.resolveRouting(ClusterState.builder(ClusterName.DEFAULT).build(), indexMetadata("gated"))
        );
    }

    /** Registering null restores the unregistered default, which is how every test in this repo cleans up. */
    public void testRegisteringNullClearsIt() {
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public boolean isActive() {
                return true;
            }
        });
        assertTrue(IndexCatalogRegistry.isActive());

        IndexCatalogRegistry.register(null);

        assertFalse(IndexCatalogRegistry.isRegistered());
        assertFalse(IndexCatalogRegistry.isActive());
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
