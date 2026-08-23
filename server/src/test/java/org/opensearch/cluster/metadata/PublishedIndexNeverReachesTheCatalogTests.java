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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The published-first fast path, which is what makes an ordinary cluster pay nothing for this seam.
 *
 * <p>{@link Metadata#indexOrResolved(String)} and {@link ClusterState#getIndexRoutingTable(String)} both
 * answer from their published map <em>before</em> touching an {@link IndexCatalog}, so an index that is in
 * cluster state costs exactly what it always cost, even on a node with a catalog registered. That is a
 * property of the code's shape rather than of any single caller, which makes it exactly the kind of thing
 * that decays silently under refactoring -- so it is pinned here with a catalog that fails the test if it is
 * consulted at all, rather than merely asserting the returned value looks right (it would look right either
 * way; the published entry is what a correct catalog would return too).
 */
public class PublishedIndexNeverReachesTheCatalogTests extends OpenSearchTestCase {

    private final AtomicInteger metadataConsultations = new AtomicInteger();
    private final AtomicInteger routingConsultations = new AtomicInteger();

    @After
    public void clearRegistry() {
        IndexCatalogRegistry.register(null);
    }

    /** Counts every consultation instead of throwing, so a failure names how many rather than just "one". */
    private void registerCountingCatalog() {
        IndexCatalogRegistry.register(new IndexCatalog() {
            @Override
            public IndexMetadata resolveMetadata(Metadata metadata, String indexName) {
                metadataConsultations.incrementAndGet();
                return null;
            }

            @Override
            public IndexRoutingTable resolveRouting(ClusterState state, IndexMetadata indexMetadata) {
                routingConsultations.incrementAndGet();
                return null;
            }

            @Override
            public boolean isActive() {
                return true;
            }
        });
    }

    public void testAPublishedIndexResolvesWithoutConsultingTheCatalog() {
        registerCountingCatalog();
        IndexMetadata published = indexMetadata("ordinary");
        Metadata metadata = Metadata.builder().put(published, false).build();

        assertSame(published, metadata.indexOrResolved("ordinary"));
        assertSame(published, metadata.indexOrResolved(published.getIndex()));
        assertTrue(metadata.existsOrResolved("ordinary"));

        assertEquals("a published index must never reach the catalog", 0, metadataConsultations.get());
    }

    public void testAPublishedRoutingEntryResolvesWithoutConsultingTheCatalog() {
        registerCountingCatalog();
        IndexMetadata published = indexMetadata("ordinary");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(published, false).build())
            .routingTable(RoutingTable.builder().addAsNew(published).build())
            .build();

        IndexRoutingTable resolved = state.getIndexRoutingTable("ordinary");

        assertNotNull(resolved);
        assertSame(state.routingTable().index("ordinary"), resolved);
        assertEquals("a published routing entry must never reach the catalog", 0, routingConsultations.get());
        assertEquals(
            "and must not reach the metadata half either -- the compound question is not asked at all",
            0,
            metadataConsultations.get()
        );
    }

    /**
     * The control that stops the two tests above passing vacuously: a name with no published entry
     * <em>does</em> reach the catalog, so the counters are wired to something real.
     */
    public void testAnUnpublishedNameDoesReachTheCatalog() {
        registerCountingCatalog();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().build())
            .routingTable(RoutingTable.builder().build())
            .build();

        assertNull(state.metadata().indexOrResolved("gated"));
        assertEquals(1, metadataConsultations.get());

        assertNull(state.getIndexRoutingTable("gated"));
        assertEquals("the routing half is skipped when the metadata half has no answer", 0, routingConsultations.get());
        assertEquals(2, metadataConsultations.get());
    }

    /** With nothing registered the fast path is the only path, which is the pre-seam behavior verbatim. */
    public void testWithNoCatalogRegisteredNothingChanges() {
        IndexMetadata published = indexMetadata("ordinary");
        Metadata metadata = Metadata.builder().put(published, false).build();

        assertSame(published, metadata.indexOrResolved("ordinary"));
        assertNull(metadata.indexOrResolved("gated"));
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
