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
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * The one adapter that replaced {@code SupplierBackedIndexMetadataResolverTests} and {@code
 * SupplierBackedIndexRoutingResolverTests}, whose cases are merged here unchanged: the two adapters were
 * two classes in two packages forwarding to the two static registries for one plugin.
 *
 * <p>Both static registries are process-wide, so everything registered here is unregistered in {@code
 * @After} -- a registration left behind leaks into whichever unrelated test runs next in the same JVM.
 */
public class SupplierBackedIndexCatalogTests extends OpenSearchTestCase {

    private final SupplierBackedIndexCatalog catalog = new SupplierBackedIndexCatalog();

    @After
    public void clearRegistries() {
        AbsentIndexDescriptorSuppliers.register(null);
        AbsentIndexDescriptorSuppliers.registerCached(null);
        AbsentIndexDescriptorSuppliers.clearSynthesised();
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.clearMemos();
    }

    // ------------------------------------------------------------------ the metadata half

    public void testResolveMetadataReturnsNullWhenNothingIsRegistered() {
        assertNull(catalog.resolveMetadata(Metadata.builder().build(), "any-index"));
    }

    public void testResolveMetadataDelegatesToWhateverIsRegisteredWithTheStaticRegistry() {
        IndexDescriptor descriptor = descriptor("gated-index");
        AbsentIndexDescriptorSuppliers.register(name -> "gated-index".equals(name) ? descriptor : null);

        IndexMetadata resolved = catalog.resolveMetadata(Metadata.builder().build(), "gated-index");

        assertNotNull(resolved);
        assertEquals("gated-index", resolved.getIndex().getName());
    }

    public void testResolveMetadataReturnsNullForATombstonedDescriptor() {
        IndexDescriptor tombstone = descriptor("deleted-index").tombstoned();
        AbsentIndexDescriptorSuppliers.register(name -> "deleted-index".equals(name) ? tombstone : null);

        assertNull(
            "a tombstoned descriptor must resolve like a genuinely missing index, matching "
                + "AbsentIndexDescriptorSuppliers#synthesisedMetadata's own contract",
            catalog.resolveMetadata(Metadata.builder().build(), "deleted-index")
        );
    }

    public void testResolveMetadataMatchesWhatMetadataOrDescriptorAlreadyReturns() {
        // The whole point of delegating to synthesisedMetadata rather than reimplementing lookup logic:
        // this class's answer must be identical to the pre-existing call sites' answer, not just similar.
        IndexDescriptor descriptor = descriptor("gated-index");
        AbsentIndexDescriptorSuppliers.register(name -> "gated-index".equals(name) ? descriptor : null);
        Metadata metadata = Metadata.builder().build();

        assertSame(
            AbsentIndexDescriptorSuppliers.metadataOrDescriptor(metadata, "gated-index"),
            catalog.resolveMetadata(metadata, "gated-index")
        );
    }

    // ------------------------------------------------------------------ the routing half

    public void testResolveRoutingReturnsNullWhenNothingIsRegistered() {
        IndexMetadata indexMetadata = indexMetadata("real");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        assertNull(catalog.resolveRouting(state, indexMetadata));
    }

    public void testResolveRoutingDelegatesToWhateverIsRegistered() {
        IndexMetadata indexMetadata = indexMetadata("computed");
        IndexRoutingTable computed = IndexRoutingTable.builder(indexMetadata.getIndex()).build();
        AbsentIndexRoutingSuppliers.register((state, meta) -> computed);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        assertSame(computed, catalog.resolveRouting(state, indexMetadata));
    }

    public void testShouldPublishRoutingDefaultsToTrue() {
        assertTrue(catalog.shouldPublishRouting(indexMetadata("real")));
    }

    public void testShouldPublishRoutingDelegatesToWhateverIsRegistered() {
        IndexMetadata computed = indexMetadata("computed");
        AbsentIndexRoutingSuppliers.registerUnpublished(meta -> meta.getIndex().getName().equals("computed"));

        assertFalse(catalog.shouldPublishRouting(computed));
        assertTrue(catalog.shouldPublishRouting(indexMetadata("ordinary")));
    }

    // ------------------------------------------------------------------ the activation question

    /**
     * The reason {@code isActive()} is on the interface at all: this object is installed for the node's
     * lifetime while the registry underneath it fills and empties, so the answer has to be read live.
     */
    public void testIsActiveFollowsThePlacementRegistryLive() {
        assertFalse("nothing registered underneath means the feature is off", catalog.isActive());

        AbsentIndexRoutingSuppliers.register((state, meta) -> null);
        assertTrue("the same catalog instance must now read as active", catalog.isActive());

        AbsentIndexRoutingSuppliers.register(null);
        assertFalse("and must follow the feature back off again", catalog.isActive());
    }

    /**
     * Deliberately the placement registry and not "either registry": all four core callers ask whether an
     * open index can legitimately have no published routing entry, which a descriptor supplier alone does
     * not make true. Widening it would make cat shards request metadata it does not need and suppress an
     * assertion that is still valid.
     */
    public void testIsActiveIgnoresTheDescriptorRegistry() {
        AbsentIndexDescriptorSuppliers.register(name -> descriptor(name));

        assertFalse("a descriptor-only node has no computed placement, so the guard must stay off", catalog.isActive());
    }

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(indexMetadata(name));
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
