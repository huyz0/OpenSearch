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
 * Phase C5 of core-pluggability-refactor-plan.md. See {@code SupplierBackedIndexMetadataResolverTests}'
 * own javadoc for why every test here registers and unregisters within itself rather than trusting
 * {@code @After} alone -- {@link AbsentIndexRoutingSuppliers} is a static, process-wide registry.
 */
public class SupplierBackedIndexRoutingResolverTests extends OpenSearchTestCase {

    private final SupplierBackedIndexRoutingResolver resolver = new SupplierBackedIndexRoutingResolver();

    @After
    public void clearRegistry() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
        AbsentIndexRoutingSuppliers.clearMemos();
    }

    public void testResolveReturnsNullWhenNothingIsRegistered() {
        IndexMetadata indexMetadata = indexMetadata("real");
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        assertNull(resolver.resolve(state, indexMetadata));
    }

    public void testResolveDelegatesToWhateverIsRegistered() {
        IndexMetadata indexMetadata = indexMetadata("computed");
        IndexRoutingTable computed = IndexRoutingTable.builder(indexMetadata.getIndex()).build();
        AbsentIndexRoutingSuppliers.register((state, meta) -> computed);
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).build();

        assertSame(computed, resolver.resolve(state, indexMetadata));
    }

    public void testShouldPublishRoutingDefaultsToTrue() {
        assertTrue(resolver.shouldPublishRouting(indexMetadata("real")));
    }

    public void testShouldPublishRoutingDelegatesToWhateverIsRegistered() {
        IndexMetadata computed = indexMetadata("computed");
        AbsentIndexRoutingSuppliers.registerUnpublished(meta -> meta.getIndex().getName().equals("computed"));

        assertFalse(resolver.shouldPublishRouting(computed));
        assertTrue(resolver.shouldPublishRouting(indexMetadata("ordinary")));
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
