/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * D2's final slice of {@code core-pluggability-refactor-plan.md}: {@code IndexCreationStrategy}'s two
 * methods ({@code claims(String)} and {@code skipsClusterState(IndexMetadata)}) and {@link
 * SupplierBackedIndexCreationStrategy}'s delegation to {@link DescriptorOnlyCreation} for both, mirroring
 * core's own {@code SupplierBackedIndexCatalogTests} shape for the analogous C5 adapter.
 *
 * <p>Phase D3: relocated from {@code server/src/test} into this plugin alongside both classes it tests --
 * see {@link DescriptorOnlyCreation}'s own javadoc for the full move.
 */
public class SupplierBackedIndexCreationStrategyTests extends OpenSearchTestCase {

    private final SupplierBackedIndexCreationStrategy strategy = new SupplierBackedIndexCreationStrategy();

    @After
    public void clearRegistry() {
        DescriptorOnlyCreation.register(null);
    }

    public void testClaimsAnswersFalseWhenNothingIsRegistered() {
        assertFalse(strategy.claims("serverless_tenant-1"));
        assertFalse(strategy.claims("serverless_tenant-1", null));
    }

    public void testClaimsIsExactlyTheNamePrefixCheckOnceRegistered() {
        DescriptorOnlyCreation.register(indexMetadata -> true);

        assertTrue("a namespaced name is claimed once something is registered", strategy.claims("serverless_tenant-1"));
        assertFalse("an ordinary name is never claimed regardless of what's registered", strategy.claims("tenant-1"));
    }

    public void testTwoArgOverloadDefaultsToTheNameOnlyAnswer() {
        DescriptorOnlyCreation.register(indexMetadata -> true);

        // The request itself is never consulted -- this is the whole point of D2's final-slice finding that
        // every real implementation of this interface answers from the name alone.
        assertEquals(strategy.claims("serverless_tenant-1"), strategy.claims("serverless_tenant-1", null));
    }

    public void testSkipsClusterStateDelegatesToTheRegisteredGate() {
        assertFalse("nothing registered", strategy.skipsClusterState(anIndex("serverless_tenant-1")));

        DescriptorOnlyCreation.register(indexMetadata -> "serverless_tenant-1".equals(indexMetadata.getIndex().getName()));

        assertTrue(strategy.skipsClusterState(anIndex("serverless_tenant-1")));
        assertFalse(
            "a claimed name can still keep its cluster state entry -- claims() and skipsClusterState() are different questions",
            strategy.skipsClusterState(anIndex("serverless_tenant-2"))
        );
    }

    public void testDescribeClaimedNamespaceNamesTheRealPrefix() {
        assertTrue(strategy.describeClaimedNamespace().contains(DescriptorOnlyCreation.SERVERLESS_NAME_PREFIX));
    }

    private static IndexMetadata anIndex(String name) {
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
