/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.DescriptorOnlyCreation;
import org.opensearch.cluster.metadata.DescriptorPrefetch;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.index.mapper.UnknownFieldRefresh;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import static org.mockito.Mockito.mock;

/**
 * Every seam this gate installs must actually be installed by it, and every one must come back out.
 *
 * <h2>Why this exists as a test rather than as care</h2>
 *
 * This area's own record names the failure it guards: <em>correct and unreachable</em>, a mechanism that is
 * right and that nothing calls, produced seven times before this branch and six more during it. Counting
 * them needed a grep, and a grep is not run by CI.
 *
 * <p>{@code DescriptorPrefetch} is the case that motivated it. The seam landed in core and its hook landed
 * in {@code TransportBulkAction}, both correct, and nothing registered a prefetcher. The hook therefore
 * found nothing installed and returned immediately on every request in every cluster. Nothing failed,
 * nothing logged, and a no-op prefetch is indistinguishable from a fast one, so only counting callers
 * found it.
 *
 * <h2>Why it asserts the uninstall half too</h2>
 *
 * A seam left registered after {@code uninstall} leaks a supplier into whatever runs next, which in a test
 * suite is an unrelated test and in production is a plugin that was meant to be disabled. The registries
 * are static, so forgetting one is not visible at the call site of either method.
 */
public class DescriptorGateReachabilityTests extends OpenSearchTestCase {

    @After
    public void alwaysUninstall() {
        DescriptorGate.uninstall();
    }

    private void install() {
        DescriptorGate.install(
            new DescriptorStore(mock(org.opensearch.transport.client.Client.class), 1),
            mock(MappingGenerationStore.Store.class),
            mock(org.opensearch.action.admin.cluster.stats.GatedMappingStatsAggregator.Aggregator.class),
            mock(UnknownFieldRefresh.Refresher.class),
            true
        );
    }

    /**
     * The list is spelled out rather than derived, so adding a seam and forgetting to register it fails
     * here only if somebody also adds it here. That is a weaker guarantee than reflection would give and a
     * much clearer failure message, and the thing being defended against is forgetting the registration,
     * which this catches the moment the seam is added to either list.
     */
    public void testInstallRegistersEverySeamItOwns() {
        assertFalse("nothing may be registered before install", AbsentIndexDescriptorSuppliers.isRegistered());
        assertFalse(DescriptorPrefetch.isRegistered());

        install();

        assertTrue("the descriptor supplier", AbsentIndexDescriptorSuppliers.isRegistered());
        assertTrue("the pager", AbsentIndexDescriptorSuppliers.isPagerRegistered());
        assertTrue("the wildcard expander", AbsentIndexDescriptorSuppliers.isExpanderRegistered());
        assertTrue(
            "the prefetcher, which had no registrar at all until the wiring pass and so did nothing",
            DescriptorPrefetch.isRegistered()
        );
        assertTrue("the creation gate", DescriptorOnlyCreation.isRegistered());
        assertTrue("the mapping store", MappingGenerationStore.isRegistered());
        assertTrue("the unknown field refresher", UnknownFieldRefresh.isRegistered());
    }

    public void testUninstallClearsEverySeamItOwns() {
        install();
        DescriptorGate.uninstall();

        assertFalse(AbsentIndexDescriptorSuppliers.isRegistered());
        assertFalse(AbsentIndexDescriptorSuppliers.isPagerRegistered());
        assertFalse(AbsentIndexDescriptorSuppliers.isExpanderRegistered());
        assertFalse("a prefetcher left behind would outlive the plugin that installed it", DescriptorPrefetch.isRegistered());
        assertFalse(DescriptorOnlyCreation.isRegistered());
        assertFalse(MappingGenerationStore.isRegistered());
        assertFalse(UnknownFieldRefresh.isRegistered());
    }

    /** Installing twice must be idempotent, since a node that reloads the plugin does exactly that. */
    public void testInstallIsIdempotent() {
        install();
        install();

        assertTrue(AbsentIndexDescriptorSuppliers.isRegistered());
        assertTrue(DescriptorPrefetch.isRegistered());
    }
}
