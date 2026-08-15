/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.DescriptorOnlyCreation;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.junit.After;

/**
 * W12. The switch that decides whether any index is gated at all.
 *
 * <p>Until this was registered, {@code DescriptorOnlyCreation.skipsClusterState} answered false for every
 * index, so nothing was ever gated and every seam W1 through W10 installed served a population of zero.
 * H3 and H5 built creation without a cluster state update and it had never once run on a real node.
 *
 * <p>The ordering was deliberate rather than an oversight: installing this before the seams existed would
 * have created indices that nothing could resolve, name, map or place. It is the last thing to turn on and
 * the first thing to turn off.
 *
 * <p><b>What is asserted is the agreement between gating and placement.</b> An index is gated exactly when
 * its placement is computed, and that is a correctness requirement rather than a convention: a gated index
 * has no cluster state entry, so it can have no published routing table, so its placement must be derived.
 * An index gated without computed placement would have metadata nothing could place; one with computed
 * placement but not gated would have published routing and duplicated metadata.
 */
public class GatedCreationSwitchIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    private void install(boolean enabled) throws Exception {
        installBlobBackedDescriptorPlane(enabled);
    }

    /** With nothing installed, no index is gated, which is what production looked like until now. */
    public void testWithoutTheGateNothingIsEverGated() throws Exception {
        assertFalse(
            "an unregistered gate must answer false, so every index keeps costing what it always cost",
            DescriptorOnlyCreation.skipsClusterState(serverlessIndex("anything"))
        );
    }

    /** Disabled installs nothing, so an ordinary cluster is untouched by any of this. */
    public void testInstallingWhileDisabledLeavesNothingGated() throws Exception {
        install(false);

        assertFalse("a disabled gate must gate nothing", DescriptorOnlyCreation.skipsClusterState(serverlessIndex("anything")));
    }

    /**
     * An ordinary index is never gated, whatever the setting. This is the control that keeps the switch
     * from turning a normal cluster's indices into descriptors nothing was built to serve.
     */
    public void testAnOrdinaryIndexIsNeverGated() throws Exception {
        install(true);

        assertFalse(
            "an index without serverless storage must keep its cluster state entry",
            DescriptorOnlyCreation.skipsClusterState(ordinaryIndex("normal"))
        );
    }

    /**
     * Gating and computed placement must give the same answer for the same index, since a gated index has
     * no cluster state entry and therefore can have no published routing.
     */
    public void testGatingAgreesWithComputedPlacement() throws Exception {
        install(true);
        IndexMetadata serverless = serverlessIndex("serverless_gated-candidate");
        IndexMetadata ordinary = ordinaryIndex("plain-candidate");

        assertEquals(
            "an index must be gated exactly when its placement is computed, or it ends up with metadata "
                + "nothing can place or with published routing and duplicated metadata",
            org.opensearch.serverless.storage.placement.ComputedPlacementGate.ownsIndex(serverless),
            DescriptorOnlyCreation.skipsClusterState(serverless)
        );
        assertEquals(
            org.opensearch.serverless.storage.placement.ComputedPlacementGate.ownsIndex(ordinary),
            DescriptorOnlyCreation.skipsClusterState(ordinary)
        );
    }

    /** Uninstalling stops new gating immediately, which node close depends on. */
    public void testUninstallStopsGating() throws Exception {
        install(true);
        DescriptorGate.uninstall();

        assertFalse("a closed node must not keep gating", DescriptorOnlyCreation.skipsClusterState(serverlessIndex("anything")));
    }

    private static IndexMetadata serverlessIndex(String name) throws Exception {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .put("index.serverless_storage.enabled", true)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static IndexMetadata ordinaryIndex(String name) throws Exception {
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
