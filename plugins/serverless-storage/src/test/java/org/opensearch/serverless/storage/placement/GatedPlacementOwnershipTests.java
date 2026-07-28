/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.placement;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

/**
 * P6. Whether a gated index has anywhere to place its shards.
 *
 * <p>Found while working out a memo key, which is worth recording: the question "what is this method's
 * argument when the index is gated" had a worse answer than the memo needed.
 *
 * <p>{@code AbsentIndexRoutingSuppliers.resolve} calls {@code supply(state, state.metadata().index(name))},
 * and for a gated index that metadata is null, because removing the cluster state entry is what gating
 * means. {@code ComputedPlacementGate.ownsIndex} then answers false for null and the gate returns no table.
 *
 * <p>So after W3 made a gated index nameable and W12 turned gating on, an index can be created, resolved and
 * written to while having no routing at all. Nothing throws. That is the failure this area has produced at
 * every layer: a confident empty answer where an error would have been kinder.
 *
 * <p>Pinned here before being fixed, so the fix has a target that fails today.
 */
public class GatedPlacementOwnershipTests extends OpenSearchTestCase {

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.clearMemos();
        ComputedPlacementGate.uninstall();
    }

    /** The gap. An index whose metadata is gone owns nothing, so it is placed nowhere. */
    public void testAGatedIndexIsNotRecognisedAsOwned() {
        assertFalse(
            "ownsIndex cannot recognise an index whose metadata gating removed, which is every gated "
                + "index. Closing this means deciding ownership from the descriptor when metadata is "
                + "absent, which is what the descriptor seam exists for",
            ComputedPlacementGate.ownsIndex(null)
        );
    }

    /** And the consequence: resolution produces no routing table for it. */
    public void testAGatedIndexResolvesToNoRouting() {
        ComputedPlacementGate.install(true);
        ClusterState gated = ClusterState.builder(ClusterName.DEFAULT).build();

        assertNull(
            "a gated index must not silently have nowhere to place its shards. It is nameable after W3 and "
                + "gated after W12, so this is an index that can be created and addressed and never served",
            AbsentIndexRoutingSuppliers.resolve(gated, "gated-index")
        );
    }

    /** The control: an ordinary serverless index with metadata is still owned and still placed. */
    public void testAnOrdinaryServerlessIndexIsStillOwned() {
        assertTrue("an index that still has metadata must be unaffected", ComputedPlacementGate.ownsIndex(serverlessIndex()));
    }

    private static IndexMetadata serverlessIndex() {
        return IndexMetadata.builder("ordinary")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "ordinary-uuid")
                    .put("index.serverless_storage.enabled", true)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
