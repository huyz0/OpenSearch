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
 * <p><b>Closed by P7.</b> The name is what makes the descriptor reachable, so the two resolution paths that
 * lose it now pass it, and metadata is synthesised from the descriptor when cluster state has none. The
 * routing seam and every supplier behind it are unchanged, which matters because C3 and every routing caller
 * depend on that signature.
 */
public class GatedPlacementOwnershipTests extends OpenSearchTestCase {

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.clearMemos();
        org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.register(null);
        ComputedPlacementGate.uninstall();
    }

    /**
     * ownsIndex still cannot judge a null, which is why the fix supplies it something to judge rather than
     * teaching it to guess.
     */
    public void testOwnsIndexStillCannotJudgeAbsentMetadata() {
        assertFalse("a null carries nothing to decide ownership from", ComputedPlacementGate.ownsIndex(null));
    }

    /** The property P6 pinned as broken and P7 closes: a gated index is placed. */
    public void testAGatedIndexIsPlacedFromItsDescriptor() {
        org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.register(
            name -> "serverless_gated-index".equals(name)
                ? new org.opensearch.cluster.metadata.IndexDescriptor(
                    name,
                    name + "-uuid",
                    3,
                    0,
                    true,
                    org.opensearch.cluster.metadata.IndexDescriptor.State.OPEN,
                    java.util.List.of(),
                    Version.CURRENT.id,
                    false,
                    false,
                    false,
                    false,
                    0L,
                    1_700_000_000_000L
                )
                : null
        );
        ComputedPlacementGate.install(true);
        ClusterState gated = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                org.opensearch.cluster.node.DiscoveryNodes.builder()
                    .add(
                        new org.opensearch.cluster.node.DiscoveryNode(
                            "node-1",
                            buildNewFakeTransportAddress(),
                            java.util.Map.of(),
                            java.util.Set.of(org.opensearch.cluster.node.DiscoveryNodeRole.DATA_ROLE),
                            Version.CURRENT
                        )
                    )
                    .localNodeId("node-1")
                    .build()
            )
            .build();

        org.opensearch.cluster.routing.IndexRoutingTable placed = AbsentIndexRoutingSuppliers.resolve(gated, "serverless_gated-index");

        assertNotNull(
            "a gated index must have somewhere to place its shards. Before P7 this was null: nameable "
                + "after W3, gated after W12, and served by nothing",
            placed
        );
        assertEquals("with the shard count its descriptor declares", 3, placed.shards().size());
    }

    /** A name with no descriptor is still unplaced, or the fallback would invent routing for typos. */
    public void testAnUnknownNameIsStillUnplaced() {
        org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers.register(name -> null);
        ComputedPlacementGate.install(true);

        assertNull(
            "a name no descriptor answers for must stay unplaced",
            AbsentIndexRoutingSuppliers.resolve(ClusterState.builder(ClusterName.DEFAULT).build(), "never-existed")
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
