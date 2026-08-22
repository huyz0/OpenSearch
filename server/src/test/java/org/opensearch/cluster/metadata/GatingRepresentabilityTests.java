/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * T7. What gating an index silently drops.
 *
 * <p>TiDB names every reason a table cannot be lazily loaded in one array, {@code checkAttributesInOrder}
 * ({@code meta/meta.go:1286}): partitioning, table lock, TiFlash replica, temp type, placement policy
 * reference, TTL, affinity, and any table with foreign keys. Adding a feature that needs residency forces an
 * edit to that list, so the set of things the lazy path cannot represent is written down in one place.
 *
 * <p><b>We have no such list.</b> {@code ComputedPlacementGate.ownsIndex} answers on one setting,
 * {@code index.serverless_storage.enabled}, and a gated index has no cluster state entry at all. So whatever
 * {@link IndexDescriptor} cannot carry is not degraded, it is gone, and nothing anywhere checks whether the
 * index being gated had any of it.
 *
 * <p>These tests exist to state the boundary rather than to argue it. They are written against
 * {@link IndexDescriptor#from} because that is the one place full metadata becomes a descriptor, so it is
 * where the loss is observable.
 */
public class GatingRepresentabilityTests extends OpenSearchTestCase {

    /**
     * The one that matters most: an alias filter restricts which documents a query through that alias can
     * see, and a descriptor keeps only the alias name.
     *
     * <p>So gating an index with a filtered alias does not fail, it widens the alias to every document in
     * the index. That is this area's signature failure aimed at something worse than latency.
     */
    public void testAnAliasFilterDoesNotSurviveGating() throws Exception {
        AliasMetadata filtered = AliasMetadata.builder("visible-subset")
            .filter(new CompressedXContent("{\"term\":{\"tenant\":\"acme\"}}"))
            .build();
        IndexMetadata metadata = serverlessIndex().putAlias(filtered).build();

        assertNotNull("the alias carries a filter before gating", metadata.getAliases().get("visible-subset").filter());

        IndexDescriptor descriptor = IndexDescriptor.from(metadata);

        assertTrue("the alias name survives", descriptor.aliases().contains("visible-subset"));
        assertEquals(
            "a descriptor carries alias names and nothing else, so there is nowhere for a filter to go. "
                + "Gating an index with a filtered alias therefore turns a restricted view into an "
                + "unrestricted one, silently",
            java.util.List.of("visible-subset"),
            descriptor.aliases()
        );
    }

    /** Routing on an alias decides which shards a request touches, and is dropped the same way. */
    public void testAliasRoutingDoesNotSurviveGating() {
        IndexMetadata metadata = serverlessIndex().putAlias(
            AliasMetadata.builder("routed").indexRouting("shard-key").searchRouting("shard-key").build()
        ).build();

        assertEquals("routing is set before gating", "shard-key", metadata.getAliases().get("routed").indexRouting());
        assertTrue("and the name is all that survives", IndexDescriptor.from(metadata).aliases().contains("routed"));
    }

    /**
     * A write index among several aliases decides where writes land, and a list of names cannot say which
     * one it was.
     */
    public void testTheWriteIndexFlagDoesNotSurviveGating() {
        IndexMetadata metadata = serverlessIndex().putAlias(AliasMetadata.builder("rollover-target").writeIndex(true).build()).build();

        assertEquals(Boolean.TRUE, metadata.getAliases().get("rollover-target").writeIndex());
        assertEquals(java.util.List.of("rollover-target"), IndexDescriptor.from(metadata).aliases());
    }

    /**
     * A plain alias survives the conversion intact, which is still true and no longer the whole story.
     *
     * <p>This was written as the reason T7 is a list rather than a blanket rule: the plain case loses
     * nothing, so refusing every aliased index looked like the wrong fix. T29 refused them anyway, on a
     * ground this test cannot see. Representability is about what a descriptor can carry, and the alias name
     * is carried; the problem is that nothing can change it afterwards, because every alias operation is a
     * cluster state update over metadata a gated index does not have.
     *
     * <p>Kept as it is, because the distinction is worth preserving. If alias names ever become a second key
     * space in the descriptor store with their own write path, this is the assertion that says the data was
     * never the obstacle.
     */
    public void testAPlainAliasIsFullyRepresented() {
        IndexMetadata metadata = serverlessIndex().putAlias(AliasMetadata.builder("plain").build()).build();

        assertEquals(java.util.List.of("plain"), IndexDescriptor.from(metadata).aliases());
    }

    /**
     * The list itself, which is the T7 deliverable rather than the individual losses above.
     *
     * <p>Each reason names the feature, because an operator who asked for a gated index and got a cluster
     * state entry has to be able to find out which one kept it there.
     */
    public void testEveryLossIsAStatedReasonNotToGate() throws Exception {
        assertNull(
            "the plain case must stay gatable, or this would gate nothing",
            DescriptorRepresentable.whyNotRepresentable(serverlessIndex().build())
        );

        // T29 widened four alias rules into one, so every alias is now refused and each of these reports
        // the same reason. They are kept as separate cases rather than collapsed into one because they are
        // the four ways an alias can carry meaning a descriptor drops, and a future change that re-admits
        // plain aliases has to decide about each of them again rather than about "aliases" in general.
        assertReason("alias", serverlessIndex().putAlias(AliasMetadata.builder("plain").build()).build());
        assertReason(
            "alias",
            serverlessIndex().putAlias(
                AliasMetadata.builder("filtered").filter(new CompressedXContent("{\"term\":{\"tenant\":\"acme\"}}")).build()
            ).build()
        );
        assertReason("alias", serverlessIndex().putAlias(AliasMetadata.builder("routed").indexRouting("k").build()).build());
        assertReason("alias", serverlessIndex().putAlias(AliasMetadata.builder("routed").searchRouting("k").build()).build());
        assertReason("alias", serverlessIndex().putAlias(AliasMetadata.builder("w").writeIndex(true).build()).build());
        assertReason("alias", serverlessIndex().putAlias(AliasMetadata.builder("h").isHidden(true).build()).build());
    }

    /**
     * Why a plain alias is refused, which is not the reason the other four are.
     *
     * <p>The other four are refused because a descriptor drops something: a filter, routing, a write index
     * flag, a hidden flag. A plain alias loses nothing on the way into a descriptor, since the name is
     * carried. It is refused because nothing can ever change it afterwards.
     *
     * <p>Every alias operation is a cluster state update that rewrites the index's {@code IndexMetadata},
     * and a gated index has none, so T29 measured an alias add against one timing out with
     * {@code ClusterManagerNotDiscoveredException} rather than failing cleanly. An alias that can be set at
     * creation and never repointed is the opposite of what an alias is for.
     */
    public void testAPlainAliasIsRefusedForMutabilityRatherThanForLoss() throws Exception {
        IndexMetadata plainAlias = serverlessIndex().putAlias(AliasMetadata.builder("stable-name").build()).build();

        assertTrue(
            "the name itself survives into a descriptor, so this is not a loss of information",
            IndexDescriptor.from(plainAlias).aliases().contains("stable-name")
        );
        assertNotNull("and it is refused anyway", DescriptorRepresentable.whyNotRepresentable(plainAlias));
        assertTrue(
            "the reason must be about the alias never changing rather than about a dropped field",
            DescriptorRepresentable.whyNotRepresentable(plainAlias).contains("changed")
        );
    }

    /** An index with no metadata at all is not representable either, rather than being quietly accepted. */
    public void testNullMetadataIsNotRepresentable() {
        assertFalse(DescriptorRepresentable.isRepresentable(null));
        assertNotNull(DescriptorRepresentable.whyNotRepresentable(null));
    }

    private static void assertReason(String expectedFeature, IndexMetadata metadata) {
        String reason = DescriptorRepresentable.whyNotRepresentable(metadata);
        assertNotNull("gating must be refused for an index whose " + expectedFeature + " a descriptor cannot carry", reason);
        assertTrue(
            "the reason must name the feature so an operator can act on it, but was [" + reason + "]",
            reason.contains(expectedFeature)
        );
    }

    private static IndexMetadata.Builder serverlessIndex() {
        return IndexMetadata.builder("gated")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "gated-uuid")
                    .put("index.serverless_storage.enabled", true)
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0);
    }
}
