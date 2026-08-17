/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Phase C2 of core-pluggability-refactor-plan.md.
 *
 * <p>{@link Metadata#index(String)} consults an attached {@link IndexMetadataResolver} on a miss --
 * this covers the resolve-on-miss behavior itself, and (the part that's easy to get wrong) that the
 * resolver survives every mechanism a new {@link Metadata} instance gets produced from an existing one
 * on the same node: {@link Metadata.Builder#Builder(Metadata)} and {@link Metadata#diff(Metadata)}'s
 * {@link Diff#apply}. Neither of those two paths carries the field for free -- see the resolver field's
 * own javadoc on {@code Metadata} for why each needed an explicit line.
 */
public class IndexMetadataResolverPropagationTests extends OpenSearchTestCase {

    private static final IndexMetadataResolver ALWAYS_SYNTHESIZE = (metadata, name) -> index(name);

    public void testResolverIsConsultedOnlyOnAMiss() {
        Metadata metadata = Metadata.builder().put(index("real"), false).build();
        metadata.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        assertNotNull("a real entry must resolve without ever reaching the resolver", metadata.index("real"));
        assertNotNull("a miss must fall through to the resolver", metadata.index("synthesized"));
        assertEquals("synthesized", metadata.index("synthesized").getIndex().getName());
    }

    public void testNoResolverAttachedMeansUnchangedBehavior() {
        Metadata metadata = Metadata.builder().put(index("real"), false).build();

        assertNull("with nothing attached, a miss must still just be null, exactly as before this phase", metadata.index("missing"));
    }

    public void testResolverPropagatesThroughBuilderMutation() {
        Metadata before = Metadata.builder().put(index("real"), false).build();
        before.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        // Metadata.builder(before) goes through Builder(Metadata), which must copy the resolver forward.
        Metadata after = Metadata.builder(before).put(IndexMetadata.builder(before.index("real")).numberOfReplicas(2)).build();

        assertSame(ALWAYS_SYNTHESIZE, after.indexMetadataResolver());
        assertNotNull("the mutated copy must still resolve a miss", after.index("still-a-miss"));
    }

    public void testResolverPropagatesThroughDiffApply() {
        Metadata before = Metadata.builder().put(index("real"), false).build();
        before.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        Metadata after = Metadata.builder(before).put(IndexMetadata.builder(before.index("real")).numberOfReplicas(2)).build();

        // Simulates the normal cluster-state-propagation path: a node applies a diff against ITS OWN
        // previous Metadata (which already carries the resolver), not against a freshly Builder-copied one.
        Diff<Metadata> diff = after.diff(before);
        Metadata applied = diff.apply(before);

        assertSame(
            "a diff-applied Metadata must inherit the resolver from the pre-diff state on this node, "
                + "not lose it -- this is the normal way cluster state propagates after the first full state",
            ALWAYS_SYNTHESIZE,
            applied.indexMetadataResolver()
        );
        assertNotNull(applied.index("still-a-miss-after-diff"));
    }

    public void testResolverIsNotConsultedOnAnUnsafeThread() throws InterruptedException {
        // See ClusterStateMutationThreadsTests for the thread-name matching itself; this is the
        // consumer-side guarantee IndexMetadataResolver's own javadoc documents: Metadata#index(String)
        // must never call resolve() from one of these threads, since a resolver may do real work to answer.
        Metadata metadata = Metadata.builder().put(index("real"), false).build();
        metadata.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        java.util.concurrent.atomic.AtomicReference<IndexMetadata> result = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        Thread clusterApplierThread = new Thread(
            () -> {
                try {
                    result.set(metadata.index("synthesized"));
                } catch (Throwable t) {
                    failure.set(t);
                }
            },
            "opensearch[nodeA][clusterApplierService#updateTask][T#1]"
        );
        clusterApplierThread.start();
        clusterApplierThread.join();

        assertNull("a resolver must never be consulted from the cluster applier's own update thread", result.get());
        assertNull(failure.get());
    }

    private static IndexMetadata index(String name) {
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
