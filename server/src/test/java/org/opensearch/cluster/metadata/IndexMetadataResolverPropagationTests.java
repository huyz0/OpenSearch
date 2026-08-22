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
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Phase C2 of core-pluggability-refactor-plan.md, corrected mid-session after a real regression this design
 * caused: {@link Metadata#index(String)} itself no longer consults a resolver -- see {@link
 * Metadata#indexOrResolved(String)}'s own javadoc, and the plan's C5 status-log entry, for the full story.
 *
 * <p>Covers: {@code index(String)} never resolving (its pre-existing, unconditional contract), {@code
 * indexOrResolved(String)}/{@code indexOrResolved(Index)} resolving on a miss, and (the part that's easy to
 * get wrong) that the resolver survives every mechanism a new {@link Metadata} instance gets produced from
 * an existing one on the same node: {@link Metadata.Builder#Builder(Metadata)} and {@link
 * Metadata#diff(Metadata)}'s {@link Diff#apply}. Neither of those two paths carries the field for free --
 * see the resolver field's own javadoc on {@code Metadata} for why each needed an explicit line.
 */
public class IndexMetadataResolverPropagationTests extends OpenSearchTestCase {

    private static final IndexMetadataResolver ALWAYS_SYNTHESIZE = (metadata, name) -> index(name);

    public void testPlainIndexNeverConsultsTheResolverEvenWhenOneIsAttached() {
        // The whole point of the mid-session correction: index(String) must behave identically whether or
        // not a resolver is attached, so every one of its many existing callers -- most never audited for
        // reliance on its null-ness as a signal -- is completely unaffected by this feature existing.
        Metadata metadata = Metadata.builder().put(index("real"), false).build();
        metadata.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        assertNotNull("a real entry must still resolve", metadata.index("real"));
        assertNull("a miss must stay a miss -- index(String) is not the resolver seam", metadata.index("synthesized"));
    }

    public void testIndexOrResolvedIsConsultedOnlyOnAMiss() {
        Metadata metadata = Metadata.builder().put(index("real"), false).build();
        metadata.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        assertNotNull("a real entry must resolve without ever reaching the resolver", metadata.indexOrResolved("real"));
        assertNotNull("a miss must fall through to the resolver", metadata.indexOrResolved("synthesized"));
        assertEquals("synthesized", metadata.indexOrResolved("synthesized").getIndex().getName());
    }

    public void testIndexOrResolvedByIndexChecksTheUuid() {
        Metadata metadata = Metadata.builder().build();
        metadata.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);
        IndexMetadata synthesized = index("gated");

        assertNotNull(metadata.indexOrResolved(synthesized.getIndex()));
        assertNull(
            "a stale uuid for the same name must not resolve -- that would serve a deleted index's successor",
            metadata.indexOrResolved(new Index("gated", "some-other-uuid"))
        );
    }

    public void testNoResolverAttachedMeansUnchangedBehavior() {
        Metadata metadata = Metadata.builder().put(index("real"), false).build();

        assertNull(
            "with nothing attached, a miss must still just be null, exactly as before this phase",
            metadata.indexOrResolved("missing")
        );
    }

    public void testResolverPropagatesThroughBuilderMutation() {
        Metadata before = Metadata.builder().put(index("real"), false).build();
        before.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        // Metadata.builder(before) goes through Builder(Metadata), which must copy the resolver forward.
        Metadata after = Metadata.builder(before).put(IndexMetadata.builder(before.index("real")).numberOfReplicas(2)).build();

        assertSame(ALWAYS_SYNTHESIZE, after.indexMetadataResolver());
        assertNotNull("the mutated copy must still resolve a miss", after.indexOrResolved("still-a-miss"));
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
        assertNotNull(applied.indexOrResolved("still-a-miss-after-diff"));
    }

    public void testResolverIsNotConsultedOnAnUnsafeThread() throws InterruptedException {
        // See ClusterStateMutationThreadsTests for the thread-name matching itself; this is the
        // consumer-side guarantee IndexMetadataResolver's own javadoc documents: indexOrResolved(String)
        // must never call resolve() from one of these threads, since a resolver may do real work to answer.
        Metadata metadata = Metadata.builder().put(index("real"), false).build();
        metadata.attachIndexMetadataResolver(ALWAYS_SYNTHESIZE);

        java.util.concurrent.atomic.AtomicReference<IndexMetadata> result = new java.util.concurrent.atomic.AtomicReference<>();
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        Thread clusterApplierThread = new Thread(() -> {
            try {
                result.set(metadata.indexOrResolved("synthesized"));
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "opensearch[nodeA][clusterApplierService#updateTask][T#1]");
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
