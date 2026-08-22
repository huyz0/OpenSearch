/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Spike S7: is it safe to take {@code Metadata.Builder}'s incremental lookup-reuse path on the
 * diff-apply path?
 *
 * <p>Background: {@code Metadata.Builder.build()} can either reuse the previous {@code indicesLookup}
 * and the six derived index-name arrays, or recompute them from scratch (an O(N log N) rebuild
 * allocating a fresh {@code TreeMap} with an entry per index <em>and</em> per alias). It picks by
 * checking {@code previousMetadata}. But {@code MetadataDiff.apply} constructs the builder with the
 * <em>no-arg</em> {@code builder()}, so {@code previousMetadata} is always null and the recompute
 * path is always taken -- on every node, for every metadata-touching cluster state, however small
 * the diff. Making the diff path supply the previous {@code Metadata} therefore looks like a cheap,
 * self-contained win.
 *
 * <p>This spike checks the two things that have to hold before that change is safe:
 * <ol>
 *   <li>the reuse <em>condition</em> is conservative enough -- in particular, that an alias change
 *       (which lives inside {@code IndexMetadata}, not in a separate structure) still forces a
 *       recompute;</li>
 *   <li>the obvious implementation is actually correct -- it is not, and this pins down why.</li>
 * </ol>
 */
public class MetadataLookupReuseSpikeTests extends OpenSearchTestCase {

    /**
     * The trap. {@code Builder(Metadata)} seeds {@code indices} from the previous metadata
     * <em>and</em> sets {@code previousMetadata}; {@code Builder#indices(Map)} then does a
     * {@code putAll}, which cannot express a deletion. So naively swapping {@code builder()} for
     * {@code builder(part)} in {@code MetadataDiff.apply} would leave deleted indices resident --
     * a correctness bug, not a performance regression.
     */
    public void testSeedingBuilderFromPreviousResurrectsDeletedIndices() {
        Metadata previous = Metadata.builder().put(index("keep"), false).put(index("remove-me"), false).build();

        // What the post-diff index map looks like: "remove-me" is gone.
        Metadata correct = Metadata.builder().put(index("keep"), false).build();

        // The naive "just pass the previous metadata" shape.
        Metadata naive = Metadata.builder(previous).indices(correct.indices()).build();

        assertTrue("sanity: the index really was dropped from the intended result", correct.hasIndex("keep"));
        assertFalse("sanity: the index really was dropped from the intended result", correct.hasIndex("remove-me"));

        assertTrue(naive.hasIndex("keep"));
        assertTrue(
            "seeding from previous + putAll cannot express deletion -- this is why the naive C1 fix is unsafe",
            naive.hasIndex("remove-me")
        );
    }

    /**
     * The reuse condition itself is safe for aliases: aliases live inside {@link IndexMetadata},
     * {@link IndexMetadata#equals} compares them, so any alias change makes the index map unequal
     * and forces a recompute. This was the specific risk flagged when C1 was first proposed, and it
     * turns out not to be a risk.
     */
    public void testAliasChangeForcesRecomputeAndCorrectLookup() {
        IndexMetadata before = IndexMetadata.builder("idx")
            .settings(baseSettings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias-old").build())
            .build();
        IndexMetadata after = IndexMetadata.builder("idx")
            .settings(baseSettings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias-new").build())
            .build();

        assertNotEquals("an alias change must make IndexMetadata unequal, or the reuse check cannot see it", before, after);

        Metadata previous = Metadata.builder().put(before, false).build();
        assertTrue(previous.getIndicesLookup().containsKey("alias-old"));

        // Build the successor via the previous-metadata path, the way a fixed diff-apply would.
        Metadata next = Metadata.builder(previous).put(after, false).build();

        assertTrue("the new alias must appear", next.getIndicesLookup().containsKey("alias-new"));
        assertFalse(
            "the old alias must be gone -- a stale reused lookup would still contain it",
            next.getIndicesLookup().containsKey("alias-old")
        );
    }

    /**
     * The diff-apply path must still express deletions. This is the regression guard for the fix:
     * {@code MetadataDiff.apply} now supplies the previous metadata as a reuse reference only, so a
     * removed index must genuinely disappear rather than being carried over from the seed map.
     */
    public void testDiffApplyRemovesDeletedIndices() {
        Metadata before = Metadata.builder().put(index("keep"), false).put(index("remove-me"), false).build();
        Metadata after = Metadata.builder().put(index("keep"), false).build();

        Metadata applied = after.diff(before).apply(before);

        assertTrue(applied.hasIndex("keep"));
        assertFalse("deleted index must not survive the diff-apply path", applied.hasIndex("remove-me"));
        assertFalse(applied.getIndicesLookup().containsKey("remove-me"));
        assertArrayEquals(after.getConcreteAllIndices(), applied.getConcreteAllIndices());
    }

    /** Additions and alias changes must also round-trip correctly through diff-apply. */
    public void testDiffApplyHandlesAdditionsAndAliasChanges() {
        Metadata before = Metadata.builder().put(index("a"), false).build();

        IndexMetadata aliased = IndexMetadata.builder("a")
            .settings(baseSettings())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("a-alias").build())
            .build();
        Metadata after = Metadata.builder().put(aliased, false).put(index("b"), false).build();

        Metadata applied = after.diff(before).apply(before);

        assertTrue(applied.hasIndex("a"));
        assertTrue("added index must appear", applied.hasIndex("b"));
        assertTrue("added alias must appear in the lookup", applied.getIndicesLookup().containsKey("a-alias"));
        assertEquals(after.getIndicesLookup().keySet(), applied.getIndicesLookup().keySet());
    }

    /**
     * The payoff case: when nothing about the index set changed, the applied metadata should reuse
     * the previous lookup instance rather than rebuilding it.
     */
    public void testDiffApplyReusesLookupWhenIndicesUnchanged() {
        Metadata before = Metadata.builder().put(index("a"), false).put(index("b"), false).build();
        Metadata after = Metadata.builder(before).persistentSettings(Settings.builder().put("cluster.foo", "bar").build()).build();

        Metadata applied = after.diff(before).apply(before);

        assertEquals(before.getIndicesLookup().keySet(), applied.getIndicesLookup().keySet());
        assertSame(
            "an unchanged index set must reuse the previous lookup rather than rebuild it",
            before.getIndicesLookup(),
            applied.getIndicesLookup()
        );
    }

    /** With the index set genuinely unchanged, the reused lookup must equal a freshly computed one. */
    public void testUnchangedIndicesProduceEquivalentLookup() {
        Metadata previous = Metadata.builder().put(index("a"), false).put(index("b"), false).build();

        // Same index set, only the cluster-state version moves -- the case the reuse path exists for.
        Metadata reused = Metadata.builder(previous).version(previous.version() + 1).build();
        Metadata recomputed = Metadata.builder().put(index("a"), false).put(index("b"), false).build();

        assertEquals(recomputed.getIndicesLookup().keySet(), reused.getIndicesLookup().keySet());
        assertArrayEquals(recomputed.getConcreteAllIndices(), reused.getConcreteAllIndices());
        assertArrayEquals(recomputed.getConcreteVisibleIndices(), reused.getConcreteVisibleIndices());
        assertArrayEquals(recomputed.getConcreteAllOpenIndices(), reused.getConcreteAllOpenIndices());
    }

    private static IndexMetadata index(String name) {
        return IndexMetadata.builder(name).settings(baseSettings()).numberOfShards(1).numberOfReplicas(0).build();
    }

    private static Settings.Builder baseSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
    }
}
