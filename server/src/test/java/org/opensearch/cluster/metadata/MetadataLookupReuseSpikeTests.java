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
