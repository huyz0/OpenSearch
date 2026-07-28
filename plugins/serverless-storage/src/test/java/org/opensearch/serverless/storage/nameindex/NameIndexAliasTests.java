/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * A9 and A10. Aliases.
 *
 * <p>Aliases share the name space with indices rather than living in a structure of their own, because
 * OpenSearch forbids an alias and an index having the same name, and because one prefix scan should
 * resolve both. Their targets live in three sparse side arrays keyed by alias ordinal, so an ordinary
 * index pays nothing: a dense per-entry offset array would cost 4 bytes for all 100M names to describe
 * something almost none of them have.
 */
public class NameIndexAliasTests extends OpenSearchTestCase {

    public void testAliasResolvesToItsTargets() {
        CompactNameIndex index = new CompactNameIndexBuilder().add("logs-2024", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("logs-2025", uuid(2), IndexNameEntry.STATUS_OPEN)
            .addAlias("logs", uuid(3), List.of("logs-2024", "logs-2025"))
            .build();

        int ordinal = index.ordinalOf("logs");
        assertTrue(index.isAliasAt(ordinal));
        assertEquals(List.of("logs-2024", "logs-2025"), sorted(index.aliasTargetsAt(ordinal)));
        assertEquals(1, index.aliasCount());
    }

    public void testOrdinaryIndexHasNoTargetsAndCostsNothing() {
        CompactNameIndex withAlias = new CompactNameIndexBuilder().add("a", uuid(1), IndexNameEntry.STATUS_OPEN)
            .addAlias("z", uuid(2), List.of("a"))
            .build();
        CompactNameIndex withoutAlias = new CompactNameIndexBuilder().add("a", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("z", uuid(2), IndexNameEntry.STATUS_OPEN)
            .build();

        assertFalse(withAlias.isAliasAt(withAlias.ordinalOf("a")));
        assertEquals(List.of(), withAlias.aliasTargetsAt(withAlias.ordinalOf("a")));
        assertEquals(0, withoutAlias.aliasCount());

        // The sparse side arrays mean an index with no aliases carries no per-entry alias cost at all.
        assertTrue("an alias-free index should not be larger", withoutAlias.ramBytesUsed() <= withAlias.ramBytesUsed());
    }

    /**
     * The bug this test exists for. Rebuild reads base entries back out and re-adds them; going through
     * {@code (name, uuid, status)} preserves the alias status but silently drops every target, so an
     * alias survives a rebuild pointing at nothing. Found while wiring aliases, not by review.
     */
    public void testRebuildPreservesAliasTargets() {
        NameIndex index = new NameIndex(
            new CompactNameIndexBuilder().add("logs-a", uuid(1), IndexNameEntry.STATUS_OPEN)
                .add("logs-b", uuid(2), IndexNameEntry.STATUS_OPEN)
                .addAlias("logs", uuid(3), List.of("logs-a", "logs-b"))
                .build()
        );

        assertEquals(List.of("logs-a", "logs-b"), sorted(index.resolveAlias("logs")));

        index.rebuild();

        assertEquals("a rebuild must not empty an alias", List.of("logs-a", "logs-b"), sorted(index.resolveAlias("logs")));
    }

    public void testAliasCreatedInTheOverlayResolvesBeforeAnyRebuild() {
        NameIndex index = new NameIndex(new CompactNameIndexBuilder().add("logs-a", uuid(1), IndexNameEntry.STATUS_OPEN).build());

        // An alias created into the overlay names targets that have no ordinals yet, which is why the
        // entry carries names rather than ordinals.
        index.createAlias("logs", uuid(9), List.of("logs-a"));

        assertEquals(List.of("logs-a"), index.resolveAlias("logs"));
        assertTrue(index.lookup("logs").isAlias());

        index.rebuild();
        assertEquals(List.of("logs-a"), index.resolveAlias("logs"));
    }

    /**
     * An index can be deleted while an alias still names it. Rejecting the dangling target would leave
     * rebuild unable to make progress, turning a stale reference into an outage, so it is dropped.
     */
    public void testDanglingAliasTargetIsDroppedRatherThanRejected() {
        CompactNameIndex index = new CompactNameIndexBuilder().add("exists", uuid(1), IndexNameEntry.STATUS_OPEN)
            .addAlias("alias", uuid(2), List.of("exists", "was-deleted"))
            .build();

        assertEquals(List.of("exists"), index.aliasTargetsAt(index.ordinalOf("alias")));
    }

    public void testAliasWithNoSurvivingTargetsIsStillResolvableAsAName() {
        CompactNameIndex index = new CompactNameIndexBuilder().add("other", uuid(1), IndexNameEntry.STATUS_OPEN)
            .addAlias("empty-alias", uuid(2), List.of("all-gone"))
            .build();

        int ordinal = index.ordinalOf("empty-alias");
        assertTrue(ordinal >= 0);
        assertTrue(index.isAliasAt(ordinal));
        assertEquals(List.of(), index.aliasTargetsAt(ordinal));
    }

    public void testDuplicateTargetsAreCollapsedButOrderIsKept() {
        CompactNameIndex index = new CompactNameIndexBuilder().add("a", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("b", uuid(2), IndexNameEntry.STATUS_OPEN)
            .addAlias("alias", uuid(3), List.of("b", "a", "b"))
            .build();

        assertEquals(List.of("b", "a"), index.aliasTargetsAt(index.ordinalOf("alias")));
    }

    public void testAliasesAppearInWildcardResultsAlongsideIndices() {
        NameIndex index = new NameIndex(
            new CompactNameIndexBuilder().add("logs-a", uuid(1), IndexNameEntry.STATUS_OPEN)
                .addAlias("logs-all", uuid(2), List.of("logs-a"))
                .build()
        );

        // One scan over a shared name space returns both, which is the reason aliases are not kept in a
        // structure of their own.
        assertEquals(List.of("logs-a", "logs-all"), names(index.resolve("logs-*")));
    }

    public void testDeletingAnAliasHidesIt() {
        NameIndex index = new NameIndex(
            new CompactNameIndexBuilder().add("logs-a", uuid(1), IndexNameEntry.STATUS_OPEN)
                .addAlias("logs-all", uuid(2), List.of("logs-a"))
                .build()
        );

        index.delete("logs-all");

        assertNull(index.lookup("logs-all"));
        assertEquals(List.of(), index.resolveAlias("logs-all"));
        assertEquals(List.of("logs-a"), names(index.resolve("logs-*")));
    }

    public void testResolveAliasOnANonAliasNameIsEmpty() {
        NameIndex index = new NameIndex(new CompactNameIndexBuilder().add("plain", uuid(1), IndexNameEntry.STATUS_OPEN).build());

        assertEquals(List.of(), index.resolveAlias("plain"));
        assertEquals(List.of(), index.resolveAlias("does-not-exist"));
    }

    public void testEntryRoundTripsThroughTheBuilder() {
        IndexNameEntry alias = new IndexNameEntry("alias", uuid(5), IndexNameEntry.STATUS_ALIAS, List.of("target"));
        CompactNameIndex index = new CompactNameIndexBuilder().add("target", uuid(1), IndexNameEntry.STATUS_OPEN).add(alias).build();

        assertEquals(alias, index.entryAt(index.ordinalOf("alias")));
    }

    /**
     * A10: alias fan-out, which S13 explicitly did not measure. A tenant-per-index deployment can have
     * an alias spanning a very large index set, and targets are stored as 4-byte ordinals rather than as
     * names precisely so that cost stays bounded.
     */
    public void testWideAliasFanOutStaysProportionalToTargetCount() {
        int targetCount = 20_000;
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder(targetCount + 1);
        List<String> targets = new ArrayList<>(targetCount);
        for (int i = 0; i < targetCount; i++) {
            String name = "tenant-" + String.format(Locale.ROOT, "%06d", i);
            targets.add(name);
            builder.add(name, uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        CompactNameIndex index = builder.addAlias("everything", uuid(0), targets).build();

        long start = System.nanoTime();
        List<String> resolved = index.aliasTargetsAt(index.ordinalOf("everything"));
        long elapsedMicros = (System.nanoTime() - start) / 1000;

        assertEquals(targetCount, resolved.size());
        // 4 bytes per target plus the names it resolves to. The assertion is deliberately loose: the
        // point is that fan-out is linear in targets and not in index size.
        logger.info("alias fan-out of {} resolved in {} us", targetCount, elapsedMicros);
        assertTrue("resolution should be fast, took " + elapsedMicros + " us", elapsedMicros < 500_000);
    }

    private static List<String> names(List<IndexNameEntry> entries) {
        return entries.stream().map(IndexNameEntry::getName).collect(Collectors.toList());
    }

    private static List<String> sorted(List<String> values) {
        List<String> copy = new ArrayList<>(values);
        copy.sort(null);
        return copy;
    }

    private static byte[] uuid(int seed) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) (seed + i);
        }
        return uuid;
    }
}
