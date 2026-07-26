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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A5 to A8. Glob matching, the mutable overlay, the composite view, and rebuild.
 *
 * <p>The read structure was the easy half and S13 already proved it. This covers the half that was not
 * measured: that a create is visible immediately, that a delete of a name held in the immutable base
 * actually hides it, that the merge of base and overlay emits each name once and in order, and that a
 * rebuild changes nothing observable.
 */
public class NameIndexTests extends OpenSearchTestCase {

    // ------------------------------------------------------------- patterns

    public void testGlobMatching() {
        assertTrue(NamePatterns.matches("logs-*", "logs-2024"));
        assertTrue(NamePatterns.matches("logs-*", "logs-"));
        assertFalse(NamePatterns.matches("logs-*", "logs"));
        assertTrue(NamePatterns.matches("*", "anything"));
        assertTrue(NamePatterns.matches("*", ""));
        assertTrue(NamePatterns.matches("logs-*-2024", "logs-app-2024"));
        assertFalse(NamePatterns.matches("logs-*-2024", "logs-app-2025"));
        assertTrue(NamePatterns.matches("a?c", "abc"));
        assertFalse(NamePatterns.matches("a?c", "ac"));
        assertTrue(NamePatterns.matches("exact", "exact"));
        assertFalse(NamePatterns.matches("exact", "exactly"));
        assertTrue(NamePatterns.matches("**", "anything"));
        assertTrue(NamePatterns.matches("*logs*", "my-logs-here"));
    }

    /**
     * A regex metacharacter in a name is legal and must be treated as an ordinary character. Treating
     * patterns as regexes would silently change what a user's pattern selects.
     */
    public void testRegexMetacharactersAreLiteral() {
        assertTrue(NamePatterns.matches("a.c", "a.c"));
        assertFalse(NamePatterns.matches("a.c", "abc"));
        assertTrue(NamePatterns.matches("a+b", "a+b"));
        assertTrue(NamePatterns.matches("x[0]", "x[0]"));
    }

    /**
     * The backtracking form exists so an adversarial pattern cannot blow up. A recursive matcher goes
     * exponential on this shape; if it ever regresses, this test hangs rather than fails, which is why
     * the pattern is kept short enough to stay fast and long enough to matter.
     */
    public void testPathologicalPatternDoesNotBlowUp() {
        assertFalse(NamePatterns.matches("a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assertTrue(NamePatterns.matches("a*a*a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaab"));
    }

    public void testLiteralPrefixExtraction() {
        assertEquals("logs-", NamePatterns.literalPrefixOf("logs-*"));
        assertEquals("logs-", NamePatterns.literalPrefixOf("logs-?x"));
        assertEquals("", NamePatterns.literalPrefixOf("*-logs"));
        assertEquals("exact", NamePatterns.literalPrefixOf("exact"));
        assertEquals("", NamePatterns.literalPrefixOf(""));
    }

    /**
     * Cutting a prefix between the halves of a surrogate pair yields a string that is not valid UTF-16,
     * which encodes to a replacement character and matches nothing.
     */
    public void testLiteralPrefixNeverSplitsASurrogatePair() {
        String supplementary = new String(Character.toChars(0x10000));
        String prefix = NamePatterns.literalPrefixOf("x" + supplementary + "*");

        assertEquals("x" + supplementary, prefix);
        for (int i = 0; i < prefix.length(); i++) {
            assertFalse("prefix ends on a lone surrogate", Character.isHighSurrogate(prefix.charAt(prefix.length() - 1)));
        }
    }

    public void testLeadingWildcardIsDetectable() {
        assertTrue(NamePatterns.isLeadingWildcard("*-logs"));
        assertTrue(NamePatterns.isLeadingWildcard("?x"));
        assertFalse(NamePatterns.isLeadingWildcard("logs-*"));
        assertFalse(NamePatterns.isLeadingWildcard("exact"));
    }

    // -------------------------------------------------------------- overlay

    /**
     * The property the whole overlay design turns on. The base is immutable, so a delete cannot remove
     * anything from it; without a tombstone the base entry stays visible and a deleted index reappears.
     */
    public void testDeleteOfABaseEntryLeavesATombstone() {
        NameIndexOverlay overlay = new NameIndexOverlay();

        overlay.delete("lives-only-in-base");

        assertTrue(overlay.isDeleted("lives-only-in-base"));
        assertTrue(overlay.covers("lives-only-in-base"));
        assertNull(overlay.get("lives-only-in-base"));
    }

    public void testPutAndDeleteAreMutuallyExclusive() {
        NameIndexOverlay overlay = new NameIndexOverlay();

        overlay.put(entry("a"));
        overlay.delete("a");
        assertTrue(overlay.isDeleted("a"));
        assertNull(overlay.get("a"));

        overlay.put(entry("a"));
        assertFalse(overlay.isDeleted("a"));
        assertNotNull(overlay.get("a"));
    }

    // ------------------------------------------------------------ composite

    public void testLookupFallsThroughToBase() {
        NameIndex index = new NameIndex(base("alpha", "beta"));

        assertEquals("alpha", index.lookup("alpha").getName());
        assertEquals("beta", index.lookup("beta").getName());
        assertNull(index.lookup("gamma"));
    }

    public void testCreateIsVisibleImmediately() {
        NameIndex index = new NameIndex(base("alpha"));

        assertNull(index.lookup("gamma"));
        index.create(entry("gamma"));
        assertNotNull(index.lookup("gamma"));
        assertEquals(1, index.pendingSize());
    }

    public void testDeleteHidesABaseEntry() {
        NameIndex index = new NameIndex(base("alpha", "beta"));

        index.delete("alpha");

        assertNull(index.lookup("alpha"));
        assertFalse(index.contains("alpha"));
        assertNotNull(index.lookup("beta"));
    }

    public void testOverlaySupersedesBaseForTheSameName() {
        NameIndex index = new NameIndex(base("alpha"));
        byte[] replacement = uuid(99);

        index.create(new IndexNameEntry("alpha", replacement, IndexNameEntry.STATUS_CLOSED));

        IndexNameEntry resolved = index.lookup("alpha");
        assertArrayEquals(replacement, resolved.getUuid());
        assertTrue(resolved.isClosed());
    }

    // ------------------------------------------------------- merged pattern

    public void testPatternMergesBaseAndOverlayInOrder() {
        NameIndex index = new NameIndex(base("logs-a", "logs-c", "logs-e"));

        index.create(entry("logs-b"));
        index.create(entry("logs-d"));

        assertEquals(List.of("logs-a", "logs-b", "logs-c", "logs-d", "logs-e"), names(index.resolve("logs-*")));
    }

    public void testPatternSkipsTombstonedBaseEntries() {
        NameIndex index = new NameIndex(base("logs-a", "logs-b", "logs-c"));

        index.delete("logs-b");

        assertEquals(List.of("logs-a", "logs-c"), names(index.resolve("logs-*")));
    }

    /**
     * A name held in both base and overlay must be emitted once. Emitting it from both streams is the
     * most likely merge bug, and it produces duplicate indices in a wildcard result.
     */
    public void testSupersededNameAppearsExactlyOnce() {
        NameIndex index = new NameIndex(base("logs-a", "logs-b"));

        index.create(new IndexNameEntry("logs-a", uuid(42), IndexNameEntry.STATUS_CLOSED));

        List<IndexNameEntry> resolved = index.resolve("logs-*");
        assertEquals(List.of("logs-a", "logs-b"), names(resolved));
        assertEquals(1, resolved.stream().filter(e -> e.getName().equals("logs-a")).count());
        assertTrue("the overlay version should win", resolved.get(0).isClosed());
    }

    public void testOverlayOnlyMatchesAtBothEnds() {
        NameIndex index = new NameIndex(base("logs-m"));

        index.create(entry("logs-a"));
        index.create(entry("logs-z"));

        assertEquals(List.of("logs-a", "logs-m", "logs-z"), names(index.resolve("logs-*")));
    }

    public void testExactPatternTakesTheLookupPath() {
        NameIndex index = new NameIndex(base("alpha", "alphabet"));

        assertEquals(List.of("alpha"), names(index.resolve("alpha")));
    }

    public void testLeadingWildcardStillResolvesCorrectly() {
        NameIndex index = new NameIndex(base("app-logs", "sys-logs", "metrics"));

        index.create(entry("db-logs"));

        // Correct but unbounded: no literal prefix means every name is scanned. A11 decides whether to
        // reject, index reversed names, or bound this with a timeout.
        assertEquals(List.of("app-logs", "db-logs", "sys-logs"), names(index.resolve("*-logs")));
    }

    public void testMatchEverything() {
        NameIndex index = new NameIndex(base("a", "b"));
        index.create(entry("c"));

        assertEquals(List.of("a", "b", "c"), names(index.resolve("*")));
    }

    // -------------------------------------------------------------- rebuild

    public void testRebuildPreservesExactlyTheVisibleSet() {
        NameIndex index = new NameIndex(base("keep-1", "keep-2", "drop-me"));
        index.create(entry("added"));
        index.delete("drop-me");

        List<String> before = names(index.resolve("*"));
        assertEquals(List.of("added", "keep-1", "keep-2"), before);

        index.rebuild();

        assertEquals("nothing should be pending after a rebuild", 0, index.pendingSize());
        assertEquals("the base should now hold everything", 3, index.baseSize());
        assertEquals("a rebuild must change nothing observable", before, names(index.resolve("*")));
    }

    public void testRebuildCarriesTheOverlayVersionOfASupersededName() {
        NameIndex index = new NameIndex(base("alpha"));
        index.create(new IndexNameEntry("alpha", uuid(77), IndexNameEntry.STATUS_CLOSED));

        index.rebuild();

        IndexNameEntry resolved = index.lookup("alpha");
        assertEquals(1, index.baseSize());
        assertArrayEquals(uuid(77), resolved.getUuid());
        assertTrue(resolved.isClosed());
    }

    public void testRebuildOfATombstoneForANameNotInTheBase() {
        NameIndex index = new NameIndex(base("alpha"));
        index.delete("never-existed");

        index.rebuild();

        assertEquals(1, index.baseSize());
        assertNull(index.lookup("never-existed"));
        assertNotNull(index.lookup("alpha"));
    }

    public void testRebuildPolicyHasAFloorSoSmallIndexesDoNotThrash() {
        NameIndex index = new NameIndex(base("a"), 0.05, 1_000);

        for (int i = 0; i < 100; i++) {
            index.create(entry("added-" + i));
        }

        // 100 pending against a base of 1 is far past the ratio, but under the floor. Without the floor
        // a tiny index would rebuild on essentially every write.
        assertFalse(index.shouldRebuild());
        assertFalse(index.maybeRebuild());
    }

    public void testRebuildTriggersOnceBothRatioAndFloorAreMet() {
        NameIndex index = new NameIndex(base(namesArray("base-", 1000)), 0.05, 10);

        for (int i = 0; i < 60; i++) {
            index.create(entry("added-" + i));
        }

        assertTrue(index.shouldRebuild());
        assertTrue(index.maybeRebuild());
        assertEquals(0, index.pendingSize());
        assertEquals(1060, index.baseSize());
    }

    /**
     * Reads must not block or observe a torn state while a rebuild swaps the base. Base and overlay are
     * held in one immutable holder replaced by a single reference assignment, so a reader sees one
     * consistent generation or the other.
     */
    public void testReadsStayConsistentAcrossAConcurrentRebuild() throws Exception {
        NameIndex index = new NameIndex(base(namesArray("idx-", 2000)));
        for (int i = 0; i < 200; i++) {
            index.create(entry("extra-" + String.format("%04d", i)));
        }

        int expected = 2200;
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean sawWrongCount = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);

        Thread reader = new Thread(() -> {
            started.countDown();
            while (stop.get() == false) {
                if (index.resolve("*").size() != expected) {
                    sawWrongCount.set(true);
                    return;
                }
            }
        });
        reader.start();
        assertTrue(started.await(10, TimeUnit.SECONDS));

        for (int i = 0; i < 20; i++) {
            index.rebuild();
        }
        stop.set(true);
        reader.join(TimeUnit.SECONDS.toMillis(30));

        assertFalse("a reader observed a torn base/overlay pair", sawWrongCount.get());
        assertEquals(expected, index.resolve("*").size());
    }

    /**
     * A16. The overlay was a hash map, so every pattern query walked all of it. The rebuild policy lets
     * the overlay reach a fraction of the base before folding, which at 100M names is millions of
     * entries scanned per wildcard -- in a structure whose entire purpose is that cost tracks matches
     * rather than population.
     *
     * <p>Asserted structurally rather than by timing: the range view a query walks must be a small
     * fraction of the overlay, which is the property a hash map cannot have at any speed.
     */
    public void testOverlayPatternQueryScansARangeNotTheWholeOverlay() {
        NameIndexOverlay overlay = new NameIndexOverlay();
        for (int i = 0; i < 10_000; i++) {
            String name = "prefix-" + String.format("%02d", i % 50) + "-" + String.format("%05d", i);
            overlay.put(new IndexNameEntry(name, uuid(i), IndexNameEntry.STATUS_OPEN));
        }

        assertEquals(10_000, overlay.size());
        // Every name at or after this prefix, which for the last bucket is only its own 200 entries.
        int visited = 0;
        for (String name : overlay.putsFrom("prefix-49").keySet()) {
            if (name.startsWith("prefix-49") == false) {
                break;
            }
            visited++;
        }
        assertEquals(200, visited);

        // The range view itself is bounded, which is what a hash map could not offer.
        assertTrue("the tail view should be far smaller than the overlay", overlay.putsFrom("prefix-49").size() < overlay.size() / 10);
    }

    public void testOverlayIterationIsInByteOrder() {
        NameIndexOverlay overlay = new NameIndexOverlay();
        for (String name : List.of("zulu", "alpha", "mike", "bravo")) {
            overlay.put(new IndexNameEntry(name, uuid(1), IndexNameEntry.STATUS_OPEN));
        }

        // Sorted iteration is what lets the merge skip its per-query sort.
        assertEquals(List.of("alpha", "bravo", "mike", "zulu"), new ArrayList<>(overlay.puts().keySet()));
    }

    public void testRamUsageIsReportedFromTheBase() {
        NameIndex index = new NameIndex(base(namesArray("idx-", 1000)));

        assertTrue(index.ramBytesUsed() > 0);
    }

    // --------------------------------------------------------------- helpers

    private static List<String> names(List<IndexNameEntry> entries) {
        return entries.stream().map(IndexNameEntry::getName).collect(Collectors.toList());
    }

    private static String[] namesArray(String prefix, int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add(prefix + String.format("%06d", i));
        }
        return names.toArray(new String[0]);
    }

    private static CompactNameIndex base(String... names) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder();
        for (int i = 0; i < names.length; i++) {
            builder.add(names[i], uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        return builder.build();
    }

    private static IndexNameEntry entry(String name) {
        return new IndexNameEntry(name, uuid(name.hashCode() & 0x7F), IndexNameEntry.STATUS_OPEN);
    }

    private static byte[] uuid(int seed) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) (seed + i);
        }
        return uuid;
    }
}
