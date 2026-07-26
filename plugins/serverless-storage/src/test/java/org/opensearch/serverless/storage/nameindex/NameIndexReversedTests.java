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
import java.util.stream.Collectors;

/**
 * A11a to A11d. The reversed index, which A11 decided to build after measuring the alternative.
 *
 * <p>A pattern with a literal prefix seeks the forward structure. A pattern starting with a wildcard has
 * no seekable prefix, and over 100M names a full scan costs about 18 seconds. Reversing the names turns
 * {@code *-logs} into the prefix {@code sgol-} on a second structure, at the cost of doubling memory
 * from 4.2 to 8.4 GiB.
 *
 * <p>Most of these tests are about A11c, the trap the plan called out: the forward and reversed
 * structures must move together. A create that reaches one and not the other is visible to prefix
 * queries and invisible to suffix ones, which is worse than failing, because nothing reports an error.
 */
public class NameIndexReversedTests extends OpenSearchTestCase {

    // ------------------------------------------------------- suffix extraction

    public void testLiteralSuffixExtraction() {
        assertEquals("-logs", NamePatterns.literalSuffixOf("*-logs"));
        // The last wildcard is the leading ?, so everything after it is literal.
        assertEquals("x-logs", NamePatterns.literalSuffixOf("?x-logs"));
        assertEquals("-logs", NamePatterns.literalSuffixOf("x?-logs"));
        assertEquals("", NamePatterns.literalSuffixOf("logs-*"));
        assertEquals("exact", NamePatterns.literalSuffixOf("exact"));
        assertEquals("", NamePatterns.literalSuffixOf(""));
        assertEquals("", NamePatterns.literalSuffixOf("*"));
        assertEquals("-2024", NamePatterns.literalSuffixOf("logs-*-2024"));
    }

    public void testSuffixExtractionNeverSplitsASurrogatePair() {
        String supplementary = new String(Character.toChars(0x10000));
        String suffix = NamePatterns.literalSuffixOf("*" + supplementary + "x");

        assertEquals(supplementary + "x", suffix);
        assertFalse("suffix starts on a lone low surrogate", Character.isLowSurrogate(suffix.charAt(0)));
    }

    public void testReversalIsItsOwnInverseAndPreservesCodePoints() {
        String name = "prefix-" + new String(Character.toChars(0x10000)) + "-suffix";

        assertEquals(name, NamePatterns.reverse(NamePatterns.reverse(name)));
        assertEquals(name.codePointCount(0, name.length()), NamePatterns.reverse(name).codePointCount(0, name.length()));
    }

    // ------------------------------------------------------------ suffix path

    public void testSuffixQueryResolvesFromTheBase() {
        NameIndex index = new NameIndex(base("app-logs", "sys-logs", "app-metrics", "sys-metrics"));

        assertEquals(List.of("app-logs", "sys-logs"), names(index.resolve("*-logs")));
        assertEquals(List.of("app-metrics", "sys-metrics"), names(index.resolve("*-metrics")));
        assertEquals(List.of(), names(index.resolve("*-nothing")));
    }

    /** Callers are promised forward byte order even though the reversed structure yields reversed order. */
    public void testSuffixResultsComeBackInForwardByteOrder() {
        NameIndex index = new NameIndex(base("zeta-logs", "alpha-logs", "mike-logs", "bravo-logs"));

        assertEquals(List.of("alpha-logs", "bravo-logs", "mike-logs", "zeta-logs"), names(index.resolve("*-logs")));
    }

    public void testSuffixQueryStillAppliesTheFullPattern() {
        NameIndex index = new NameIndex(base("app-2024-logs", "app-2025-logs", "sys-2024-logs"));

        // The suffix only seeks; the rest of the pattern still has to match.
        assertEquals(List.of("app-2024-logs", "app-2025-logs"), names(index.resolve("app-*-logs")));
        assertEquals(List.of("app-2024-logs", "sys-2024-logs"), names(index.resolve("*-2024-logs")));
    }

    // ------------------------------------------------- A11c: the two in step

    /**
     * The trap. A create must be visible to both directions. Reaching the forward structure and not the
     * reversed one produces an index that answers prefix queries correctly and suffix queries with stale
     * data, silently.
     */
    public void testCreateIsVisibleToBothPrefixAndSuffixQueries() {
        NameIndex index = new NameIndex(base("app-logs"));

        index.create(entry("db-logs"));

        assertEquals("prefix query", List.of("db-logs"), names(index.resolve("db-*")));
        assertEquals("suffix query", List.of("app-logs", "db-logs"), names(index.resolve("*-logs")));
    }

    public void testDeleteIsHiddenFromBothPrefixAndSuffixQueries() {
        NameIndex index = new NameIndex(base("app-logs", "sys-logs"));

        index.delete("app-logs");

        assertEquals("prefix query", List.of(), names(index.resolve("app-*")));
        assertEquals("suffix query", List.of("sys-logs"), names(index.resolve("*-logs")));
    }

    public void testOverlaySupersedesTheBaseOnTheSuffixPathToo() {
        NameIndex index = new NameIndex(base("app-logs"));

        index.create(new IndexNameEntry("app-logs", uuid(99), IndexNameEntry.STATUS_CLOSED));

        List<IndexNameEntry> resolved = index.resolve("*-logs");
        assertEquals(1, resolved.size());
        assertTrue("the overlay version should win on the suffix path", resolved.get(0).isClosed());
    }

    /** A rebuild that produced only the forward structure would leave suffix queries answering stale data. */
    public void testRebuildKeepsBothStructuresInStep() {
        NameIndex index = new NameIndex(base("app-logs"));
        index.create(entry("db-logs"));
        index.delete("app-logs");

        index.rebuild();

        assertEquals(0, index.pendingSize());
        assertEquals("suffix query after rebuild", List.of("db-logs"), names(index.resolve("*-logs")));
        assertEquals("prefix query after rebuild", List.of("db-logs"), names(index.resolve("db-*")));
    }

    public void testManyCreatesAndDeletesStayConsistentAcrossBothDirections() {
        NameIndex index = new NameIndex(base(generated(200, "-logs")));

        for (int i = 0; i < 50; i++) {
            index.create(entry("added-" + String.format("%03d", i) + "-logs"));
        }
        for (int i = 0; i < 50; i++) {
            index.delete("tenant-" + String.format("%03d", i) + "-logs");
        }

        List<String> viaSuffix = names(index.resolve("*-logs"));
        List<String> viaPrefixAdded = names(index.resolve("added-*"));
        List<String> viaEverything = names(index.resolve("*"));

        assertEquals("suffix and full scan must agree", viaEverything, viaSuffix);
        assertEquals(50, viaPrefixAdded.size());
        assertEquals(200, viaSuffix.size());

        index.rebuild();
        assertEquals("a rebuild must change nothing observable", viaSuffix, names(index.resolve("*-logs")));
    }

    /**
     * Found by the review pass, and the same failure as the rebuild one in a second place. The reversed
     * twin holds no alias targets, because target ordinals refer to the forward structure and mean
     * nothing in a differently-sorted one. Building the result entry from the twin therefore returned an
     * alias pointing at nothing, only ever on the suffix path.
     */
    public void testAliasResolvedViaTheSuffixPathKeepsItsTargets() {
        CompactNameIndex forward = new CompactNameIndexBuilder().add("app-logs", uuid(1), IndexNameEntry.STATUS_OPEN)
            .add("sys-logs", uuid(2), IndexNameEntry.STATUS_OPEN)
            .addAlias("all-logs", uuid(3), List.of("app-logs", "sys-logs"))
            .build();
        NameIndex index = new NameIndex(forward);

        List<IndexNameEntry> resolved = index.resolve("*-logs");
        assertEquals(List.of("all-logs", "app-logs", "sys-logs"), names(resolved));

        IndexNameEntry alias = resolved.stream().filter(IndexNameEntry::isAlias).findFirst().orElseThrow();
        assertEquals(List.of("app-logs", "sys-logs"), alias.getTargets());
    }

    // ------------------------------------------------------------- routing

    /** A pattern anchored at both ends should take the prefix path, which streams instead of collecting. */
    public void testPatternWithBothAnchorsUsesThePrefixPath() {
        NameIndex index = new NameIndex(base("logs-a-2024", "logs-b-2024", "metrics-a-2024"));

        assertEquals(List.of("logs-a-2024", "logs-b-2024"), names(index.resolve("logs-*-2024")));
    }

    /** Neither end anchored is a genuine full scan, and must still be correct. */
    public void testUnanchoredPatternStillResolves() {
        NameIndex index = new NameIndex(base("app-logs", "sys-metrics"));
        index.create(entry("db-logs"));

        assertEquals(List.of("app-logs", "db-logs", "sys-metrics"), names(index.resolve("*")));
        assertEquals(List.of("app-logs", "db-logs"), names(index.resolve("*log*")));
    }

    public void testRamUsageCountsBothStructures() {
        CompactNameIndex forward = base(generated(500, "-logs"));
        NameIndex index = new NameIndex(forward);

        // Hiding the reversed twin's cost would understate sizing by about half, which is the number
        // capacity planning is done against.
        assertTrue(
            "expected roughly double the forward structure, got " + index.ramBytesUsed() + " against " + forward.ramBytesUsed(),
            index.ramBytesUsed() > forward.ramBytesUsed() * 1.5
        );
    }

    // -------------------------------------------------------------- helpers

    private static List<String> names(List<IndexNameEntry> entries) {
        return entries.stream().map(IndexNameEntry::getName).collect(Collectors.toList());
    }

    private static String[] generated(int count, String suffix) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add("tenant-" + String.format("%03d", i) + suffix);
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
