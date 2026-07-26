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

/**
 * A11. The one question the Area A plan left explicitly open, answered with a measurement rather than
 * an opinion.
 *
 * <p>A pattern with a literal prefix seeks into the sorted structure and scans only the matching range,
 * so cost tracks the size of the answer. A pattern starting with a wildcard, {@code *-logs}, has no
 * seekable prefix and degenerates to a full scan of every name. At 100M that is the one query shape
 * that does not scale.
 *
 * <p>Three options were on the table:
 *
 * <ol>
 *   <li><b>Reversed-name index.</b> A second {@link CompactNameIndex} over reversed names, turning
 *       {@code *-logs} into the prefix {@code sgol-} on the reversed structure. Roughly doubles memory.
 *   <li><b>Reject.</b> Fail leading wildcards with a clear error.
 *   <li><b>Accept and bound.</b> Allow the scan with a timeout.
 * </ol>
 *
 * <p>This measures option (a) so the choice is informed. It is a measurement kept as a test rather than
 * a benchmark, so it runs in CI and cannot silently rot; the assertions are loose bounds on the shape of
 * the answer, not on absolute timings, which vary by machine.
 */
public class LeadingWildcardSpikeTests extends OpenSearchTestCase {

    private static final int NAME_COUNT = 200_000;

    /**
     * Reversing turns a suffix query into a prefix query. Surrogate pairs must survive it, which is why
     * this uses {@link StringBuilder#reverse()} rather than reversing a char array: reversing chars
     * naively splits a pair and produces a name that encodes to replacement characters and matches
     * nothing.
     */
    static String reverse(String name) {
        return new StringBuilder(name).reverse().toString();
    }

    public void testReversedIndexTurnsALeadingWildcardIntoAPrefixQuery() {
        List<String> names = syntheticNames();

        CompactNameIndexBuilder forwardBuilder = new CompactNameIndexBuilder(names.size());
        CompactNameIndexBuilder reversedBuilder = new CompactNameIndexBuilder(names.size());
        for (int i = 0; i < names.size(); i++) {
            forwardBuilder.add(names.get(i), uuid(i), IndexNameEntry.STATUS_OPEN);
            reversedBuilder.add(reverse(names.get(i)), uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        CompactNameIndex forward = forwardBuilder.build();
        CompactNameIndex reversed = reversedBuilder.build();

        String suffix = "-logs";
        String pattern = "*" + suffix;

        // Option (c): scan everything and glob-match each name.
        long start = System.nanoTime();
        int scanMatches = 0;
        for (int ordinal = 0; ordinal < forward.size(); ordinal++) {
            if (NamePatterns.matches(pattern, forward.nameAt(ordinal))) {
                scanMatches++;
            }
        }
        long scanMicros = (System.nanoTime() - start) / 1000;

        // Option (a): seek the reversed structure by the reversed suffix.
        start = System.nanoTime();
        List<Integer> seekMatches = new ArrayList<>();
        reversed.forEachWithPrefix(reverse(suffix), seekMatches::add);
        long seekMicros = (System.nanoTime() - start) / 1000;

        assertEquals("both routes must find the same set", scanMatches, seekMatches.size());
        assertTrue("the synthetic population should actually contain matches", scanMatches > 0);

        double memoryRatio = (double) (forward.ramBytesUsed() + reversed.ramBytesUsed()) / forward.ramBytesUsed();

        logger.info(
            "leading wildcard over {} names: full scan {} us, reversed seek {} us, speedup {}x, memory {}x",
            NAME_COUNT,
            scanMicros,
            seekMicros,
            scanMicros == 0 ? "n/a" : String.format("%.1f", (double) scanMicros / Math.max(seekMicros, 1)),
            String.format("%.2f", memoryRatio)
        );

        // The decision-relevant properties, asserted as shape rather than as absolute timings.
        assertTrue("a reversed index should not much more than double memory, was " + memoryRatio, memoryRatio < 2.2);
        assertTrue("the reversed seek should not be slower than the full scan", seekMicros <= Math.max(scanMicros, 1));
    }

    /**
     * The reason the seek wins is that its cost tracks matches while the scan's tracks population. This
     * pins that difference: a suffix that matches nothing is nearly free on the reversed structure and
     * still costs a full pass on the forward one.
     */
    public void testAMissIsCheapOnTheReversedIndexAndExpensiveOnAScan() {
        List<String> names = syntheticNames();
        CompactNameIndexBuilder reversedBuilder = new CompactNameIndexBuilder(names.size());
        CompactNameIndexBuilder forwardBuilder = new CompactNameIndexBuilder(names.size());
        for (int i = 0; i < names.size(); i++) {
            forwardBuilder.add(names.get(i), uuid(i), IndexNameEntry.STATUS_OPEN);
            reversedBuilder.add(reverse(names.get(i)), uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        CompactNameIndex forward = forwardBuilder.build();
        CompactNameIndex reversed = reversedBuilder.build();

        String pattern = "*-nothing-matches-this";

        long start = System.nanoTime();
        for (int ordinal = 0; ordinal < forward.size(); ordinal++) {
            NamePatterns.matches(pattern, forward.nameAt(ordinal));
        }
        long scanMicros = (System.nanoTime() - start) / 1000;

        start = System.nanoTime();
        List<Integer> found = new ArrayList<>();
        reversed.forEachWithPrefix(reverse("-nothing-matches-this"), found::add);
        long seekMicros = (System.nanoTime() - start) / 1000;

        assertEquals(0, found.size());
        logger.info("leading-wildcard miss: full scan {} us, reversed seek {} us", scanMicros, seekMicros);
        assertTrue("a miss should be near-free on the reversed index", seekMicros < Math.max(scanMicros, 1));
    }

    public void testReversalSurvivesSurrogatePairs() {
        String supplementary = new String(Character.toChars(0x10000));
        String name = "prefix-" + supplementary + "-suffix";

        assertEquals(name, reverse(reverse(name)));
        // The pair must still be a pair after one reversal, or the encoded form is not valid UTF-8.
        assertEquals(name.codePointCount(0, name.length()), reverse(name).codePointCount(0, reverse(name).length()));
    }

    private static List<String> syntheticNames() {
        List<String> names = new ArrayList<>(NAME_COUNT);
        // A mixed population: a minority share a common suffix, which is the shape a leading wildcard
        // is actually used to select.
        for (int i = 0; i < NAME_COUNT; i++) {
            String suffix = (i % 20 == 0) ? "-logs" : "-metrics";
            names.add("tenant-" + String.format("%08x", i) + suffix);
        }
        return names;
    }

    private static byte[] uuid(int seed) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) (seed + i);
        }
        return uuid;
    }
}
