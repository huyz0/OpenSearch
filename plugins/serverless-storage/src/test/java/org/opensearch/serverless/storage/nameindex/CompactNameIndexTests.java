/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A1 to A4. The immutable base of the name index tier.
 *
 * <p>The structure exists because wildcard resolution is the one part of the partitioned design that
 * cannot be partitioned, and S13 measured this layout at 45 B/name against a {@code HashMap}'s 145.5 B.
 * These tests cover the contract that saving is paid for with: no per-entry object, so everything is an
 * offset into a shared blob and every comparison is on raw bytes.
 */
public class CompactNameIndexTests extends OpenSearchTestCase {

    public void testEmptyIndex() {
        CompactNameIndex index = CompactNameIndex.empty();

        assertEquals(0, index.size());
        assertTrue(index.isEmpty());
        assertFalse(index.contains("anything"));
        // Absent from an empty index still reports an insertion point, namely the front.
        assertEquals(-1, index.ordinalOf("anything"));
        assertEquals(0, index.lowerBound("a".getBytes(StandardCharsets.UTF_8)));
    }

    public void testBuilderProducesSortedOrderRegardlessOfInsertionOrder() {
        CompactNameIndex index = builder("zulu", "alpha", "mike", "bravo").build();

        assertEquals(4, index.size());
        assertEquals("alpha", index.nameAt(0));
        assertEquals("bravo", index.nameAt(1));
        assertEquals("mike", index.nameAt(2));
        assertEquals("zulu", index.nameAt(3));
    }

    public void testRoundTripOfEveryField() {
        byte[] uuid = uuidOf(7);
        CompactNameIndex index = new CompactNameIndexBuilder().add("only", uuid, IndexNameEntry.STATUS_CLOSED).build();

        assertEquals("only", index.nameAt(0));
        assertArrayEquals(uuid, index.uuidAt(0));
        assertEquals(IndexNameEntry.STATUS_CLOSED, index.statusAt(0));

        IndexNameEntry entry = index.entryAt(0);
        assertEquals("only", entry.getName());
        assertArrayEquals(uuid, entry.getUuid());
        assertTrue(entry.isClosed());
    }

    public void testOrdinalOfReturnsInsertionPointWhenAbsent() {
        CompactNameIndex index = builder("b", "d", "f").build();

        assertEquals(0, index.ordinalOf("b"));
        assertEquals(1, index.ordinalOf("d"));
        assertEquals(2, index.ordinalOf("f"));

        // The negative form is -(insertionPoint) - 1, which the overlay merge relies on to find where a
        // name would sit without paying for a second search.
        assertEquals(-1, index.ordinalOf("a"));
        assertEquals(-2, index.ordinalOf("c"));
        assertEquals(-3, index.ordinalOf("e"));
        assertEquals(-4, index.ordinalOf("g"));
    }

    /**
     * The trap this structure is most likely to fall into, and the reason every comparison here is on
     * bytes rather than on {@code String}.
     *
     * <p>{@link String#compareTo} orders by UTF-16 code unit. A supplementary character is a surrogate
     * pair starting at 0xD800, so in UTF-16 it sorts <em>below</em> U+E000..U+FFFF. In UTF-8 it starts
     * with 0xF0, so it sorts <em>above</em> them. Sort with one order and search with the other and the
     * binary search walks the wrong way, failing to find entries that are present -- for non-ASCII names
     * only, which is exactly the sort of bug an ASCII-only test suite never sees.
     */
    public void testUtf8AndUtf16OrderingDisagreeAndUtf8Wins() {
        String supplementary = new String(Character.toChars(0x10000));  // UTF-8 F0 90 80 80
        String bmpHigh = "Ａ";                                      // UTF-8 EF BC A1

        // Establish that the two orders genuinely disagree for this pair, so the test below is
        // meaningful rather than accidentally passing.
        assertTrue("expected UTF-16 order to place the supplementary char first", supplementary.compareTo(bmpHigh) < 0);
        byte[] supplementaryBytes = supplementary.getBytes(StandardCharsets.UTF_8);
        byte[] bmpHighBytes = bmpHigh.getBytes(StandardCharsets.UTF_8);
        assertTrue("expected UTF-8 order to place it second", (supplementaryBytes[0] & 0xFF) > (bmpHighBytes[0] & 0xFF));

        CompactNameIndex index = builder(supplementary, bmpHigh, "ascii").build();

        // Byte order, so the BMP character precedes the supplementary one.
        assertEquals("ascii", index.nameAt(0));
        assertEquals(bmpHigh, index.nameAt(1));
        assertEquals(supplementary, index.nameAt(2));

        // And both are findable, which is what actually breaks when the two orders are mixed.
        assertEquals(1, index.ordinalOf(bmpHigh));
        assertEquals(2, index.ordinalOf(supplementary));
    }

    /**
     * Java bytes are signed, and every UTF-8 continuation byte is above 0x7F. Without masking, any
     * non-ASCII name compares as negative and sorts before all ASCII, so the structure is ordered
     * inconsistently with its own search.
     */
    public void testNonAsciiNamesSortAfterAsciiAndRemainFindable() {
        String accented = "tenant-é";   // UTF-8 ends C3 A9, both above 0x7F
        CompactNameIndex index = builder("tenant-a", accented, "tenant-z").build();

        assertEquals("tenant-a", index.nameAt(0));
        assertEquals("tenant-z", index.nameAt(1));
        assertEquals(accented, index.nameAt(2));

        assertEquals(2, index.ordinalOf(accented));
        assertTrue(index.contains(accented));
    }

    public void testPrefixIsAPrefixNotAnEqualityMatch() {
        CompactNameIndex index = builder("logs", "logs-2024", "logs-2025", "logsx", "metrics").build();

        assertEquals(List.of("logs", "logs-2024", "logs-2025", "logsx"), namesWithPrefix(index, "logs"));
        assertEquals(List.of("logs-2024", "logs-2025"), namesWithPrefix(index, "logs-"));
        assertEquals(List.of("logs-2024"), namesWithPrefix(index, "logs-2024"));
        assertEquals(List.of(), namesWithPrefix(index, "logs-2026"));
    }

    public void testPrefixMatchingEverythingAndNothing() {
        CompactNameIndex index = builder("a", "b", "c").build();

        assertEquals(List.of("a", "b", "c"), namesWithPrefix(index, ""));
        assertEquals(List.of(), namesWithPrefix(index, "zzz"));
    }

    /**
     * A name shorter than the prefix cannot match it. Worth a test of its own because the comparison
     * loop reads {@code prefix.length} bytes and would run off the end of a short name without the
     * length guard.
     */
    public void testPrefixLongerThanCandidateName() {
        CompactNameIndex index = builder("ab").build();

        assertEquals(List.of(), namesWithPrefix(index, "abc"));
        assertEquals(List.of("ab"), namesWithPrefix(index, "ab"));
    }

    public void testLowerBoundFindsFirstAtOrAfter() {
        CompactNameIndex index = builder("b", "d", "f").build();

        assertEquals(0, index.lowerBound("a".getBytes(StandardCharsets.UTF_8)));
        assertEquals(0, index.lowerBound("b".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, index.lowerBound("c".getBytes(StandardCharsets.UTF_8)));
        assertEquals(3, index.lowerBound("z".getBytes(StandardCharsets.UTF_8)));
    }

    public void testDuplicateNamesAreRejectedRatherThanDeduplicated() {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder().add("same", uuidOf(1), IndexNameEntry.STATUS_OPEN)
            .add("same", uuidOf(2), IndexNameEntry.STATUS_OPEN);

        // Two entries with one name make ordinalOf ambiguous about which UUID it returns. A caller that
        // produced them has a bug, and silently keeping one would hide it.
        IllegalStateException e = expectThrows(IllegalStateException.class, builder::build);
        assertTrue(e.getMessage(), e.getMessage().contains("duplicate name"));
    }

    public void testBuilderRejectsMalformedInput() {
        expectThrows(IllegalArgumentException.class, () -> new CompactNameIndexBuilder().add("", uuidOf(1), IndexNameEntry.STATUS_OPEN));
        expectThrows(IllegalArgumentException.class, () -> new CompactNameIndexBuilder().add("n", new byte[4], IndexNameEntry.STATUS_OPEN));
        expectThrows(NullPointerException.class, () -> new CompactNameIndexBuilder().add(null, uuidOf(1), IndexNameEntry.STATUS_OPEN));
        expectThrows(NullPointerException.class, () -> new CompactNameIndexBuilder().add("n", null, IndexNameEntry.STATUS_OPEN));
    }

    public void testOrdinalBoundsAreChecked() {
        CompactNameIndex index = builder("a").build();

        expectThrows(IndexOutOfBoundsException.class, () -> index.nameAt(1));
        expectThrows(IndexOutOfBoundsException.class, () -> index.nameAt(-1));
        expectThrows(IndexOutOfBoundsException.class, () -> index.uuidAt(1));
        expectThrows(IndexOutOfBoundsException.class, () -> index.statusAt(1));
    }

    /**
     * The entry hands out a copy, because entries outlive the structure they came from once
     * {@link NameIndex} starts swapping bases on rebuild.
     */
    public void testEntryDoesNotShareItsUuidArray() {
        byte[] uuid = uuidOf(3);
        IndexNameEntry entry = new IndexNameEntry("n", uuid, IndexNameEntry.STATUS_OPEN);

        uuid[0] = (byte) 0xFF;
        assertNotEquals((byte) 0xFF, entry.getUuid()[0]);

        byte[] handedOut = entry.getUuid();
        handedOut[0] = (byte) 0xFF;
        assertNotEquals((byte) 0xFF, entry.getUuid()[0]);
    }

    /**
     * Guards the property the whole structure exists for. S13 measured 45 B/name; this asserts the
     * representation has not quietly regressed to something with a per-entry object, without pinning an
     * exact figure that would break on an unrelated JVM detail.
     */
    public void testRamUsageStaysCompact() {
        int count = 10_000;
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.add("tenant-" + String.format("%08x", i) + "-index", uuidOf(i), IndexNameEntry.STATUS_OPEN);
        }
        CompactNameIndex index = builder.build();

        double perName = (double) index.ramBytesUsed() / count;
        // Names here are 21 bytes, plus 16 for the UUID, 4 for the offset and 1 for the status: 42.
        assertTrue("expected under 60 B/name, got " + perName, perName < 60);
        assertTrue("expected at least the raw content, got " + perName, perName > 40);
    }

    public void testLargerRandomPopulationRoundTrips() {
        int count = 5_000;
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder(count);
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String name = "idx-" + randomAlphaOfLength(12) + "-" + i;
            names.add(name);
            builder.add(name, uuidOf(i), IndexNameEntry.STATUS_OPEN);
        }
        CompactNameIndex index = builder.build();

        assertEquals(count, index.size());
        for (String name : names) {
            assertTrue("missing " + name, index.contains(name));
        }
        // Sorted, checked pairwise on the encoded form rather than on String order.
        for (int i = 1; i < index.size(); i++) {
            byte[] previous = index.nameAt(i - 1).getBytes(StandardCharsets.UTF_8);
            byte[] current = index.nameAt(i).getBytes(StandardCharsets.UTF_8);
            assertTrue("not sorted at " + i, compareUnsigned(previous, current) < 0);
        }
    }

    private static int compareUnsigned(byte[] a, byte[] b) {
        int shared = Math.min(a.length, b.length);
        for (int i = 0; i < shared; i++) {
            int diff = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.length - b.length;
    }

    private static List<String> namesWithPrefix(CompactNameIndex index, String prefix) {
        List<String> found = new ArrayList<>();
        index.forEachWithPrefix(prefix, ordinal -> found.add(index.nameAt(ordinal)));
        return found;
    }

    private static CompactNameIndexBuilder builder(String... names) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder();
        for (int i = 0; i < names.length; i++) {
            builder.add(names[i], uuidOf(i), IndexNameEntry.STATUS_OPEN);
        }
        return builder;
    }

    private static byte[] uuidOf(int seed) {
        byte[] uuid = new byte[CompactNameIndex.UUID_LENGTH];
        for (int i = 0; i < uuid.length; i++) {
            uuid[i] = (byte) (seed + i);
        }
        return uuid;
    }
}
