/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * A15. The blocking defect a review pass found after phases 1 to 4 were already committed and green.
 *
 * <p>The name blob was a single {@code byte[]} addressed by {@code int} offsets. A Java array cannot
 * hold more than {@link Integer#MAX_VALUE} elements, which is about 2.15 GB, and 100M names at a
 * realistic 30 to 40 bytes each is 3 to 4 GB of text. **The structure could not hold the population it
 * was designed for.**
 *
 * <p>What makes it worth recording rather than just fixing: S13's synthetic names were 21 bytes, so
 * 100M of them total 2.10 GB and land just under the limit. The benchmark that justified the whole
 * design would have passed at full scale while any real deployment failed, and the margin was 2%. The
 * builder's own guard threw rather than corrupting, so this would have surfaced as a hard failure
 * rather than as silent truncation, but the target was unreachable either way.
 *
 * <p>These tests shrink {@link CompactNameIndex#CHUNK_SIZE} so a boundary is actually crossed. Building
 * a real 1 GB chunk in a unit test is not viable, and chunking that is never exercised is chunking that
 * does not work.
 */
public class CompactNameIndexChunkingTests extends OpenSearchTestCase {

    private int originalChunkSize;

    @Before
    public void shrinkChunkSize() {
        originalChunkSize = CompactNameIndex.CHUNK_SIZE;
        // Small enough that a few dozen names span several chunks.
        CompactNameIndex.CHUNK_SIZE = 64;
    }

    @After
    public void restoreChunkSize() {
        CompactNameIndex.CHUNK_SIZE = originalChunkSize;
    }

    public void testNamesSpanningManyChunksRoundTrip() {
        List<String> names = names(200);
        CompactNameIndex index = build(names);

        assertEquals(names.size(), index.size());
        for (int i = 0; i < names.size(); i++) {
            assertEquals("name at " + i, names.get(i), index.nameAt(i));
            assertEquals("lookup of " + names.get(i), i, index.ordinalOf(names.get(i)));
        }
    }

    /**
     * Binary search compares names that live in different chunks, so an implementation that forgot the
     * chunk lookup would read the wrong bytes rather than fail outright.
     */
    public void testBinarySearchAcrossChunkBoundaries() {
        List<String> names = names(500);
        CompactNameIndex index = build(names);

        for (String name : names) {
            assertTrue("missing " + name, index.contains(name));
        }
        assertFalse(index.contains("name-zzzzzz"));
        assertTrue("absent name should report an insertion point", index.ordinalOf("name-zzzzzz") < 0);
    }

    /**
     * The last name in a chunk ends at the chunk's used length rather than at the next entry's offset,
     * because the next entry starts at zero in a new chunk. Getting that wrong yields a truncated or
     * over-long name exactly at each boundary, which is why this walks every entry.
     */
    public void testLastNameInEachChunkIsNotTruncated() {
        List<String> names = names(300);
        CompactNameIndex index = build(names);

        for (int i = 0; i < index.size(); i++) {
            String name = index.nameAt(i);
            assertEquals("name at " + i + " has the wrong length", names.get(i).length(), name.length());
            assertEquals(names.get(i), name);
        }
    }

    public void testPrefixScanAcrossChunkBoundaries() {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            names.add("logs-" + String.format("%05d", i));
        }
        for (int i = 0; i < 100; i++) {
            names.add("metrics-" + String.format("%05d", i));
        }
        names.sort(null);
        CompactNameIndex index = build(names);

        List<String> matched = new ArrayList<>();
        index.forEachWithPrefix("logs-", ordinal -> matched.add(index.nameAt(ordinal)));

        assertEquals(300, matched.size());
        assertEquals("logs-00000", matched.get(0));
        assertEquals("logs-00299", matched.get(299));
    }

    public void testAliasTargetsResolveAcrossChunks() {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder();
        List<String> targets = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String name = "target-" + String.format("%05d", i);
            targets.add(name);
            builder.add(name, uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        CompactNameIndex index = builder.addAlias("zzz-alias", uuid(0), targets).build();

        assertEquals(targets, index.aliasTargetsAt(index.ordinalOf("zzz-alias")));
    }

    public void testRebuildAcrossChunksPreservesEverything() {
        NameIndex index = new NameIndex(build(names(150)));
        index.create("added-name-0001", uuid(1), IndexNameEntry.STATUS_OPEN);
        index.delete("name-000010");

        List<String> before = new ArrayList<>();
        index.forEachMatching("*", entry -> before.add(entry.getName()));

        index.rebuild();

        List<String> after = new ArrayList<>();
        index.forEachMatching("*", entry -> after.add(entry.getName()));
        assertEquals(before, after);
        assertFalse(after.contains("name-000010"));
        assertTrue(after.contains("added-name-0001"));
    }

    /**
     * A name longer than a whole chunk cannot be packed at all. Rejecting it explicitly beats letting
     * the packing loop spin forever trying to find room.
     */
    public void testNameLargerThanAChunkIsRejected() {
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < CompactNameIndex.CHUNK_SIZE + 10; i++) {
            huge.append('x');
        }
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder().add(huge.toString(), uuid(1), IndexNameEntry.STATUS_OPEN);

        IllegalStateException e = expectThrows(IllegalStateException.class, builder::build);
        assertTrue(e.getMessage(), e.getMessage().contains("exceeds the chunk size"));
    }

    public void testMultiByteNamesAreNotSplitMidCharacterAtAChunkBoundary() {
        List<String> names = new ArrayList<>();
        // Each of these encodes to 3 UTF-8 bytes per character, so boundaries land inside characters
        // unless the packer refuses to split a name.
        for (int i = 0; i < 120; i++) {
            names.add("名前-" + String.format("%04d", i));
        }
        names.sort(null);
        CompactNameIndex index = build(names);

        for (int i = 0; i < index.size(); i++) {
            String name = index.nameAt(i);
            assertFalse("name at " + i + " contains a replacement character: " + name, name.contains("�"));
            assertArrayEquals(names.get(i).getBytes(StandardCharsets.UTF_8), name.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static CompactNameIndex build(List<String> names) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder(names.size());
        for (int i = 0; i < names.size(); i++) {
            builder.add(names.get(i), uuid(i), IndexNameEntry.STATUS_OPEN);
        }
        return builder.build();
    }

    private static List<String> names(int count) {
        List<String> names = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            names.add("name-" + String.format("%06d", i));
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
