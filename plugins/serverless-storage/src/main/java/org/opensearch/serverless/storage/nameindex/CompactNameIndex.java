/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import java.nio.charset.StandardCharsets;
import java.util.function.IntConsumer;

/**
 * An immutable, sorted index over every index name in the system.
 *
 * <p>This exists because wildcard and alias resolution is the one part of the partitioned design that
 * cannot be partitioned. Everything else scales by hashing on the index, so a node only needs the
 * indices hashed to it. Answering {@code logs-*} needs every name, which is global by construction. The
 * alternatives were scattering every wildcard to every partition, or restricting the query language.
 *
 * <p>Measured (S13, {@code benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md}): this layout costs 45.0 B per
 * name against 145.5 B for a {@code HashMap<String, byte[]>}, so 4.2 GiB against 13.6 GiB for 100M
 * names. Nearly all of the difference is per-entry object overhead -- a {@code String} header, its cached
 * hash, its backing array, and a map node -- and none of that survives when names live in one blob
 * addressed by an offset array.
 *
 * <h2>Ordering is UTF-8 byte order, deliberately</h2>
 *
 * Every comparison here treats names as unsigned UTF-8 bytes, never as {@code String}s.
 * {@link String#compareTo} orders by UTF-16 code unit, which disagrees with UTF-8 byte order for
 * anything above the basic multilingual plane, because surrogate pairs sort below U+E000..U+FFFF in
 * UTF-16 and above them in UTF-8. Sorting with one order and searching with the other yields a binary
 * search that silently fails to find entries that are present, for some inputs and not others. Keeping
 * one order end to end is the only way to avoid that, and it has to be the byte order, because that is
 * what the stored form is.
 */
public final class CompactNameIndex {

    /** Raw UUID width. 16 bytes rather than the 36 characters of the canonical text form. */
    public static final int UUID_LENGTH = 16;

    /** All names concatenated, in sorted order, with no separators. */
    private final byte[] nameBlob;

    /** Start offset of each name, with a trailing entry so {@code offsets[i+1]} is always the end. */
    private final int[] offsets;

    /** {@link #UUID_LENGTH} bytes per entry, parallel to the name order. */
    private final byte[] uuids;

    /** One status byte per entry, parallel to the name order. */
    private final byte[] statuses;

    CompactNameIndex(byte[] nameBlob, int[] offsets, byte[] uuids, byte[] statuses) {
        this.nameBlob = nameBlob;
        this.offsets = offsets;
        this.uuids = uuids;
        this.statuses = statuses;
    }

    /** An index over no names. Useful as a starting base before the first rebuild. */
    public static CompactNameIndex empty() {
        return new CompactNameIndex(new byte[0], new int[] { 0 }, new byte[0], new byte[0]);
    }

    public int size() {
        return statuses.length;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Ordinal of an exact name, or a negative insertion point when absent, following the
     * {@link java.util.Arrays#binarySearch} convention of {@code -(insertionPoint) - 1}.
     *
     * <p>The negative form is not a courtesy: it is what lets a caller find where a name would go
     * without paying for a second search, which the overlay merge in {@link NameIndex} relies on.
     */
    public int ordinalOf(String name) {
        return search(name.getBytes(StandardCharsets.UTF_8));
    }

    /** Whether an exact name is present. */
    public boolean contains(String name) {
        return ordinalOf(name) >= 0;
    }

    public String nameAt(int ordinal) {
        checkOrdinal(ordinal);
        int start = offsets[ordinal];
        return new String(nameBlob, start, offsets[ordinal + 1] - start, StandardCharsets.UTF_8);
    }

    public byte[] uuidAt(int ordinal) {
        checkOrdinal(ordinal);
        byte[] uuid = new byte[UUID_LENGTH];
        System.arraycopy(uuids, ordinal * UUID_LENGTH, uuid, 0, UUID_LENGTH);
        return uuid;
    }

    public byte statusAt(int ordinal) {
        checkOrdinal(ordinal);
        return statuses[ordinal];
    }

    public IndexNameEntry entryAt(int ordinal) {
        return new IndexNameEntry(nameAt(ordinal), uuidAt(ordinal), statusAt(ordinal));
    }

    /**
     * First ordinal whose name is at or after {@code prefix} in byte order, or {@link #size()} if every
     * name sorts before it.
     *
     * <p>Separate from {@link #ordinalOf} because a prefix scan needs the position of the first possible
     * match rather than an exact hit, and folding the two together would make both harder to read.
     */
    public int lowerBound(byte[] prefix) {
        int lo = 0;
        int hi = size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (compareAt(mid, prefix) < 0) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    /**
     * Calls {@code consumer} with the ordinal of every name starting with {@code prefix}, in sorted
     * order.
     *
     * <p>Streaming rather than returning a collection, because a wildcard can match a large fraction of
     * the index and materializing that would defeat the point of the structure. Cost is a binary search
     * plus one step per match, so it tracks the size of the answer rather than the size of the index.
     */
    public void forEachWithPrefix(String prefix, IntConsumer consumer) {
        byte[] target = prefix.getBytes(StandardCharsets.UTF_8);
        for (int i = lowerBound(target); i < size() && startsWith(i, target); i++) {
            consumer.accept(i);
        }
    }

    /** Bytes retained, so the sizing claim can be asserted in a test rather than assumed. */
    public long ramBytesUsed() {
        // Object headers and the four references, then the arrays themselves. Approximate by design:
        // the point is to catch a representation that has quietly stopped being compact, not to model
        // the JVM exactly.
        return 16L + 4L * 8L + arrayBytes(nameBlob.length) + arrayBytes(4L * offsets.length) + arrayBytes(uuids.length) + arrayBytes(
            statuses.length
        );
    }

    private static long arrayBytes(long contentBytes) {
        // 16-byte header plus length, rounded up to an 8-byte boundary.
        return ((16 + contentBytes) + 7) / 8 * 8;
    }

    private int search(byte[] target) {
        int lo = 0;
        int hi = size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int cmp = compareAt(mid, target);
            if (cmp < 0) {
                lo = mid + 1;
            } else if (cmp > 0) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -(lo + 1);
    }

    /**
     * Compares the name at {@code ordinal} against {@code target} as unsigned bytes. Unsigned matters:
     * Java bytes are signed, so any name containing a byte above 0x7F -- which is every non-ASCII name,
     * since UTF-8 continuation bytes are all above it -- would compare as negative and sort before pure
     * ASCII without the mask.
     */
    private int compareAt(int ordinal, byte[] target) {
        int start = offsets[ordinal];
        int length = offsets[ordinal + 1] - start;
        int shared = Math.min(length, target.length);
        for (int i = 0; i < shared; i++) {
            int diff = (nameBlob[start + i] & 0xFF) - (target[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return length - target.length;
    }

    private boolean startsWith(int ordinal, byte[] prefix) {
        int start = offsets[ordinal];
        int length = offsets[ordinal + 1] - start;
        if (length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (nameBlob[start + i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    private void checkOrdinal(int ordinal) {
        if (ordinal < 0 || ordinal >= size()) {
            throw new IndexOutOfBoundsException("ordinal " + ordinal + " out of range for size " + size());
        }
    }
}
