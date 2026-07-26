/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Accumulates entries and packs them into a {@link CompactNameIndex}.
 *
 * <p>Building is a separate object because the packed form cannot be added to. Names live in one
 * contiguous blob in sorted order, so inserting one would mean shifting every byte after it. That is
 * the trade the compact representation makes: reads are cheap and dense, writes go somewhere else
 * ({@link NameIndexOverlay}) and are folded back in by a rebuild.
 */
public final class CompactNameIndexBuilder {

    /** Held as encoded bytes rather than {@code String}s so sorting and packing use one representation. */
    private static final class Entry {
        final byte[] name;
        final byte[] uuid;
        final byte status;
        /** Target index names, for aliases only. Null for an ordinary index. */
        final List<String> targets;

        Entry(byte[] name, byte[] uuid, byte status, List<String> targets) {
            this.name = name;
            this.uuid = uuid;
            this.status = status;
            this.targets = targets;
        }
    }

    /**
     * Unsigned byte order, matching {@link CompactNameIndex}'s comparisons exactly. Sorting with any
     * other order -- notably {@link String#compareTo}, which is UTF-16 code unit order -- produces a
     * structure whose binary search fails to find entries that are present, for non-ASCII names only,
     * which is the kind of bug that survives a test suite full of ASCII.
     */
    private static final Comparator<Entry> BY_UTF8_BYTES = (a, b) -> {
        int shared = Math.min(a.name.length, b.name.length);
        for (int i = 0; i < shared; i++) {
            int diff = (a.name[i] & 0xFF) - (b.name[i] & 0xFF);
            if (diff != 0) {
                return diff;
            }
        }
        return a.name.length - b.name.length;
    };

    private final List<Entry> entries;

    public CompactNameIndexBuilder() {
        this.entries = new ArrayList<>();
    }

    public CompactNameIndexBuilder(int expectedSize) {
        this.entries = new ArrayList<>(expectedSize);
    }

    public CompactNameIndexBuilder add(String name, byte[] uuid, byte status) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uuid, "uuid");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
        if (uuid.length != CompactNameIndex.UUID_LENGTH) {
            throw new IllegalArgumentException("uuid must be " + CompactNameIndex.UUID_LENGTH + " bytes, was " + uuid.length);
        }
        entries.add(new Entry(name.getBytes(StandardCharsets.UTF_8), uuid.clone(), status, null));
        return this;
    }

    /**
     * Adds an alias, which names a set of indices rather than one.
     *
     * <p>Targets are given as names because ordinals do not exist until the sort has happened. They are
     * resolved to ordinals in {@link #build()}, which is also where a target that does not exist gets
     * dropped.
     */
    public CompactNameIndexBuilder addAlias(String name, byte[] uuid, List<String> targets) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(uuid, "uuid");
        Objects.requireNonNull(targets, "targets");
        if (name.isEmpty()) {
            throw new IllegalArgumentException("name must not be empty");
        }
        if (uuid.length != CompactNameIndex.UUID_LENGTH) {
            throw new IllegalArgumentException("uuid must be " + CompactNameIndex.UUID_LENGTH + " bytes, was " + uuid.length);
        }
        entries.add(
            new Entry(
                name.getBytes(StandardCharsets.UTF_8),
                uuid.clone(),
                IndexNameEntry.STATUS_ALIAS,
                new ArrayList<>(new LinkedHashSet<>(targets))
            )
        );
        return this;
    }

    /**
     * Packs an already-sorted stream directly, without buffering entries.
     *
     * <p>A18 measured the buffered path at 13.8x the steady-state size during a rebuild, which
     * extrapolates to about 108 GiB at 100M names. The cause is this class's own intermediate list: one
     * {@code Entry} object per name, each with its own arrays, which is exactly the per-entry overhead
     * the packed form exists to avoid. Double-buffering the bases was never the expensive part.
     *
     * <p>Rebuild does not need the buffer at all. Its two inputs -- the existing base and the overlay --
     * are both already in UTF-8 byte order, so a merge of them is sorted by construction. This takes two
     * passes over that merge: one to count entries and name bytes so the arrays are allocated exactly
     * once, and one to write. Walking a merge twice is free next to materializing it.
     *
     * <p>The supplier must return a fresh iterator each call, and both iterators must yield the same
     * sequence.
     */
    public static CompactNameIndex buildFromSorted(Supplier<Iterator<IndexNameEntry>> sortedEntries) {
        int count = 0;
        long totalNameBytes = 0;
        for (Iterator<IndexNameEntry> it = sortedEntries.get(); it.hasNext();) {
            IndexNameEntry entry = it.next();
            int nameLength = entry.getName().getBytes(StandardCharsets.UTF_8).length;
            if (nameLength > CompactNameIndex.CHUNK_SIZE) {
                throw new IllegalStateException("name of " + nameLength + " bytes exceeds the chunk size");
            }
            count++;
            totalNameBytes += nameLength;
        }
        if (count == 0) {
            return CompactNameIndex.empty();
        }

        int[] offsets = new int[count + 1];
        byte[] uuids = new byte[count * CompactNameIndex.UUID_LENGTH];
        byte[] statuses = new byte[count];
        List<byte[]> chunks = new ArrayList<>();
        List<Integer> chunkFirstOrdinals = new ArrayList<>();
        List<Integer> chunkLengths = new ArrayList<>();
        byte[] chunk = new byte[(int) Math.min(CompactNameIndex.CHUNK_SIZE, Math.max(totalNameBytes, 1))];
        chunkFirstOrdinals.add(0);
        int position = 0;
        long packed = 0;

        // Aliases are a small minority, so holding just their names and targets costs little next to the
        // buffer this method exists to avoid.
        List<Integer> aliasOrdinalList = new ArrayList<>();
        List<List<String>> aliasTargetNames = new ArrayList<>();

        int ordinal = 0;
        byte[] previousName = null;
        for (Iterator<IndexNameEntry> it = sortedEntries.get(); it.hasNext(); ordinal++) {
            IndexNameEntry entry = it.next();
            byte[] name = entry.getName().getBytes(StandardCharsets.UTF_8);
            if (previousName != null) {
                int cmp = NamePatterns.compareUtf8(previousName, name);
                if (cmp == 0) {
                    throw new IllegalStateException("duplicate name: " + entry.getName());
                }
                if (cmp > 0) {
                    // The whole point of this path is that the input is already ordered. Silently
                    // accepting an unsorted stream would produce a structure whose binary search is
                    // wrong, which is far harder to diagnose than a failure here.
                    throw new IllegalStateException("stream is not sorted at " + entry.getName());
                }
            }
            previousName = name;

            if (position + name.length > chunk.length) {
                chunks.add(chunk);
                chunkLengths.add(position);
                chunkFirstOrdinals.add(ordinal);
                packed += position;
                chunk = new byte[(int) Math.min(CompactNameIndex.CHUNK_SIZE, Math.max(totalNameBytes - packed, name.length))];
                position = 0;
            }
            offsets[ordinal] = position;
            System.arraycopy(name, 0, chunk, position, name.length);
            position += name.length;
            System.arraycopy(entry.getUuid(), 0, uuids, ordinal * CompactNameIndex.UUID_LENGTH, CompactNameIndex.UUID_LENGTH);
            statuses[ordinal] = entry.getStatus();
            if (entry.isAlias()) {
                aliasOrdinalList.add(ordinal);
                aliasTargetNames.add(entry.getTargets());
            }
        }
        chunks.add(chunk);
        chunkLengths.add(position);
        chunkFirstOrdinals.add(count);
        offsets[count] = position;

        byte[][] nameChunks = chunks.toArray(new byte[0][]);
        int[] chunkFirstOrdinalArray = toIntArray(chunkFirstOrdinals);
        int[] chunkLengthArray = toIntArray(chunkLengths);

        CompactNameIndex withoutAliases = new CompactNameIndex(
            nameChunks,
            offsets,
            chunkFirstOrdinalArray,
            chunkLengthArray,
            uuids,
            statuses
        );
        if (aliasOrdinalList.isEmpty()) {
            return withoutAliases;
        }

        int[] aliasOrdinals = toIntArray(aliasOrdinalList);
        int[] aliasTargetOffsets = new int[aliasOrdinals.length + 1];
        List<Integer> flatTargets = new ArrayList<>();
        for (int i = 0; i < aliasOrdinals.length; i++) {
            aliasTargetOffsets[i] = flatTargets.size();
            for (String target : aliasTargetNames.get(i)) {
                int targetOrdinal = withoutAliases.ordinalOf(target);
                if (targetOrdinal >= 0) {
                    flatTargets.add(targetOrdinal);
                }
            }
        }
        aliasTargetOffsets[aliasOrdinals.length] = flatTargets.size();

        return new CompactNameIndex(
            nameChunks,
            offsets,
            chunkFirstOrdinalArray,
            chunkLengthArray,
            uuids,
            statuses,
            aliasOrdinals,
            aliasTargetOffsets,
            toIntArray(flatTargets)
        );
    }

    private static int[] toIntArray(List<Integer> values) {
        int[] array = new int[values.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = values.get(i);
        }
        return array;
    }

    public CompactNameIndexBuilder add(IndexNameEntry entry) {
        if (entry.isAlias()) {
            return addAlias(entry.getName(), entry.getUuid(), entry.getTargets());
        }
        return add(entry.getName(), entry.getUuid(), entry.getStatus());
    }

    public int size() {
        return entries.size();
    }

    /**
     * Sorts, checks for duplicates, and packs.
     *
     * <p>Duplicates are rejected rather than deduplicated. Two entries with the same name make
     * {@link CompactNameIndex#ordinalOf} ambiguous about which UUID it is returning, and a caller that
     * produced them has a bug worth surfacing rather than papering over.
     */
    public CompactNameIndex build() {
        if (entries.isEmpty()) {
            return CompactNameIndex.empty();
        }

        entries.sort(BY_UTF8_BYTES);

        // One pass to check ordering and size the chunks, so a large build allocates deliberately rather
        // than growing repeatedly.
        long totalNameBytes = 0;
        for (int i = 0; i < entries.size(); i++) {
            int nameLength = entries.get(i).name.length;
            if (nameLength > CompactNameIndex.CHUNK_SIZE) {
                throw new IllegalStateException("name of " + nameLength + " bytes exceeds the chunk size");
            }
            totalNameBytes += nameLength;
            if (i > 0 && BY_UTF8_BYTES.compare(entries.get(i - 1), entries.get(i)) == 0) {
                throw new IllegalStateException("duplicate name: " + new String(entries.get(i).name, StandardCharsets.UTF_8));
            }
        }

        int count = entries.size();
        int[] offsets = new int[count + 1];
        byte[] uuids = new byte[count * CompactNameIndex.UUID_LENGTH];
        byte[] statuses = new byte[count];

        // Names are packed into chunks and never split across a boundary, because a single Java array
        // cannot exceed Integer.MAX_VALUE elements and 100M names at a realistic 30 to 40 bytes each is
        // 3 to 4 GB of text. A contiguous blob works at benchmark name lengths and fails at real ones.
        List<byte[]> chunks = new ArrayList<>();
        List<Integer> chunkFirstOrdinals = new ArrayList<>();
        List<Integer> chunkLengths = new ArrayList<>();
        byte[] chunk = new byte[(int) Math.min(CompactNameIndex.CHUNK_SIZE, Math.max(totalNameBytes, 1))];
        chunkFirstOrdinals.add(0);
        int position = 0;
        long packed = 0;

        for (int i = 0; i < count; i++) {
            Entry entry = entries.get(i);
            if (position + entry.name.length > chunk.length) {
                // Close this chunk short rather than splitting a name across the boundary, which would
                // make every comparison and every substring read span two arrays.
                chunks.add(chunk);
                chunkLengths.add(position);
                chunkFirstOrdinals.add(i);
                packed += position;
                chunk = new byte[(int) Math.min(CompactNameIndex.CHUNK_SIZE, Math.max(totalNameBytes - packed, entry.name.length))];
                position = 0;
            }
            offsets[i] = position;
            System.arraycopy(entry.name, 0, chunk, position, entry.name.length);
            position += entry.name.length;
            System.arraycopy(entry.uuid, 0, uuids, i * CompactNameIndex.UUID_LENGTH, CompactNameIndex.UUID_LENGTH);
            statuses[i] = entry.status;
        }
        chunks.add(chunk);
        chunkLengths.add(position);
        chunkFirstOrdinals.add(count);
        offsets[count] = position;

        byte[][] nameChunks = chunks.toArray(new byte[0][]);
        int[] chunkFirstOrdinalArray = new int[chunkFirstOrdinals.size()];
        for (int i = 0; i < chunkFirstOrdinalArray.length; i++) {
            chunkFirstOrdinalArray[i] = chunkFirstOrdinals.get(i);
        }
        int[] chunkLengthArray = new int[chunkLengths.size()];
        for (int i = 0; i < chunkLengthArray.length; i++) {
            chunkLengthArray[i] = chunkLengths.get(i);
        }

        CompactNameIndex withoutAliases = new CompactNameIndex(
            nameChunks,
            offsets,
            chunkFirstOrdinalArray,
            chunkLengthArray,
            uuids,
            statuses
        );

        // Alias targets resolve in a second pass, against the just-built structure rather than against a
        // transient name-to-ordinal map. At 100M entries such a map would cost more than the index it
        // describes, and the structure can already answer the question by binary search.
        int aliasCount = 0;
        for (Entry entry : entries) {
            if (entry.targets != null) {
                aliasCount++;
            }
        }
        if (aliasCount == 0) {
            return withoutAliases;
        }

        int[] aliasOrdinals = new int[aliasCount];
        int[] aliasTargetOffsets = new int[aliasCount + 1];
        List<Integer> flatTargets = new ArrayList<>();
        int aliasPosition = 0;
        for (int i = 0; i < count; i++) {
            if (entries.get(i).targets == null) {
                continue;
            }
            aliasOrdinals[aliasPosition] = i;
            aliasTargetOffsets[aliasPosition] = flatTargets.size();
            for (String target : entries.get(i).targets) {
                int targetOrdinal = withoutAliases.ordinalOf(target);
                // A target that is not present is dropped rather than rejected. An index can be deleted
                // while an alias still names it, and a rebuild that threw would then be unable to make
                // progress at all -- turning a stale reference into an outage.
                if (targetOrdinal >= 0) {
                    flatTargets.add(targetOrdinal);
                }
            }
            aliasPosition++;
        }
        aliasTargetOffsets[aliasCount] = flatTargets.size();

        int[] aliasTargets = new int[flatTargets.size()];
        for (int i = 0; i < aliasTargets.length; i++) {
            aliasTargets[i] = flatTargets.get(i);
        }

        return new CompactNameIndex(
            nameChunks,
            offsets,
            chunkFirstOrdinalArray,
            chunkLengthArray,
            uuids,
            statuses,
            aliasOrdinals,
            aliasTargetOffsets,
            aliasTargets
        );
    }
}
