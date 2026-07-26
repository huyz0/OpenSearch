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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

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

        // One pass to size the blob, so a large build allocates once rather than growing repeatedly.
        long totalNameBytes = 0;
        for (int i = 0; i < entries.size(); i++) {
            totalNameBytes += entries.get(i).name.length;
            if (i > 0 && BY_UTF8_BYTES.compare(entries.get(i - 1), entries.get(i)) == 0) {
                throw new IllegalStateException("duplicate name: " + new String(entries.get(i).name, StandardCharsets.UTF_8));
            }
        }
        if (totalNameBytes > Integer.MAX_VALUE) {
            throw new IllegalStateException("total name bytes " + totalNameBytes + " exceeds what a single blob can address");
        }

        int count = entries.size();
        byte[] nameBlob = new byte[(int) totalNameBytes];
        int[] offsets = new int[count + 1];
        byte[] uuids = new byte[count * CompactNameIndex.UUID_LENGTH];
        byte[] statuses = new byte[count];

        int position = 0;
        for (int i = 0; i < count; i++) {
            Entry entry = entries.get(i);
            offsets[i] = position;
            System.arraycopy(entry.name, 0, nameBlob, position, entry.name.length);
            position += entry.name.length;
            System.arraycopy(entry.uuid, 0, uuids, i * CompactNameIndex.UUID_LENGTH, CompactNameIndex.UUID_LENGTH);
            statuses[i] = entry.status;
        }
        offsets[count] = position;

        CompactNameIndex withoutAliases = new CompactNameIndex(nameBlob, offsets, uuids, statuses);

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

        return new CompactNameIndex(nameBlob, offsets, uuids, statuses, aliasOrdinals, aliasTargetOffsets, aliasTargets);
    }
}
