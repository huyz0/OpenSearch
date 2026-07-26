/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The name index: every index name in the system, resolvable by exact name or by glob pattern.
 *
 * <p>This is the only public entry point of the tier. It composes an immutable {@link CompactNameIndex}
 * base holding the bulk with a small mutable {@link NameIndexOverlay} holding changes since the last
 * rebuild.
 *
 * <p>It exists because wildcard resolution is the one part of the partitioned design that cannot be
 * partitioned: everything else is reached by hashing on the index, but answering {@code logs-*} needs
 * every name. S13 measured the base at 45 B/name, so 100M names cost about 4.2 GiB, which is small
 * enough that this tier is replicated for availability rather than sharded for capacity.
 *
 * <h2>Consistency model, stated rather than discovered</h2>
 *
 * <ul>
 *   <li>A single {@link #create} or {@link #delete} is visible to any subsequent read. There is no
 *       delay and no eventual-consistency window for one change.
 *   <li>A <em>batch</em> of changes is not atomic. A reader running concurrently with several writes may
 *       observe some and not others. Nothing here offers a snapshot across multiple names.
 *   <li>Results are always in UTF-8 byte order, including the merge of base and overlay, so callers
 *       resolving a wildcard get a deterministic list.
 *   <li>{@link #rebuild()} blocks concurrent writers for its duration. At 100M names that is not brief,
 *       and double-buffering the overlay so writes proceed during a rebuild is a known follow-up rather
 *       than something this class does.
 * </ul>
 *
 * <p>Reads never block, including during a rebuild. Base and overlay are held in a single immutable
 * {@link State} swapped by one reference assignment, so a reader sees one consistent pair or the other
 * and never a base from one generation with an overlay from another.
 */
public final class NameIndex {

    /**
     * Rebuild once the overlay reaches this fraction of the base. Small enough that overlay scans stay
     * cheap, large enough that rebuilds are rare.
     */
    static final double DEFAULT_REBUILD_RATIO = 0.05;

    /**
     * Never rebuild below this many pending changes regardless of ratio. Without a floor, a small index
     * rebuilds on almost every write: at a base of 10 entries the ratio alone would trigger on the first
     * one.
     */
    static final int DEFAULT_REBUILD_FLOOR = 1_000;

    /** Base, reversed base and overlay as one unit, so a reader can never mix generations. */
    private static final class State {
        final CompactNameIndex base;
        /**
         * The same names reversed, so a suffix query becomes a prefix query.
         *
         * <p>A11 measured the alternative: a leading wildcard over 100M names by full scan is about 18
         * seconds, against microseconds for a seek. Doubling memory from 4.2 to 8.4 GiB buys that, and
         * 8.4 GiB is still one node.
         */
        final CompactNameIndex reversedBase;
        final NameIndexOverlay overlay;

        State(CompactNameIndex base, CompactNameIndex reversedBase, NameIndexOverlay overlay) {
            this.base = base;
            this.reversedBase = reversedBase;
            this.overlay = overlay;
        }
    }

    private final double rebuildRatio;
    private final int rebuildFloor;

    /** Guards writes and rebuilds against each other. Reads do not take it. */
    private final Object writeLock = new Object();

    private volatile State state;

    public NameIndex() {
        this(CompactNameIndex.empty(), DEFAULT_REBUILD_RATIO, DEFAULT_REBUILD_FLOOR);
    }

    public NameIndex(CompactNameIndex base) {
        this(base, DEFAULT_REBUILD_RATIO, DEFAULT_REBUILD_FLOOR);
    }

    public NameIndex(CompactNameIndex base, double rebuildRatio, int rebuildFloor) {
        Objects.requireNonNull(base, "base");
        this.state = new State(base, reversedOf(base), new NameIndexOverlay());
        this.rebuildRatio = rebuildRatio;
        this.rebuildFloor = rebuildFloor;
    }

    /** Builds the reversed twin of a base. Aliases are carried by name, so targets survive unchanged. */
    private static CompactNameIndex reversedOf(CompactNameIndex base) {
        CompactNameIndexBuilder builder = new CompactNameIndexBuilder(base.size());
        for (int ordinal = 0; ordinal < base.size(); ordinal++) {
            builder.add(NamePatterns.reverse(base.nameAt(ordinal)), base.uuidAt(ordinal), base.statusAt(ordinal));
        }
        return builder.build();
    }

    // ---------------------------------------------------------------- reads

    /** The entry for an exact name, or null if absent. */
    public IndexNameEntry lookup(String name) {
        Objects.requireNonNull(name, "name");
        State current = state;

        // Overlay first, and tombstones before puts, because the overlay is authoritative about
        // anything it has an opinion on.
        if (current.overlay.isDeleted(name)) {
            return null;
        }
        IndexNameEntry pending = current.overlay.get(name);
        if (pending != null) {
            return pending;
        }

        int ordinal = current.base.ordinalOf(name);
        return ordinal >= 0 ? current.base.entryAt(ordinal) : null;
    }

    public boolean contains(String name) {
        return lookup(name) != null;
    }

    /**
     * Every entry matching a glob pattern, in UTF-8 byte order, streamed rather than collected.
     *
     * <p>A pattern with a literal prefix seeks to it and scans only the matching range, so cost tracks
     * the size of the answer. A pattern starting with a wildcard has no seekable prefix and scans
     * everything; {@link NamePatterns#isLeadingWildcard} lets a caller detect and reject that case
     * before paying for it.
     */
    public void forEachMatching(String pattern, Consumer<IndexNameEntry> consumer) {
        Objects.requireNonNull(pattern, "pattern");
        State current = state;

        // An exact name is the common case and does not need a merge at all.
        if (NamePatterns.isPattern(pattern) == false) {
            IndexNameEntry entry = lookup(pattern);
            if (entry != null) {
                consumer.accept(entry);
            }
            return;
        }

        String prefix = NamePatterns.literalPrefixOf(pattern);
        if (prefix.isEmpty()) {
            String suffix = NamePatterns.literalSuffixOf(pattern);
            if (suffix.isEmpty() == false) {
                forEachMatchingBySuffix(current, pattern, suffix, consumer);
                return;
            }
            // Neither end is anchored, so there is nothing to seek on in either direction and this is a
            // genuine full scan. `*` alone is the common case and means everything.
        }

        // A range scan over the sorted overlay, stopping at the first name that leaves the prefix. The
        // overlay was a hash map originally, which meant walking all of it on every query; the rebuild
        // policy lets it reach a fraction of the base, so at 100M that was millions of entries per
        // wildcard. Already in byte order, so no per-query sort either.
        List<IndexNameEntry> overlayMatches = new ArrayList<>();
        for (IndexNameEntry entry : current.overlay.putsFrom(prefix).values()) {
            if (entry.getName().startsWith(prefix) == false) {
                break;
            }
            if (NamePatterns.matches(pattern, entry.getName())) {
                overlayMatches.add(entry);
            }
        }

        // Merge the base scan with the sorted overlay matches, emitting in byte order. Base entries that
        // the overlay has tombstoned or replaced are skipped, so a name never appears twice.
        int overlayPosition = 0;
        int ordinal = current.base.lowerBound(prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        for (; ordinal < current.base.size(); ordinal++) {
            String name = current.base.nameAt(ordinal);
            if (name.startsWith(prefix) == false) {
                break;
            }
            if (NamePatterns.matches(pattern, name) == false) {
                continue;
            }
            if (current.overlay.covers(name)) {
                // Either deleted, or superseded by a put that the overlay stream will emit.
                continue;
            }
            while (overlayPosition < overlayMatches.size()
                && NamePatterns.compareUtf8(overlayMatches.get(overlayPosition).getName(), name) < 0) {
                consumer.accept(overlayMatches.get(overlayPosition++));
            }
            consumer.accept(current.base.entryAt(ordinal));
        }
        while (overlayPosition < overlayMatches.size()) {
            consumer.accept(overlayMatches.get(overlayPosition++));
        }
    }

    /**
     * Suffix-anchored resolution, seeking the reversed base instead of scanning the forward one.
     *
     * <p>Results are collected and sorted rather than streamed in order, because the reversed structure
     * yields them in reversed-name order and callers are promised forward byte order. That is affordable
     * precisely because this path exists for queries whose answer is small relative to the index; a
     * pattern selecting most of the index has a prefix to seek on instead.
     */
    private void forEachMatchingBySuffix(State current, String pattern, String suffix, Consumer<IndexNameEntry> consumer) {
        String reversedSuffix = NamePatterns.reverse(suffix);
        List<IndexNameEntry> matches = new ArrayList<>();

        current.reversedBase.forEachWithPrefix(reversedSuffix, ordinal -> {
            String name = NamePatterns.reverse(current.reversedBase.nameAt(ordinal));
            if (current.overlay.covers(name)) {
                // Deleted, or superseded by a put the overlay stream below will emit.
                return;
            }
            if (NamePatterns.matches(pattern, name)) {
                matches.add(new IndexNameEntry(name, current.reversedBase.uuidAt(ordinal), current.reversedBase.statusAt(ordinal)));
            }
        });

        for (IndexNameEntry entry : current.overlay.reversedPutsFrom(reversedSuffix)
            .entrySet()
            .stream()
            .takeWhile(e -> e.getKey().startsWith(reversedSuffix))
            .map(java.util.Map.Entry::getValue)
            .collect(java.util.stream.Collectors.toList())) {
            if (NamePatterns.matches(pattern, entry.getName())) {
                matches.add(entry);
            }
        }

        matches.sort((a, b) -> NamePatterns.compareUtf8(a.getName(), b.getName()));
        matches.forEach(consumer);
    }

    /** Convenience for callers that genuinely want the whole answer in memory. */
    public List<IndexNameEntry> resolve(String pattern) {
        List<IndexNameEntry> found = new ArrayList<>();
        forEachMatching(pattern, found::add);
        return found;
    }

    /** Entries in the base, which is everything except changes since the last rebuild. */
    public int baseSize() {
        return state.base.size();
    }

    /** Pending changes not yet folded into the base. */
    public int pendingSize() {
        return state.overlay.size();
    }

    /** Both structures, since the reversed twin is a real cost and hiding it would understate sizing. */
    public long ramBytesUsed() {
        State current = state;
        return current.base.ramBytesUsed() + current.reversedBase.ramBytesUsed();
    }

    // --------------------------------------------------------------- writes

    public void create(String name, byte[] uuid, byte status) {
        create(new IndexNameEntry(name, uuid, status));
    }

    /** Records an alias naming a set of indices. */
    public void createAlias(String name, byte[] uuid, java.util.List<String> targets) {
        create(new IndexNameEntry(name, uuid, IndexNameEntry.STATUS_ALIAS, targets));
    }

    /**
     * Index names an alias points at, or an empty list if the name is not a known alias.
     *
     * <p>One level only: an alias naming another alias is not followed, because OpenSearch does not
     * allow it and following it would turn a malformed input into an unbounded walk.
     */
    public java.util.List<String> resolveAlias(String name) {
        IndexNameEntry entry = lookup(name);
        return entry != null && entry.isAlias() ? entry.getTargets() : java.util.List.of();
    }

    public void create(IndexNameEntry entry) {
        Objects.requireNonNull(entry, "entry");
        synchronized (writeLock) {
            state.overlay.put(entry);
        }
    }

    public void delete(String name) {
        Objects.requireNonNull(name, "name");
        synchronized (writeLock) {
            state.overlay.delete(name);
        }
    }

    /** Whether the overlay has grown enough to be worth folding into the base. */
    public boolean shouldRebuild() {
        State current = state;
        int pending = current.overlay.size();
        return pending >= rebuildFloor && pending >= current.base.size() * rebuildRatio;
    }

    /**
     * Folds the overlay into a new base and swaps.
     *
     * <p>Holds the write lock throughout, so writers block for the duration. Readers do not, because the
     * swap is a single assignment to a volatile field.
     *
     * <p>Peak memory holds two bases at once. At the 4.2 GiB measured for 100M names that transient is
     * about 8.4 GiB, which is a capacity planning input rather than a detail.
     */
    public void rebuild() {
        synchronized (writeLock) {
            State current = state;
            CompactNameIndexBuilder builder = new CompactNameIndexBuilder(current.base.size() + current.overlay.size());

            for (int ordinal = 0; ordinal < current.base.size(); ordinal++) {
                String name = current.base.nameAt(ordinal);
                // Skip anything the overlay supersedes, in either direction. A put is added below from
                // the overlay itself; adding it here too would trip the builder's duplicate check.
                if (current.overlay.covers(name)) {
                    continue;
                }
                // entryAt rather than the three-argument add: an alias's targets live in the side
                // arrays, and rebuilding through (name, uuid, status) would drop every one of them.
                builder.add(current.base.entryAt(ordinal));
            }
            for (IndexNameEntry entry : current.overlay.puts().values()) {
                builder.add(entry);
            }

            // Both structures are rebuilt from the same builder output. Producing one and forgetting the
            // other is the A11c trap: the index would answer prefix queries correctly and suffix queries
            // with stale data, which is worse than failing.
            CompactNameIndex rebuilt = builder.build();
            state = new State(rebuilt, reversedOf(rebuilt), new NameIndexOverlay());
        }
    }

    /** Rebuilds only if the policy says it is worth it. Returns whether it did. */
    public boolean maybeRebuild() {
        if (shouldRebuild() == false) {
            return false;
        }
        rebuild();
        return true;
    }
}
