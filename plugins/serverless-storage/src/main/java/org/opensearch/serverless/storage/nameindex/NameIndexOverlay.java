/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nameindex;

import java.util.Collections;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Creates and deletes since the last rebuild of the {@link CompactNameIndex} base.
 *
 * <p>The base packs every name into one contiguous sorted blob, so inserting into it would mean shifting
 * every byte after the insertion point. That is the trade the compact form makes, and this is the other
 * half of it: writes land here, reads consult both, and a rebuild periodically folds this back into a
 * new base.
 *
 * <h2>Deletes are tombstones, and have to be</h2>
 *
 * The base is immutable, so a delete cannot remove anything from it. The overlay therefore has to be
 * able to assert that a name is gone even when it holds no entry for that name itself. A delete that
 * merely dropped the name from {@link #puts} would leave a base entry visible, which is a deleted index
 * reappearing.
 *
 * <p>The two collections are kept disjoint by construction: a put clears any tombstone for that name and
 * a delete clears any put. Ordering within the overlay is the thing most likely to be got wrong, so it
 * is enforced here rather than left to callers.
 */
public final class NameIndexOverlay {

    /**
     * Sorted rather than hashed, in UTF-8 byte order matching {@link CompactNameIndex}.
     *
     * <p>A hash map made every pattern query walk the whole overlay. That is harmless while the overlay
     * is small, but the rebuild policy lets it reach a fraction of the base before folding, which at
     * 100M names is millions of entries scanned per wildcard -- in a structure whose entire purpose is
     * that cost tracks matches rather than population. Sorted, a prefix query is a range scan that stops
     * at the first non-match, and the merge no longer has to sort its candidates per query.
     */
    private final ConcurrentSkipListMap<String, IndexNameEntry> puts = new ConcurrentSkipListMap<>(NamePatterns::compareUtf8);

    private final ConcurrentSkipListSet<String> tombstones = new ConcurrentSkipListSet<>(NamePatterns::compareUtf8);

    /**
     * The same entries keyed by reversed name, so a suffix query can range-scan the overlay the way a
     * prefix query does.
     *
     * <p>Held here rather than in a second overlay object on purpose. The plan flagged "keep the forward
     * and reversed structures in step" as the trap in this work, because a create that reaches one and
     * not the other is visible to prefix queries and invisible to suffix ones. Putting both maps behind
     * one {@link #put} makes that divergence impossible rather than a thing to remember.
     */
    private final ConcurrentSkipListMap<String, IndexNameEntry> reversedPuts = new ConcurrentSkipListMap<>(NamePatterns::compareUtf8);

    /** Records a create or an update. Clears any tombstone, so a delete then create resolves to present. */
    public void put(IndexNameEntry entry) {
        Objects.requireNonNull(entry, "entry");
        // Tombstone first. The reverse order would leave a window in which a concurrent reader sees the
        // name in both collections and has to guess which wins.
        tombstones.remove(entry.getName());
        puts.put(entry.getName(), entry);
        reversedPuts.put(NamePatterns.reverse(entry.getName()), entry);

    }

    /**
     * Records a delete. Always leaves a tombstone, even for a name this overlay has never seen, because
     * the base may hold it and the overlay has no way to know.
     */
    public void delete(String name) {
        Objects.requireNonNull(name, "name");
        puts.remove(name);
        reversedPuts.remove(NamePatterns.reverse(name));
        tombstones.add(name);
    }

    /** The entry for a name, or null if this overlay says nothing about it. */
    public IndexNameEntry get(String name) {
        return puts.get(name);
    }

    /** Whether this overlay asserts the name is deleted, regardless of what the base holds. */
    public boolean isDeleted(String name) {
        return tombstones.contains(name);
    }

    /** Whether this overlay has an opinion about the name at all, in either direction. */
    public boolean covers(String name) {
        return puts.containsKey(name) || tombstones.contains(name);
    }

    public Map<String, IndexNameEntry> puts() {
        return Collections.unmodifiableMap(puts);
    }

    /**
     * Entries at or after {@code fromInclusive} in byte order. The caller stops when names stop matching
     * its prefix, which is what turns a full overlay walk into a range scan.
     */
    public NavigableMap<String, IndexNameEntry> putsFrom(String fromInclusive) {
        return puts.tailMap(fromInclusive, true);
    }

    /**
     * Entries whose <em>reversed</em> name is at or after {@code fromInclusive}. The caller stops when
     * reversed names stop matching its reversed suffix.
     */
    public NavigableMap<String, IndexNameEntry> reversedPutsFrom(String fromInclusive) {
        return reversedPuts.tailMap(fromInclusive, true);
    }

    public Set<String> tombstones() {
        return Collections.unmodifiableSet(tombstones);
    }

    /** Total pending changes, which is what the rebuild policy is measured against. */
    public int size() {
        return puts.size() + tombstones.size();
    }

    public boolean isEmpty() {
        return puts.isEmpty() && tombstones.isEmpty();
    }
}
