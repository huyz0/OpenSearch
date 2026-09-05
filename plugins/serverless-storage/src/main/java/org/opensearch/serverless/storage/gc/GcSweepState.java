/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Everything one shard's GC sweep needs to remember between ticks.
 *
 * <h2>Why this is durable rather than a field on the task</h2>
 *
 * The sustained-orphan rule -- a bundle is deleted only once it has been observed unreferenced continuously
 * for a full retention window -- is the only thing standing between a racing publish and a deleted segment
 * file. Held in a {@code HashMap} field, that clock restarts from zero every time the node holding the
 * sweep restarts, the shard relocates, or the index is closed and reopened. Under autoscaling and spot
 * capacity those happen far more often than a thirty-minute window elapses, so the threshold is never
 * reached and <em>no bundle is ever deleted</em> -- silently, with no signal anywhere, leaking the full
 * merge/compaction write amplification of the shard forever. Persisting it makes the window mean elapsed
 * time rather than elapsed uptime of one particular process.
 *
 * <p>The same blob carries what the last sweep saw of the head and the pin register, which is what lets a
 * tick that can prove nothing has changed skip its two blob <em>listings</em> (priced in the object store's
 * write tier) in favour of the two register reads it had to make anyway.
 *
 * @param firstObservedOrphanedAtMillis per bundle name, when this shard's sweep first saw it unreferenced
 *        and has seen it unreferenced on every sweep since. Bounded by {@link #MAX_TRACKED_ORPHANS}.
 * @param lastSweptHeadTerm the {@code primaryTerm} of the head the last full sweep ran against, or 0 if none.
 * @param lastSweptHeadGeneration the generation of the head the last full sweep ran against, or 0 if none.
 * @param lastSweptPinsFingerprint an order-independent digest of the pin set the last full sweep saw, so a
 *        later tick can tell "the pins are exactly as they were" from "something pinned or unpinned".
 * @param lastFullSweepAtMillis when the last full sweep (one that actually listed) ran.
 */
public record GcSweepState(Map<String, Long> firstObservedOrphanedAtMillis, long lastSweptHeadTerm, long lastSweptHeadGeneration,
    long lastSweptPinsFingerprint, long lastFullSweepAtMillis) {

    /**
     * A hard cap on how many orphan observations one shard's state blob carries.
     *
     * <p>The map is naturally self-limiting -- an entry lives only from the tick a bundle is first seen
     * unreferenced to the tick it is deleted, one window later -- but "naturally" is not "always": a shard
     * whose deletes keep failing would grow it without bound, and this blob is read and written on every
     * sweep. Past the cap the sweep keeps the <em>oldest</em> observations, which are the ones closest to
     * becoming deletable, and simply re-observes the rest on a later tick.
     */
    public static final int MAX_TRACKED_ORPHANS = 10_000;

    /** The state of a shard that has never been swept: nothing observed, no head seen, no sweep yet. */
    public static GcSweepState empty() {
        return new GcSweepState(Map.of(), 0L, 0L, 0L, 0L);
    }

    /**
     * Canonicalises the observation map so a state read back from the store is never mutated in place by a
     * caller and never null.
     */
    public GcSweepState {
        Objects.requireNonNull(firstObservedOrphanedAtMillis, "firstObservedOrphanedAtMillis");
        firstObservedOrphanedAtMillis = Collections.unmodifiableMap(new java.util.LinkedHashMap<>(firstObservedOrphanedAtMillis));
    }

    /** The head identity the last full sweep ran against. */
    public ManifestId lastSweptHead() {
        return new ManifestId(lastSweptHeadTerm, lastSweptHeadGeneration);
    }
}
