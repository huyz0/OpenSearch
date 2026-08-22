/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.gc.ManifestId;

import java.io.IOException;
import java.util.Set;

/**
 * Tracks durable retention pins per shard (rfc-serverless-opensearch.md &sect;6.5/&sect;14):
 * snapshots and PITR policy each pin specific manifest generations so
 * {@link org.opensearch.serverless.storage.gc.ManifestRetentionPolicy} never judges them
 * deletable, independent of whether any lease-holding node is still alive.
 */
public interface DurablePinRegistry {

    /**
     * All pins currently held on a shard, from every pinning reason (every snapshot, PITR, ...).
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId   the shard to look up pins for.
     */
    Set<PinRecord> getPins(String indexUuid, int shardId) throws IOException;

    /**
     * Convenience projection for feeding directly into
     * {@link org.opensearch.serverless.storage.gc.ManifestRetentionPolicy#computeDeletableManifests}.
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId   the shard to look up pinned manifest IDs for.
     */
    default Set<ManifestId> getPinnedManifestIds(String indexUuid, int shardId) throws IOException {
        return getPinnedManifestIds(indexUuid, shardId, System.currentTimeMillis());
    }

    /**
     * The generations pinned at a given instant -- expired pins excluded.
     *
     * <p><b>This is the single place expiry has to be honoured, and it is why the expiry is not decorative.</b>
     * Garbage collection asks this question and only this question; a pin whose expiry has passed stops
     * answering it, so the generation it was holding becomes collectable without anyone having to remove the
     * record. That is what turns a coordinator dying mid-pin from a permanent leak into a bounded one.
     *
     * <p>The instant is a parameter rather than a clock read so that every pin in one sweep is judged
     * against the same moment, and so the behaviour can be asserted without waiting for wall time.
     */
    default Set<ManifestId> getPinnedManifestIds(String indexUuid, int shardId, long nowMillis) throws IOException {
        Set<PinRecord> pins = getPins(indexUuid, shardId);
        java.util.Set<ManifestId> ids = new java.util.HashSet<>(pins.size());
        for (PinRecord pin : pins) {
            if (pin.isLiveAt(nowMillis)) {
                ids.add(pin.toManifestId());
            }
        }
        return ids;
    }

    /**
     * Re-stamps every pin under this id with a new expiry, and does nothing if there is none.
     *
     * <p>What the second phase of taking an index-wide pin calls. Pins go down with a short expiry, and only
     * once every shard has one does anything make them permanent -- so a coordinator that stops halfway
     * leaves pins that lapse on their own rather than pins that must be found and removed. The failure mode
     * inverts: previously an interrupted pin held generations forever, and now an interrupted pin releases
     * them, which is the direction to fail in for something whose job is to stop deletion.
     *
     * @param indexUuid       the index whose shard is pinned.
     * @param shardId         the shard within it.
     * @param pinId           which pin to re-stamp.
     * @param expiresAtMillis the new expiry, or {@link PinRecord#NEVER_EXPIRES} to make it permanent.
     */
    default void confirmPin(String indexUuid, int shardId, String pinId, long expiresAtMillis) throws IOException {
        throw new UnsupportedOperationException("this registry cannot re-stamp a pin's expiry");
    }

    /**
     * Adds a pin, idempotently: adding the same {@code pinId} again (e.g. a retried snapshot
     * request) is a no-op rather than an error. Safe under concurrent pin additions/removals from
     * other reasons on the same shard.
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId   the shard to add the pin to.
     * @param pin       the pin to add.
     */
    void addPin(String indexUuid, int shardId, PinRecord pin) throws IOException;

    /**
     * Removes every pin with the given {@code pinId} (e.g. a snapshot was deleted). A no-op if
     * that pin was never present. Safe under concurrent pin additions/removals from other reasons
     * on the same shard.
     *
     * <p><b>Only safe when {@code pinId} names exactly one generation</b> (the common case: one
     * snapshot pins one generation per shard). A reason that pins <em>several</em> generations at
     * once under the same {@code pinId} -- PITR does exactly this, one pin per manifest inside the
     * retention window -- must use {@link #removePin(String, int, PinRecord)} instead to remove one
     * generation's pin without wiping out every other generation sharing that reason.
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId   the shard to remove the pin from.
     * @param pinId     the pin ID whose pins should all be removed.
     */
    void removePin(String indexUuid, int shardId, String pinId) throws IOException;

    /**
     * Removes exactly the given pin record (matched on {@code pinId}, term, and generation
     * together), leaving any other pin with the same {@code pinId} on a different generation
     * untouched. A no-op if that exact pin was never present. Safe under concurrent pin
     * additions/removals from other reasons, or other generations of the same reason, on the same
     * shard.
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId   the shard to remove the pin from.
     * @param pin       the exact pin to remove.
     */
    void removePin(String indexUuid, int shardId, PinRecord pin) throws IOException;

    /**
     * Atomically makes {@code newPin} the <em>only</em> pin under {@code newPin.pinId()}: adds it
     * and removes every other pin sharing that same {@code pinId} (a different generation), all
     * within one CAS mutation -- the create-or-replace, single-generation-per-reason contract
     * {@code SnapshotPinAction}'s own javadoc describes ("re-running a snapshot under the same
     * name updates it to the shard's current state rather than accumulating every generation ever
     * pinned").
     *
     * <p>Unlike calling {@link #addPin} followed by a separate read-then-{@link #removePin(String,
     * int, PinRecord)} loop, this is safe under two <em>concurrent</em> calls for the same {@code
     * pinId} (e.g. a client retry racing the original request): each call only ever removes pins
     * that are not the one it just added, computed from the same atomically-read-and-written state
     * its own add used -- two racing calls converge on whichever wins the last CAS, never both
     * ending up with zero pins for that {@code pinId}.
     *
     * @param indexUuid the UUID of the index the shard belongs to.
     * @param shardId   the shard to update the pin on.
     * @param newPin    the pin that should become the sole pin under its {@code pinId}.
     */
    void replacePin(String indexUuid, int shardId, PinRecord newPin) throws IOException;
}
