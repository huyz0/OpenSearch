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

    /** All pins currently held on a shard, from every pinning reason (every snapshot, PITR, ...). */
    Set<PinRecord> getPins(String indexUuid, int shardId) throws IOException;

    /**
     * Convenience projection for feeding directly into
     * {@link org.opensearch.serverless.storage.gc.ManifestRetentionPolicy#computeDeletableManifests}.
     */
    default Set<ManifestId> getPinnedManifestIds(String indexUuid, int shardId) throws IOException {
        Set<PinRecord> pins = getPins(indexUuid, shardId);
        java.util.Set<ManifestId> ids = new java.util.HashSet<>(pins.size());
        for (PinRecord pin : pins) {
            ids.add(pin.toManifestId());
        }
        return ids;
    }

    /**
     * Adds a pin, idempotently: adding the same {@code pinId} again (e.g. a retried snapshot
     * request) is a no-op rather than an error. Safe under concurrent pin additions/removals from
     * other reasons on the same shard.
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
     */
    void removePin(String indexUuid, int shardId, String pinId) throws IOException;

    /**
     * Removes exactly the given pin record (matched on {@code pinId}, term, and generation
     * together), leaving any other pin with the same {@code pinId} on a different generation
     * untouched. A no-op if that exact pin was never present. Safe under concurrent pin
     * additions/removals from other reasons, or other generations of the same reason, on the same
     * shard.
     */
    void removePin(String indexUuid, int shardId, PinRecord pin) throws IOException;
}
