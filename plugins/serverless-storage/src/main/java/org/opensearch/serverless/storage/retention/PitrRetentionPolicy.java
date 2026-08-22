/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.gc.ManifestId;
import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides which manifest generations must currently carry a {@value #PITR_PIN_ID} durable pin
 * (rfc-serverless-opensearch.md &sect;6.5/&sect;14): the piece that was previously missing --
 * {@link DurablePinRegistry} could always <em>hold</em> a pin, but nothing decided <em>when</em>
 * to add or remove one for point-in-time recovery specifically.
 *
 * <p>Every manifest created within the last {@code windowMillis} must be pinned, so a restore
 * request for any timestamp inside the window can be served exactly (each manifest answers
 * queries from its own creation time up to the next manifest's). One additional manifest -- the
 * most recent one created <em>before</em> the window -- must also stay pinned: without it, a
 * restore request for a timestamp right at the window's edge (before the oldest in-window
 * manifest, but still meant to be servable at the instant the window boundary was computed) would
 * have nothing to resolve to.
 *
 * <p>Pure functions only: no I/O, no clock reads, no registry mutation -- {@code nowMillis} is a
 * parameter, not read from the system clock, so this class is exactly as testable as {@link
 * org.opensearch.serverless.storage.gc.ManifestRetentionPolicy}, which it deliberately mirrors in
 * shape. {@link PitrRetentionReconciler} is the impure counterpart that actually calls a {@link
 * DurablePinRegistry}.
 */
public final class PitrRetentionPolicy {

    /** The {@link PinRecord#pinId()} used for every pin this class computes. */
    public static final String PITR_PIN_ID = "pitr";

    private PitrRetentionPolicy() {}

    /**
     * Computes every manifest that must currently carry a {@value #PITR_PIN_ID} pin.
     *
     * @param manifestsForOneShard all known manifests belonging to exactly one (index, shard).
     * @param nowMillis            the instant to evaluate the window against.
     * @param windowMillis         how far back point-in-time recovery must be possible; must be > 0.
     * @return every manifest ID that must currently carry a {@value #PITR_PIN_ID} pin.
     */
    public static Set<ManifestId> computeRequiredPins(List<CommitManifest> manifestsForOneShard, long nowMillis, long windowMillis) {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be > 0, got " + windowMillis);
        }
        long cutoffMillis = nowMillis - windowMillis;

        Set<ManifestId> required = new HashSet<>();
        CommitManifest latestBeforeCutoff = null;
        for (CommitManifest manifest : manifestsForOneShard) {
            if (manifest.createdAtMillis() >= cutoffMillis) {
                required.add(ManifestId.of(manifest));
            } else if (isLaterThan(manifest, latestBeforeCutoff)) {
                latestBeforeCutoff = manifest;
            }
        }
        if (latestBeforeCutoff != null) {
            required.add(ManifestId.of(latestBeforeCutoff));
        }
        return required;
    }

    /**
     * Whether {@code candidate} should replace {@code current} as the "latest before cutoff" pick.
     * Breaks a {@code createdAtMillis} tie deterministically by (primaryTerm, generation) --
     * {@code manifestsForOneShard}'s own iteration order comes from an unordered blob-listing
     * {@code Map}, so without a secondary key, two manifests sharing a millisecond-precision
     * timestamp could make consecutive reconciliation calls flip which one is picked, needlessly
     * churning the pin (add one, remove the other) even though both are equally valid answers for
     * PITR purposes. A higher (primaryTerm, generation) is later in the shard's real commit order
     * regardless of what the clock happened to read at either commit.
     */
    private static boolean isLaterThan(CommitManifest candidate, CommitManifest current) {
        if (current == null) {
            return true;
        }
        if (candidate.createdAtMillis() != current.createdAtMillis()) {
            return candidate.createdAtMillis() > current.createdAtMillis();
        }
        if (candidate.primaryTerm() != current.primaryTerm()) {
            return candidate.primaryTerm() > current.primaryTerm();
        }
        return candidate.generation() > current.generation();
    }

    /**
     * Pins that must be added to bring {@code currentPitrPins} in line with {@code requiredPins}.
     *
     * @param requiredPins     every manifest ID that must currently carry a {@value #PITR_PIN_ID} pin.
     * @param currentPitrPins  the {@value #PITR_PIN_ID} pins currently held on the shard.
     */
    public static List<PinRecord> pinsToAdd(Set<ManifestId> requiredPins, Set<PinRecord> currentPitrPins) {
        Set<ManifestId> alreadyPinned = new HashSet<>();
        for (PinRecord pin : currentPitrPins) {
            alreadyPinned.add(pin.toManifestId());
        }
        List<PinRecord> toAdd = new ArrayList<>();
        for (ManifestId id : requiredPins) {
            if (alreadyPinned.contains(id) == false) {
                toAdd.add(new PinRecord(PITR_PIN_ID, id.primaryTerm(), id.generation()));
            }
        }
        return toAdd;
    }

    /**
     * Pins that must be removed because the window has rolled past them.
     *
     * @param requiredPins     every manifest ID that must currently carry a {@value #PITR_PIN_ID} pin.
     * @param currentPitrPins  the {@value #PITR_PIN_ID} pins currently held on the shard.
     */
    public static List<PinRecord> pinsToRemove(Set<ManifestId> requiredPins, Set<PinRecord> currentPitrPins) {
        List<PinRecord> toRemove = new ArrayList<>();
        for (PinRecord pin : currentPitrPins) {
            if (requiredPins.contains(pin.toManifestId()) == false) {
                toRemove.add(pin);
            }
        }
        return toRemove;
    }
}
