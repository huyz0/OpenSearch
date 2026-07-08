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

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The impure counterpart to {@link PitrRetentionPolicy}: reads a shard's current
 * {@value PitrRetentionPolicy#PITR_PIN_ID} pins, computes the diff against what the policy
 * requires right now, and applies it to a real {@link DurablePinRegistry}. Meant to be invoked
 * periodically (e.g. on a timer, once per shard) -- every call is idempotent, so calling it more
 * often than the window actually moves is wasted work, never a correctness problem.
 *
 * <p>Only ever touches {@value PitrRetentionPolicy#PITR_PIN_ID}-tagged pins: other pinning
 * reasons (snapshots) on the same shard are read (to compute {@code currentPitrPins} correctly)
 * but never added to or removed by this class.
 */
public final class PitrRetentionReconciler {

    private final DurablePinRegistry pinRegistry;

    public PitrRetentionReconciler(DurablePinRegistry pinRegistry) {
        this.pinRegistry = pinRegistry;
    }

    /** How many pins were added/removed, so callers/tests can observe what happened without re-reading the registry. */
    public record ReconcileResult(int added, int removed) {
    }

    public ReconcileResult reconcile(
        String indexUuid,
        int shardId,
        List<CommitManifest> manifestsForShard,
        long nowMillis,
        long windowMillis
    ) throws IOException {
        Set<PinRecord> currentPitrPins = new HashSet<>();
        for (PinRecord pin : pinRegistry.getPins(indexUuid, shardId)) {
            if (pin.pinId().equals(PitrRetentionPolicy.PITR_PIN_ID)) {
                currentPitrPins.add(pin);
            }
        }

        Set<ManifestId> required = PitrRetentionPolicy.computeRequiredPins(manifestsForShard, nowMillis, windowMillis);
        List<PinRecord> toAdd = PitrRetentionPolicy.pinsToAdd(required, currentPitrPins);
        List<PinRecord> toRemove = PitrRetentionPolicy.pinsToRemove(required, currentPitrPins);

        for (PinRecord pin : toAdd) {
            pinRegistry.addPin(indexUuid, shardId, pin);
        }
        for (PinRecord pin : toRemove) {
            pinRegistry.removePin(indexUuid, shardId, pin);
        }
        return new ReconcileResult(toAdd.size(), toRemove.size());
    }
}
