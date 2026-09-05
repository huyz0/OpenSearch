/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;

/**
 * Everything an {@code ObjectStoreWriterEngine} needs to schedule its own
 * {@link PitrRetentionSchedulerTask}, bundled into one value so PITR support can be threaded
 * through as a single optional (nullable) constructor parameter instead of three. {@code null}
 * where this type is accepted means "PITR retention is not configured for this shard" -- matches
 * how {@code encryptionKeyProvider} being {@code null} means "encryption is off" elsewhere in this
 * plugin: an explicit absence, not a missing feature.
 *
 * <h2>What the window actually guarantees, and what it does not</h2>
 *
 * <b>Recovery is commit-granular, not instant-granular.</b> Within {@code windowMillis} this keeps every
 * manifest and every bundle those manifests reference, so a restore to an instant T resolves to the newest
 * commit at or before T -- not to T itself. The resolution of "point in time" is therefore the shard's
 * publication cadence, and anything written between the last commit before T and T is not recoverable.
 *
 * <p>The RFC's &sect;14 describes a stronger promise: keep the manifests, the bundles, <em>and all WAL
 * chunks</em>, and replay the WAL from the newest manifest at or before T forward to T. Neither half of that
 * is built. {@code WalGcSchedulerTask} computes its safe-to-delete bound purely from "has every registered
 * shard published past this sequence" and has no notion of a PITR window at all; and the restore path
 * deliberately does the opposite of replaying, adopting the <em>newest</em> manifest's WAL position (see
 * {@link RestoreManifestSynthesis}) specifically to fence replay off the rolled-back range. That is the
 * right call for an in-place rollback and it is why "restore to the last commit &le; T" is what this
 * delivers. Written down here rather than left to be discovered at recovery time.
 *
 * <p>Closing the gap needs two things that are outside this record: {@code WalGcSchedulerTask}'s bound
 * becoming {@code min(published-past bound, oldest PITR-pinned manifest's WAL position)}, and a
 * sequence-number-bounded replay on the restore path. {@link PitrRestoreResolution}'s own javadoc names the
 * remaining blocker for true instant precision: {@code WalRecord} carries no timestamp, so "replay forward
 * to T" has nothing to compare T against.
 */
public record PitrRetentionConfig(BlobContainerManifestStore manifestStore, DurablePinRegistry pinRegistry, long windowMillis) {

    /**
     * Validates the PITR retention configuration.
     *
     * @param manifestStore the store used to list a shard's manifests during reconciliation.
     * @param pinRegistry   the registry PITR pins are added to/removed from.
     * @param windowMillis  how far back point-in-time recovery must be possible; must be > 0.
     */
    public PitrRetentionConfig {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be > 0, got " + windowMillis);
        }
    }

    /** The store used to list a shard's manifests during reconciliation. */
    @Override
    public BlobContainerManifestStore manifestStore() {
        return manifestStore;
    }

    /** The registry PITR pins are added to/removed from. */
    @Override
    public DurablePinRegistry pinRegistry() {
        return pinRegistry;
    }

    /** How far back point-in-time recovery must be possible. */
    @Override
    public long windowMillis() {
        return windowMillis;
    }
}
