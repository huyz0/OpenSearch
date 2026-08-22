/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.manifest.CommitManifest;

import java.util.List;
import java.util.Objects;

/**
 * The manifest a restore publishes, which is what makes a restore survive recovery.
 *
 * <h2>The defect this exists to fix</h2>
 *
 * A restore used to compare-and-swap the shard head <em>backwards</em> onto the target generation. That is
 * the obvious implementation and it does not survive reopening the index, which is the only test that
 * decides whether a restore is real.
 *
 * <p>Two things undo it, and they are worth keeping separate because only one of them is the head's fault:
 *
 * <ul>
 * <li><b>The local store.</b> Reopening on a node that still holds the pre-restore Lucene files recovers
 *     from those files and never consults the object store, so the restore is invisible to recovery. That
 *     is fixed elsewhere -- {@code EngineFactory#localStoreIsStale}, which lets an engine whose authority
 *     lives in the object store say its local copy is out of date. <b>This is the half that measurably
 *     carries the fix</b>: the reopen test fails without it.</li>
 * <li><b>The replay floor.</b> {@code ObjectStoreCommitHeadPublisher#readLatestManifest} resolves the head,
 *     and {@code ObjectStoreWriterEngine#replayWalOperations} takes that manifest's
 *     {@link org.opensearch.serverless.storage.manifest.WalPosition} as the floor to replay from. Rewinding
 *     the head therefore rewinds the floor too, and WAL replay -- which exists to reapply writes a manifest
 *     does not yet carry, and cannot tell "not yet published" from "deliberately rolled back" -- would
 *     faithfully put the rolled-back writes back. Publishing the newest manifest's WAL position is what
 *     stops that. <b>No test here demonstrates it</b>: with WAL mirroring on, reverting that one field
 *     leaves the reopen test green, and so does leaving the second write unflushed. See
 *     {@code ServerlessStorageRestoreToInstantIT#testARestoreSurvivesReopeningWhenWalMirroringIsOn} for what
 *     was measured. It is kept because advertising a replay floor beneath a range you have decided not to
 *     replay is wrong by construction, not because it was caught being wrong.</li>
 * </ul>
 *
 * <h2>The shape that fixes it</h2>
 *
 * A restore publishes a <em>new</em> generation rather than reusing an old one. The new manifest carries:
 *
 * <ul>
 * <li><b>the target's segment content</b> -- its file map, segments file, sequence numbers, mapping version
 *     and doc counts. This is what "restore" means, and it is copied rather than referenced so the new
 *     manifest names every restored file itself (see below).</li>
 * <li><b>the newest manifest's WAL position</b> -- the assertion "everything in the WAL up to here is
 *     accounted for; do not replay it", so replay's floor sits ahead of the rolled-back writes rather than
 *     behind them. See the caveat above about what this is and is not known to prevent.</li>
 * </ul>
 *
 * <p>So a restore moves the head <em>forward</em>, like every other publication. Three things follow, and
 * each of them was a latent problem with the rewind:
 *
 * <ul>
 * <li>Head generations stay monotonic, which {@code ShardHead#withPublishedGeneration} enforces and the
 *     rewind had to deliberately bypass. Nothing has to bypass it any more.</li>
 * <li>A restore is itself a point on the timeline, so it can be restored past -- an operator who restores
 *     to the wrong instant can undo it, which a rewind made impossible because it left no record.</li>
 * <li>Garbage collection keeps the restored files because the live head manifest names them directly. A
 *     manifest that merely pointed at the target generation would depend on the pin outliving the restore;
 *     copying the file map means releasing the pin afterwards is safe.</li>
 * </ul>
 *
 * <h2>What it deliberately does not carry forward</h2>
 *
 * The newest manifest's sequence numbers. Restoring means the shard's seqno timeline goes back to the
 * target's, so numbers issued after it are issued again -- which is inherent to an in-place rollback, not an
 * artefact of this: the writes that held them are gone.
 */
public final class RestoreManifestSynthesis {

    private RestoreManifestSynthesis() {}

    /**
     * The manifest to publish for a restore to {@code target}.
     *
     * @param target the generation being restored to, whose segment content the result carries.
     * @param newest the newest manifest the shard has, whose WAL position the result carries so that replay
     *               does not reapply what the restore just rolled back. Passing {@code target} itself here
     *               is the degenerate "restore to the newest generation" case and is harmless.
     * @param primaryTerm the term to publish under -- the current head's, not the target's, because
     *                    publishing under an older term than the head already records is exactly what
     *                    {@code ShardHead#withPublishedGeneration} refuses.
     * @param generation the generation to publish at, which must be greater than every generation the shard
     *                   already has (see {@link #nextGeneration}).
     * @param nowMillis the instant to stamp the new manifest with, so a later point-in-time restore resolves
     *                  it at the time the restore happened rather than at the time the data was written.
     */
    public static CommitManifest restoredManifest(
        CommitManifest target,
        CommitManifest newest,
        long primaryTerm,
        long generation,
        long nowMillis
    ) {
        Objects.requireNonNull(target, "target");
        Objects.requireNonNull(newest, "newest");
        if (generation <= newest.generation()) {
            throw new IllegalArgumentException(
                "a restore must publish forward: generation " + generation + " must be > the newest generation " + newest.generation()
            );
        }
        if (primaryTerm < target.primaryTerm()) {
            throw new IllegalArgumentException(
                "cannot publish a restore under term "
                    + primaryTerm
                    + ", which is older than the restored generation's own term "
                    + target.primaryTerm()
            );
        }
        return new CommitManifest(
            target.indexUuid(),
            target.shardId(),
            primaryTerm,
            generation,
            target.segmentsFileName(),
            target.files(),
            target.maxSeqNo(),
            target.localCheckpoint(),
            // The one field taken from the newest manifest rather than the target, so that replay's floor
            // sits above the rolled-back writes rather than beneath them. See the class javadoc for what
            // this is, and is not, known to prevent.
            newest.walPosition(),
            target.mappingVersion(),
            target.pruningStats(),
            nowMillis,
            target.totalDocCount(),
            target.deletedDocCount()
        );
    }

    /**
     * The newest manifest in {@code manifests} by {@code (primaryTerm, generation)}, which is the one whose
     * WAL position a restore must adopt.
     *
     * <p>Ordered by generation first and term second, matching {@link PitrRestoreResolution}'s tie-break, so
     * both agree on what "newest" means. A shard with no manifests at all has nothing to restore and is
     * refused by the caller before this is reached.
     */
    public static CommitManifest newest(List<CommitManifest> manifests) {
        CommitManifest newest = null;
        for (CommitManifest manifest : manifests) {
            if (newest == null
                || manifest.generation() > newest.generation()
                || (manifest.generation() == newest.generation() && manifest.primaryTerm() > newest.primaryTerm())) {
                newest = manifest;
            }
        }
        if (newest == null) {
            throw new IllegalArgumentException("no manifests to restore from");
        }
        return newest;
    }

    /**
     * The first generation no manifest is using.
     *
     * <p>Taken from the whole manifest list rather than from the head, because the head may legitimately lag
     * behind the newest manifest on disk, and publishing onto a generation that already exists would
     * overwrite somebody else's commit with a copy of an older one -- the worst outcome available here.
     */
    public static long nextGeneration(List<CommitManifest> manifests) {
        long highest = -1;
        for (CommitManifest manifest : manifests) {
            highest = Math.max(highest, manifest.generation());
        }
        return highest + 1;
    }
}
