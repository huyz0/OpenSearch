/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The commits a writer has published but not yet written to its shard's manifest register.
 *
 * <p>With staging on, a routine publish uploads its segment files as usual and keeps its manifest here instead of
 * swapping the register: one PUT fewer for every shard on every publish. The register is written as a checkpoint: on
 * the first publish of a term, which is the fence; on any publish that is not routine (a release, an open's replay, a
 * split); and once the oldest staged commit is {@code maxStagedMillis} old. Readers learn of the commits in between
 * from the writer, which hands them the newest one here.
 *
 * <p><b>Why a staged commit loses nothing.</b> The write log is trimmed only behind a commit the register holds, so
 * everything a staged commit holds is either in the register or still in the log. A successor restores the register
 * and replays the log, as it does after any crash between two publishes.
 *
 * <p><b>Why its files stay.</b> Shard collection deletes only blobs of terms older than the register's, and a
 * staged commit is of the register's term. Node pack collection deletes a pack no manifest has named for
 * {@link NodePackWriter#COLLECTABLE_AFTER_MILLIS}, and the register is written within
 * {@link NodePackWriter#SWAP_DEADLINE_MILLIS} of the oldest pack a staged commit names, or the commit is dropped and
 * its files are uploaded again by the next publish.
 *
 * <p>Not thread safe on its own: its publisher's callers serialise publishes of one shard.
 */
public final class CommitStaging {

    /** The default age of the oldest staged commit at which the register is written: well inside the pack deadline. */
    public static final long DEFAULT_MAX_STAGED_MILLIS = 150_000L;

    private final long maxStagedNanos;
    private final LongSupplier nanoClock;

    private volatile CommitManifest staged;
    /** When the oldest node pack a staged commit names landed, or when the first staged commit was made. */
    private volatile long oldestNanos;
    private long sequence;

    /**
     * Creates staging that writes the register once the oldest staged commit is this old.
     *
     * @param maxStagedMillis the age; positive and below {@link NodePackWriter#SWAP_DEADLINE_MILLIS}
     */
    public CommitStaging(long maxStagedMillis) {
        this(maxStagedMillis, System::nanoTime);
    }

    CommitStaging(long maxStagedMillis, LongSupplier nanoClock) {
        if (maxStagedMillis <= 0L || maxStagedMillis >= NodePackWriter.SWAP_DEADLINE_MILLIS) {
            throw new IllegalArgumentException(
                "a commit may be staged for more than 0 and less than "
                    + NodePackWriter.SWAP_DEADLINE_MILLIS
                    + " ms, got "
                    + maxStagedMillis
            );
        }
        this.maxStagedNanos = TimeUnit.MILLISECONDS.toNanos(maxStagedMillis);
        this.nanoClock = nanoClock;
    }

    /**
     * Returns the newest staged commit, unless it is too old to be written to the register any more.
     *
     * @return the commit, or null
     */
    public CommitManifest staged() {
        final CommitManifest commit = staged;
        return commit == null || pastDeadline() ? null : commit;
    }

    /** Whether the oldest staged commit names a pack that may already be judged unreferenced once written. */
    boolean pastDeadline() {
        return staged != null && nanoClock.getAsLong() - oldestNanos > TimeUnit.MILLISECONDS.toNanos(NodePackWriter.SWAP_DEADLINE_MILLIS);
    }

    /** Whether the oldest staged commit is old enough that the register should be written now. */
    public boolean due() {
        return staged != null && nanoClock.getAsLong() - oldestNanos >= maxStagedNanos;
    }

    /**
     * Returns what a publish inherits from: the newest staged commit when it follows on from the register this writer
     * last wrote, in the same term, else null. A staged commit past its deadline is dropped here, so its files are
     * uploaded again rather than named in a register they may not outlive.
     */
    CommitManifest inheritFrom(CommitManifest register, long term, String writerId) {
        if (pastDeadline()) {
            clear();
            return null;
        }
        final CommitManifest commit = staged;
        if (commit == null || register == null || follows(register, term, writerId) == false) {
            return null;
        }
        return commit;
    }

    /** Whether a publish may stage over the register this writer last wrote, rather than write it. */
    boolean mayStage(CommitManifest register, long term, String writerId) {
        return register != null && follows(register, term, writerId) && due() == false;
    }

    private static boolean follows(CommitManifest register, long term, String writerId) {
        return register.term() == term && writerId != null && writerId.equals(register.writer());
    }

    /** The next sequence for a commit of this writer. */
    long next() {
        return ++sequence;
    }

    /** Records a commit staged rather than written; {@code packLandedNanos} is its own pack's landing, or null. */
    void stage(CommitManifest commit, Long packLandedNanos) {
        final long landed = packLandedNanos == null ? nanoClock.getAsLong() : packLandedNanos;
        if (staged == null) {
            oldestNanos = landed;
        } else {
            oldestNanos = Math.min(oldestNanos, landed);
        }
        staged = commit;
    }

    /** Forgets what was staged: the register now holds it, or a newer commit, or it was dropped. */
    void clear() {
        staged = null;
    }
}
