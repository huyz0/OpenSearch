/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

/** Outcome of {@link CompactionRebaseExecutor#publish}. */
public final class RebaseResult {

    /** Whether the compaction's manifest was published, abandoned voluntarily, or ran out of retries. */
    public enum Outcome {
        /** The compaction's manifest was successfully CAS'd onto the shard's head. */
        PUBLISHED,
        /** The publisher itself chose not to publish; no CAS was attempted. */
        ABANDONED,
        /** Every rebase-and-retry attempt lost the CAS race; the shard was left exactly as some other publisher left it. */
        EXHAUSTED
    }

    private final Outcome outcome;
    private final int attempts;

    private RebaseResult(Outcome outcome, int attempts) {
        this.outcome = outcome;
        this.attempts = attempts;
    }

    /**
     * The compaction's manifest was successfully published.
     *
     * @param attempts number of rebase attempts it took to publish
     */
    public static RebaseResult published(int attempts) {
        return new RebaseResult(Outcome.PUBLISHED, attempts);
    }

    /**
     * The publisher itself decided not to publish (e.g. the shard moved on and the merge is no longer worth publishing).
     *
     * @param attempts number of rebase attempts made before abandoning
     */
    public static RebaseResult abandoned(int attempts) {
        return new RebaseResult(Outcome.ABANDONED, attempts);
    }

    /**
     * Ran out of retries -- the shard is hot enough that some other publisher wins every race; the active owner merges instead.
     *
     * @param attempts number of rebase attempts made before giving up
     */
    public static RebaseResult exhausted(int attempts) {
        return new RebaseResult(Outcome.EXHAUSTED, attempts);
    }

    /** Returns whether the compaction was published, abandoned, or ran out of retries. */
    public Outcome outcome() {
        return outcome;
    }

    /** Returns the number of rebase attempts it took to reach this outcome. */
    public int attempts() {
        return attempts;
    }

    /** Returns whether the compaction's manifest was successfully published. */
    public boolean isPublished() {
        return outcome == Outcome.PUBLISHED;
    }

    @Override
    public String toString() {
        return "RebaseResult{outcome=" + outcome + ", attempts=" + attempts + '}';
    }
}
