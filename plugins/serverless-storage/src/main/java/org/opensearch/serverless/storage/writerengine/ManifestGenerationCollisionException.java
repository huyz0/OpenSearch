/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import java.io.IOException;

/**
 * A different actor already occupies the {@code (primaryTerm, generation)} slot this publish was
 * targeting, with content that is not this caller's own -- i.e. a <em>lost race</em>, not an error.
 *
 * <p>This exists because the condition it names used to be reported as a bare {@link IOException}
 * carrying a prose message, and that had a real consequence: a writer flush and a compaction publish
 * both target {@code currentHead.latestManifestGeneration() + 1} under the same term, so they
 * routinely compute the same manifest name. {@code ObjectStoreCommitHeadPublisher}'s retry loop
 * correctly handles a lost <em>head CAS</em>, but this collision happens strictly earlier, inside
 * {@code ObjectStoreCommitPublisher#publishCommit}, so the {@code IOException} escaped the loop
 * entirely and reached {@code ObjectStoreWriterEngine#commitIndexWriter}'s catch-all, which called
 * {@code failEngine}. A benign, fully expected race between a flush and a compaction therefore
 * killed the primary.
 *
 * <p>Giving the condition its own type is what lets the retry loop catch exactly it -- re-read the
 * head, recompute a fresh target generation, and try again -- without also swallowing genuine I/O
 * failures, which must still propagate. See {@code ObjectStoreCommitHeadPublisher#publishCommitAsHeadReturningManifest}.
 */
public class ManifestGenerationCollisionException extends IOException {

    private final long primaryTerm;
    private final long generation;

    /**
     * @param primaryTerm the primary term whose generation slot was already occupied
     * @param generation the generation that was already occupied by foreign content
     * @param detail what specifically was found to collide (a manifest, or the bundle name it would have used)
     */
    public ManifestGenerationCollisionException(long primaryTerm, long generation, String detail) {
        this(primaryTerm, generation, detail, null);
    }

    /**
     * @param primaryTerm the primary term whose generation slot was already occupied
     * @param generation the generation that was already occupied by foreign content
     * @param detail what specifically was found to collide (a manifest, or the bundle name it would have used)
     * @param cause the underlying store-level failure that revealed the collision, or {@code null}
     */
    public ManifestGenerationCollisionException(long primaryTerm, long generation, String detail, Throwable cause) {
        super(
            "generation slot (primaryTerm="
                + primaryTerm
                + ", generation="
                + generation
                + ") is already occupied by foreign content -- "
                + detail
                + "; this is a lost publication race, not a failure",
            cause
        );
        this.primaryTerm = primaryTerm;
        this.generation = generation;
    }

    /** The primary term whose generation slot was already occupied. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** The generation that was already occupied by foreign content. */
    public long generation() {
        return generation;
    }
}
