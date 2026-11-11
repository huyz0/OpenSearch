/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/**
 * What one operation in a batch did.
 *
 * <p>A plain class rather than a record because this module's javadoc check rejects records.
 */
public final class BulkOutcome {

    private final String id;
    private final boolean deletion;
    private final boolean found;
    private final String failure;
    private boolean conflict;
    private boolean retryable;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;
    private final boolean created;

    private BulkOutcome(
        String id,
        boolean deletion,
        boolean found,
        String failure,
        long seqNo,
        long primaryTerm,
        long version,
        boolean created
    ) {
        this.id = id;
        this.deletion = deletion;
        this.found = found;
        this.failure = failure;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.created = created;
    }

    /**
     * Records a document written, with the sequence identity the engine assigned it.
     *
     * @param id the document id
     * @param seqNo the sequence number assigned
     * @param primaryTerm the primary term it ran at
     * @param version the version assigned
     * @param created whether the document did not exist before this write
     * @return the outcome
     */
    public static BulkOutcome indexed(String id, long seqNo, long primaryTerm, long version, boolean created) {
        return new BulkOutcome(id, false, false, null, seqNo, primaryTerm, version, created);
    }

    /**
     * Records a deletion, which may or may not have found anything.
     *
     * @param id the document id
     * @param found whether the document was there
     * @param seqNo the sequence number assigned
     * @param primaryTerm the primary term it ran at
     * @param version the version assigned
     * @return the outcome
     */
    public static BulkOutcome deleted(String id, boolean found, long seqNo, long primaryTerm, long version) {
        return new BulkOutcome(id, true, found, null, seqNo, primaryTerm, version, false);
    }

    /**
     * Records an operation the engine refused.
     *
     * @param id the document id
     * @param reason why it failed
     * @return the outcome
     */
    public static BulkOutcome conflicted(String id, String reason) {
        final BulkOutcome outcome = failed(id, reason);
        outcome.conflict = true;
        return outcome;
    }

    /**
     * Whether this item failed because a condition it carried was not met.
     *
     * <p>Separated from any other failure because it is the one a caller asked a question with: a lost
     * compare-and-swap is a 409 and must not be retried blindly, while an ordinary item failure is a
     * 400 and usually can be.
     *
     * @return whether this was a version conflict
     */
    public boolean conflict() {
        return conflict;
    }

    /**
     * Records an operation that failed for a reason that will not recur if the caller simply tries
     * again: the index's mapping was being grown by someone else at the same moment.
     *
     * @param id the document id
     * @param reason why it failed
     * @return the outcome
     */
    public static BulkOutcome retryable(String id, String reason) {
        final BulkOutcome outcome = failed(id, reason);
        outcome.retryable = true;
        return outcome;
    }

    /**
     * Whether this item failed on contention rather than on its own content.
     *
     * <p>A 503 rather than a 400: nothing about the document was wrong, and a client that retries the
     * item gets it written. Separated from {@link #conflict()} because a lost condition must not be
     * retried blindly and this must.
     *
     * @return whether the caller should retry the item as it is
     */
    public boolean retryable() {
        return retryable;
    }

    /**
     * An outcome for an item that could not be applied.
     *
     * @param id the document id
     * @param reason what went wrong, as the caller should see it
     * @return the outcome
     */
    public static BulkOutcome failed(String id, String reason) {
        return new BulkOutcome(
            id,
            false,
            false,
            reason == null ? "unknown failure" : reason,
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
            org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            org.opensearch.common.lucene.uid.Versions.NOT_FOUND,
            false
        );
    }

    /**
     * Returns the sequence number the engine assigned.
     *
     * @return the sequence number, unassigned for a failed item
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term the operation ran at.
     *
     * @return the primary term, unassigned for a failed item
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the version the engine assigned.
     *
     * @return the version
     */
    public long version() {
        return version;
    }

    /**
     * Reports whether a write created the document rather than overwriting one.
     *
     * @return true when the document did not exist before
     */
    public boolean created() {
        return created;
    }

    /**
     * Returns the document id.
     *
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Reports whether this operation removed a document rather than adding one.
     *
     * @return true for a deletion
     */
    public boolean isDeletion() {
        return deletion;
    }

    /**
     * Reports whether a deletion found the document. Meaningless for a write.
     *
     * @return true when the document was there to delete
     */
    public boolean found() {
        return found;
    }

    /**
     * Returns why the operation failed, or null if it did not.
     *
     * @return the failure message, or null
     */
    public String failure() {
        return failure;
    }
}
