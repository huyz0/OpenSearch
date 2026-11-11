/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/**
 * One operation in a batch, with whatever condition the caller attached to it.
 *
 * <p><b>Why this type exists.</b> The batch path used to take {@link org.opensearch.serverless.store.WalRecord}
 * — the write-ahead <em>log</em> record — as its input type. A log record describes what happened, so
 * there was nowhere in it for a condition, which describes what must be true before anything happens.
 * That is the whole reason {@code _bulk} refused {@code create} and per-item {@code if_seq_no} after
 * the single-document path gained both: not a missing capability, a missing input type.
 */
public final class BulkOperation {
    private final org.opensearch.serverless.store.WalRecord record;
    private final long ifSeqNo;
    private final long ifPrimaryTerm;
    private final boolean requireAbsent;

    /**
     * Creates an operation carrying a condition.
     *
     * @param record what to write or delete
     * @param ifSeqNo the sequence number the document must be at, or unassigned when unconditional
     * @param ifPrimaryTerm the primary term the document must be at, or 0 when unconditional
     * @param requireAbsent whether the write must fail if the document already exists
     */
    public BulkOperation(org.opensearch.serverless.store.WalRecord record, long ifSeqNo, long ifPrimaryTerm, boolean requireAbsent) {
        this.record = record;
        this.ifSeqNo = ifSeqNo;
        this.ifPrimaryTerm = ifPrimaryTerm;
        this.requireAbsent = requireAbsent;
    }

    /**
     * An unconditional operation.
     *
     * @param record what to write or delete
     * @return the operation
     */
    public static BulkOperation of(org.opensearch.serverless.store.WalRecord record) {
        return new BulkOperation(record, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L, false);
    }

    /**
     * Returns what this operation writes or deletes.
     *
     * @return the record
     */
    public org.opensearch.serverless.store.WalRecord record() {
        return record;
    }

    /**
     * Returns the condition on the document's current sequence number, if any.
     *
     * @return the sequence number the document must be at, or unassigned
     */
    public long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * Returns the condition on the document's current primary term, if any.
     *
     * @return the primary term the document must be at, or 0
     */
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    /**
     * Returns whether this operation is a create rather than an overwrite.
     *
     * @return whether the document must not already exist
     */
    public boolean requireAbsent() {
        return requireAbsent;
    }
}
