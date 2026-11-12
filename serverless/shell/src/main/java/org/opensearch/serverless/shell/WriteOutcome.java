/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

/**
 * What the engine assigned one single-document write or deletion.
 *
 * <p>These are the numbers real OpenSearch returns as {@code _seq_no}, {@code _primary_term} and
 * {@code _version}. They were always being generated here — the engine is core's own and assigns them
 * on every operation — and were simply discarded at this boundary. Returning them is what lets a
 * caller hold one and send it back as {@code if_seq_no}.
 *
 * <p>A plain class rather than a record because this module's javadoc check rejects records.
 */
public final class WriteOutcome {

    private final long seqNo;
    private final long primaryTerm;
    private final long version;
    private final boolean created;
    private final boolean found;
    private final WriteHold.Target forwardedTo;

    /**
     * Creates an outcome.
     *
     * @param seqNo the sequence number assigned
     * @param primaryTerm the primary term the operation ran at
     * @param version the version assigned
     * @param created whether a write created the document rather than overwriting one
     * @param found whether a deletion found anything; always true for a write
     */
    public WriteOutcome(long seqNo, long primaryTerm, long version, boolean created, boolean found) {
        this(seqNo, primaryTerm, version, created, found, null);
    }

    private WriteOutcome(long seqNo, long primaryTerm, long version, boolean created, boolean found, WriteHold.Target forwardedTo) {
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.created = created;
        this.found = found;
        this.forwardedTo = forwardedTo;
    }

    /** The same outcome, marked as applied by a hold's target rather than by the shard the write was addressed to. */
    WriteOutcome forwardedBy(WriteHold.Target target) {
        return new WriteOutcome(seqNo, primaryTerm, version, created, found, target);
    }

    /**
     * Returns the hold target that applied the write, when a hold on the addressed shard forwarded it; a refresh the
     * caller asked for belongs where the target put the document, not on the addressed shard.
     *
     * @return the target, or null when the addressed shard applied the write itself
     */
    public WriteHold.Target forwardedTo() {
        return forwardedTo;
    }

    /**
     * Returns the sequence number the engine assigned.
     *
     * @return the sequence number
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term the operation ran at — this shard's shard-head term.
     *
     * @return the primary term
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
     * Reports whether a write created the document rather than overwriting an existing one.
     *
     * @return true when the document did not exist before
     */
    public boolean created() {
        return created;
    }

    /**
     * Reports whether a deletion found the document. Always true for a write.
     *
     * @return true when there was something to remove
     */
    public boolean found() {
        return found;
    }
}
