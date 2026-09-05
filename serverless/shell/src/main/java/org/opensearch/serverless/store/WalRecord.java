/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * One write, durable before it is acknowledged — and the sequence identity the engine gave it.
 *
 * <p><b>This is an operation log now, not merely a document-level redo log.</b> It used to record only
 * an id and a source, which restored document <em>state</em> on replay but not document
 * <em>history</em>: a successor re-applied every record as a fresh primary operation and the engine
 * minted brand-new sequence numbers for all of them. That made {@code _seq_no} monotonic within one
 * shard's lifetime on one node and meaningless across a failover, which is why optimistic concurrency
 * ({@code if_seq_no}) could not be built on it. This class's own documentation said so.
 *
 * <p>A record now carries the {@code seqNo}, {@code primaryTerm} and {@code version} the engine assigned
 * when the operation was first applied, so replay can re-apply it <em>as that same operation</em> rather
 * than as a new one. See {@code ShardReconciler}'s replay for the receiving half.
 *
 * <p><b>Replay stays idempotent, and is now idempotent for a better reason.</b> Before, replaying a
 * record twice was safe because the same id and source applied twice leaves the same state — but it
 * consumed two sequence numbers. Now a replayed operation carries the sequence number it already had, so
 * the engine recognises an already-processed operation and skips it outright. The conservative
 * truncation rule in {@link WalStore} therefore costs even less than it did.
 *
 * <p><b>A record with no sequence number is still readable</b>, and is replayed the old way — as a fresh
 * primary operation. That is what a log written before this change contains, and treating it as a parse
 * failure would turn a recoverable shard into an unrecoverable one over a field that has a sound
 * fallback.
 */
public final class WalRecord {

    private final String id;
    private final String source;
    private final boolean deletion;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;

    /**
     * Creates a record for a deletion, carrying the sequence identity the engine assigned it.
     *
     * <p>A delete has to be in the log for the same reason a write does, and the consequence of leaving
     * it out is worse. Replay is a redo of state: a successor rebuilds what its predecessor had by
     * applying every record it finds. A deletion that was acknowledged and not logged is simply absent
     * from that reconstruction, so the document it removed <em>comes back</em> — and it comes back
     * during a recovery that reports success.
     *
     * @param id the document deleted
     * @param seqNo the sequence number the engine assigned
     * @param primaryTerm the primary term the operation ran at
     * @param version the version the engine assigned
     * @return the record
     */
    public static WalRecord deletion(String id, long seqNo, long primaryTerm, long version) {
        return new WalRecord(id, "", true, seqNo, primaryTerm, version);
    }

    /**
     * Creates a record for a deletion with no sequence identity.
     *
     * <p>Replayed as a fresh primary operation. Kept for callers that genuinely have no engine result to
     * report — not for the write path, which always has one.
     *
     * @param id the document deleted
     * @return the record
     */
    public static WalRecord deletion(String id) {
        return new WalRecord(id, "", true, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, Versions.MATCH_ANY);
    }

    /**
     * Creates a record for an indexed document, carrying the sequence identity the engine assigned it.
     *
     * @param id the document id
     * @param source the document source
     * @param seqNo the sequence number the engine assigned
     * @param primaryTerm the primary term the operation ran at
     * @param version the version the engine assigned
     */
    public WalRecord(String id, String source, long seqNo, long primaryTerm, long version) {
        this(id, source, false, seqNo, primaryTerm, version);
    }

    /**
     * Creates a record for an indexed document with no sequence identity.
     *
     * @param id the document id
     * @param source the document source
     */
    public WalRecord(String id, String source) {
        this(id, source, false, SequenceNumbers.UNASSIGNED_SEQ_NO, SequenceNumbers.UNASSIGNED_PRIMARY_TERM, Versions.MATCH_ANY);
    }

    private WalRecord(String id, String source, boolean deletion, long seqNo, long primaryTerm, long version) {
        this.deletion = deletion;
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
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
     * Returns the document source.
     *
     * @return the source
     */
    public String source() {
        return source;
    }

    /**
     * Reports whether this record removes a document rather than adding one.
     *
     * @return true for a deletion
     */
    public boolean isDeletion() {
        return deletion;
    }

    /**
     * Returns the sequence number the engine assigned this operation.
     *
     * @return the sequence number, or {@link SequenceNumbers#UNASSIGNED_SEQ_NO} for a record written
     *     before this log carried one
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term the operation ran at.
     *
     * @return the primary term, or {@link SequenceNumbers#UNASSIGNED_PRIMARY_TERM} if unrecorded
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the version the engine assigned.
     *
     * @return the version, or {@link Versions#MATCH_ANY} if unrecorded
     */
    public long version() {
        return version;
    }

    /**
     * Reports whether this record can be replayed as the operation it originally was, rather than as a
     * fresh one.
     *
     * @return true if a sequence number and primary term were recorded
     */
    public boolean hasSequenceIdentity() {
        return seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO && primaryTerm != SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    }

    /**
     * Serializes this record.
     *
     * @return the blob bytes
     * @throws IOException if serialization fails
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("id", id);
            builder.field("source", source);
            if (deletion) {
                // Written only for deletions, so a log produced before deletes existed parses unchanged
                // and means what it always meant. A reader that has never heard of this field reads such
                // a record as an index operation, which is exactly what it is.
                builder.field("deleted", true);
            }
            if (hasSequenceIdentity()) {
                // Written only when there is one, for the same reason "deleted" is: a reader that has
                // never heard of these fields reads the record exactly as it used to.
                builder.field("seq_no", seqNo);
                builder.field("primary_term", primaryTerm);
                builder.field("version", version);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a record.
     *
     * @param input the serialized record
     * @return the parsed record
     * @throws IOException if the bytes are malformed
     */
    public static WalRecord fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            String id = null;
            String source = null;
            boolean deleted = false;
            long seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
            long primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
            long version = Versions.MATCH_ANY;
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token.isValue()) {
                    if ("id".equals(field)) {
                        id = parser.text();
                    } else if ("source".equals(field)) {
                        source = parser.text();
                    } else if ("deleted".equals(field)) {
                        deleted = parser.booleanValue();
                    } else if ("seq_no".equals(field)) {
                        seqNo = parser.longValue();
                    } else if ("primary_term".equals(field)) {
                        primaryTerm = parser.longValue();
                    } else if ("version".equals(field)) {
                        version = parser.longValue();
                    }
                }
            }
            if (id == null || source == null) {
                throw new IOException("malformed WAL record");
            }
            return new WalRecord(id, source, deleted, seqNo, primaryTerm, version);
        }
    }

    @Override
    public String toString() {
        return (deletion ? "WalDelete[" : "WalRecord[") + id + (hasSequenceIdentity() ? "@" + seqNo + "/" + primaryTerm : "") + "]";
    }
}
