/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * One write, durable before it is acknowledged.
 *
 * <p><b>This is a document-level redo log, not an operation log.</b> It records the id and source of a
 * write, so replaying it restores document <em>state</em>; it does not preserve sequence numbers, so it
 * does not restore document <em>history</em>. That distinction is not a detail to discover later: it
 * means replay is idempotent — the same id and source applied twice leaves the same index — which is
 * what lets truncation be conservative rather than exact. It also means anything built on sequence
 * numbers (optimistic concurrency via {@code if_seq_no}, cross-cluster replication) cannot be layered
 * on this without changing it.
 */
public final class WalRecord {

    private final String id;
    private final String source;
    private final boolean deletion;

    /**
     * Creates a record for a deletion.
     *
     * <p>A delete has to be in the log for the same reason a write does, and the consequence of leaving
     * it out is worse. Replay is an idempotent redo of state: a successor rebuilds what its predecessor
     * had by applying every record it finds. A deletion that was acknowledged and not logged is simply
     * absent from that reconstruction, so the document it removed <em>comes back</em> — and it comes back
     * during a recovery that reports success.
     *
     * @param id the document deleted
     * @return the record
     */
    public static WalRecord deletion(String id) {
        return new WalRecord(id, "", true);
    }

    /**
     * Creates a record for an indexed document.
     *
     * @param id the document id
     * @param source the document source
     */
    public WalRecord(String id, String source) {
        this(id, source, false);
    }

    private WalRecord(String id, String source, boolean deletion) {
        this.deletion = deletion;
        this.id = Objects.requireNonNull(id);
        this.source = Objects.requireNonNull(source);
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
                    }
                }
            }
            if (id == null || source == null) {
                throw new IOException("malformed WAL record");
            }
            return new WalRecord(id, source, deleted);
        }
    }

    @Override
    public String toString() {
        return (deletion ? "WalDelete[" : "WalRecord[") + id + "]";
    }
}
