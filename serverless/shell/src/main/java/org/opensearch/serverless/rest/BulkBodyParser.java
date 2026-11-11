/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.serverless.store.WalRecord;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits a bulk body into its operations: action lines and sources, with the fields this system refuses named
 * before anything is written. Split out of {@link BulkHandler}.
 */
final class BulkBodyParser {

    private BulkBodyParser() {}

    static final java.util.Set<String> ACTION_NAMES = java.util.Set.of("index", "create", "delete", "update");

    /**
     * Splits the newline-delimited body into operations.
     *
     * <p>Structural problems throw: a body whose action and source lines do not pair up has no
     * well-defined prefix of good items, so guessing where the batch resumes would be inventing a
     * request the client did not send. Problems with an individual, well-formed action — an unsupported
     * one — become a failed item instead, and the rest of the batch proceeds.
     */
    static List<BulkHandler.Item> parse(byte[] body, String defaultIndex) throws BadRequest, IOException {
        final List<BulkHandler.Item> items = new ArrayList<>();
        final List<byte[]> lines = split(body);
        int line = 0;
        while (line < lines.size()) {
            if (new String(lines.get(line), StandardCharsets.UTF_8).isBlank()) {
                line++;
                continue;
            }
            final Action action = parseAction(lines.get(line), line + 1, defaultIndex);
            if (action.index == null || action.index.isBlank()) {
                throw new BadRequest("line " + (line + 1) + ": an index is required in the action or request path");
            }
            line++;
            if (action.needsSource) {
                if (line >= lines.size()) {
                    throw new BadRequest("line " + line + ": action '" + action.name + "' has no source line after it");
                }
                action.source = new String(lines.get(line), StandardCharsets.UTF_8);
                line++;
            }
            items.add(action.toItem());
        }
        return items;
    }

    /** Splits on newlines, retaining empty sources so the next action cannot become a document. */
    static List<byte[]> split(byte[] body) {
        final List<byte[]> lines = new ArrayList<>();
        int start = 0;
        for (int i = 0; i <= body.length; i++) {
            if (i != body.length && body[i] != '\n') {
                continue;
            }
            int end = i;
            // Tolerate CRLF, which a client sending a file rather than a buffer will produce.
            if (end > start && body[end - 1] == '\r') {
                end--;
            }
            if (i < body.length || end > start) {
                final byte[] copy = new byte[end - start];
                System.arraycopy(body, start, copy, 0, end - start);
                lines.add(copy);
            }
            start = i + 1;
        }
        return lines;
    }

    /**
     * Action-line fields asking for a version model this system does not keep.
     *
     * <p>{@code _seq_no} and {@code _primary_term} used to be here too, refused with the same message. They
     * are now honoured: the batch path carries a condition per item and hands it to the same engine
     * compare-and-swap the single-document path uses. What is left is external versioning, which asks this
     * system to order writes by a number the caller maintains and it does not keep — the same refusal
     * {@code PUT /{index}/_doc/{id}} makes, for the same reason.
     */
    static final java.util.Set<String> EXTERNAL_VERSION_FIELDS = java.util.Set.of("version", "_version", "version_type");

    /**
     * The names a condition is <em>not</em> spelled with on an action line.
     *
     * <p>{@code _seq_no} and {@code _primary_term} are what a bulk <em>response</em> reports; the request
     * spells the condition {@code if_seq_no} and {@code if_primary_term}, in this shell as in OpenSearch. A
     * client that sends the response's names is asking for a compare-and-swap in a way nothing will honour,
     * and dropping the field silently would give them an unconditional write while they believed otherwise
     * — which is the failure this whole surface is built to refuse. So it is a refusal that names the right
     * spelling rather than a field quietly ignored.
     */
    static final java.util.Set<String> MISSPELLED_CONDITIONS = java.util.Set.of("_seq_no", "_primary_term");

    static Action parseAction(byte[] bytes, int lineNumber, String defaultIndex) throws BadRequest, IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, new ByteArrayInputStream(bytes))
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new BadRequest("line " + lineNumber + ": expected an action object");
            }
            if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new BadRequest("line " + lineNumber + ": expected an action name");
            }
            final String actionName = parser.currentName();
            if (ACTION_NAMES.contains(actionName) == false) {
                throw new BadRequest("line " + lineNumber + ": unknown bulk action [" + actionName + "]");
            }
            final Action action = new Action(actionName, defaultIndex);
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new BadRequest("line " + lineNumber + ": action '" + action.name + "' must be followed by an object");
            }
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                    if ("update".equals(action.name) && "_source".equals(field)) {
                        // A valid update-only option; the action itself is refused per item below.
                        parser.skipChildren();
                    } else {
                        throw new BadRequest("line " + lineNumber + ": expected a scalar value for [" + field + "]");
                    }
                } else if (token.isValue()) {
                    if ("_index".equals(field)) {
                        action.index = parser.text();
                    } else if ("_id".equals(field)) {
                        action.id = parser.text();
                    } else if ("op_type".equals(field)) {
                        // {"index":{"op_type":"create"}} is {"create":{}} spelled the other way, and core
                        // accepts both. This used to be read and dropped, so a create was an overwrite.
                        action.opType = parser.text();
                    } else if ("routing".equals(field) || "_routing".equals(field)) {
                        action.routing = parser.text();
                    } else if ("pipeline".equals(field)) {
                        action.pipeline = parser.text();
                    } else if ("require_alias".equals(field)) {
                        action.requireAlias = parser.booleanValue();
                    } else if (EXTERNAL_VERSION_FIELDS.contains(field)) {
                        // Read and kept, not merely noticed: a client that asked for a version comparison
                        // and got an unconditional write believing it got one is the confident wrong
                        // answer this design refuses everywhere else.
                        action.conditional = field;
                    } else if (MISSPELLED_CONDITIONS.contains(field)) {
                        action.misspelled = field;
                    } else if ("if_seq_no".equals(field)) {
                        action.ifSeqNo = parser.longValue();
                    } else if ("if_primary_term".equals(field)) {
                        action.ifPrimaryTerm = parser.longValue();
                    } else if ("update".equals(action.name) && ("retry_on_conflict".equals(field) || "_source".equals(field))) {
                        // Consumed only for the unsupported update action.
                    } else {
                        throw new BadRequest("line " + lineNumber + ": unknown action metadata [" + field + "]");
                    }
                }
            }
            if (token != XContentParser.Token.END_OBJECT
                || parser.nextToken() != XContentParser.Token.END_OBJECT
                || parser.nextToken() != null) {
                throw new BadRequest("line " + lineNumber + ": expected exactly one complete action");
            }
            return action;
        } catch (org.opensearch.core.xcontent.XContentParseException e) {
            throw new BadRequest("line " + lineNumber + ": " + e.getMessage());
        }
    }

    /** One parsed action line, before it becomes an item. */
    static final class Action {
        final String name;
        final boolean needsSource;
        final boolean supported;
        String index;
        String id;
        String source;
        String conditional;
        String misspelled;
        String opType;
        String routing;
        String pipeline;
        boolean requireAlias;
        long ifSeqNo = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
        long ifPrimaryTerm = 0L;

        Action(String name, String defaultIndex) {
            this.name = name;
            this.index = defaultIndex;
            // "create" is an ordinary index operation at MATCH_DELETED -- the engine compares against its
            // own live version map, so it needs no machinery this system lacks. "update" is a partial
            // merge, which needs the current document read back before anything is written; the batch path
            // applies without reading, so it stays refused rather than approximated.
            this.supported = "index".equals(name) || "delete".equals(name) || "create".equals(name);
            this.needsSource = "index".equals(name) || "create".equals(name) || "update".equals(name);
        }

        BulkHandler.Item toItem() {
            // Only index/create may generate an id. A missing delete id is a failed item and must
            // never become a deletion of a newly generated, unrelated id.
            final String documentId = id != null ? id : "delete".equals(name) || "update".equals(name) ? "" : UUIDs.base64UUID();
            final BulkHandler.Item item = new BulkHandler.Item(
                name,
                index,
                "delete".equals(name) ? WalRecord.deletion(documentId) : new WalRecord(documentId, source == null ? "" : source)
            );
            item.requireAbsent = "create".equals(name) || "create".equals(opType);
            item.ifSeqNo = ifSeqNo;
            item.ifPrimaryTerm = ifPrimaryTerm;
            item.pipeline = pipeline;
            if (("delete".equals(name) && (id == null || id.isEmpty())) || (id != null && id.isEmpty())) {
                item.fail(RestStatus.BAD_REQUEST, "missing_id", "this operation requires a nonempty document id");
            } else if (opType != null && "index".equals(name) == false) {
                item.fail(RestStatus.BAD_REQUEST, "bad_request", "op_type is an option of the index action");
            } else if (opType != null && "create".equals(opType) == false && "index".equals(opType) == false) {
                item.fail(RestStatus.BAD_REQUEST, "bad_request", "op_type must be create or index, not [" + opType + "]");
            } else if (routing != null) {
                item.fail(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "routing is not supported: a document is placed by its id alone, so a routing value would be "
                        + "accepted and change nothing"
                );
            } else if (requireAlias) {
                item.fail(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "require_alias is not supported: writes here name an index, and an alias resolves on read"
                );
            } else if (supported == false) {
                item.fail(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "'"
                        + name
                        + "' is not supported in a batch: it is a partial merge, which has to read the current "
                        + "document before it can write one, and this path applies a batch without reading it. "
                        + "Use POST /{index}/_update/{id}, which does."
                );
            } else if (conditional != null) {
                item.fail(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_action",
                    "'"
                        + conditional
                        + "' on '"
                        + documentId
                        + "' asks for external versioning, which this system does not have: it keeps no "
                        + "caller-supplied version model. Use if_seq_no and if_primary_term on the action line."
                );
            } else if (misspelled != null) {
                item.fail(
                    RestStatus.BAD_REQUEST,
                    "invalid_condition",
                    "'"
                        + misspelled
                        + "' on '"
                        + documentId
                        + "' is what a bulk response reports, not what a request asks with. A condition is "
                        + "spelled if_seq_no and if_primary_term on the action line."
                );
            } else if ((ifSeqNo < 0L && ifSeqNo != org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO)
                || ifPrimaryTerm < 0L
                || (item.requireAbsent
                    && (ifSeqNo != org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO || ifPrimaryTerm != 0L))) {
                        item.fail(
                            RestStatus.BAD_REQUEST,
                            "invalid_condition",
                            "invalid sequence condition or compare-and-set on a create action"
                        );
                    } else if ((ifSeqNo == org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO) != (ifPrimaryTerm == 0L)) {
                        item.fail(
                            RestStatus.BAD_REQUEST,
                            "invalid_condition",
                            "if_seq_no and if_primary_term must be supplied together on '" + documentId + "'"
                        );
                    } else if (("index".equals(name) || "create".equals(name)) && (source == null || source.isBlank())) {
                        item.fail(RestStatus.BAD_REQUEST, "missing_source", "the document body for '" + documentId + "' was empty");
                    }
            return item;
        }
    }

    /** A body that cannot be split into operations at all. */
    static final class BadRequest extends Exception {
        BadRequest(String message) {
            super(message);
        }
    }
}
