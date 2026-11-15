/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.documents;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.serverless.write.BulkOutcome;

import java.util.List;

/** One operation and, eventually, what became of it. */
public final class BulkItem {
    public final String action;
    public final String index;
    public String writtenIndex;
    String writtenUuid;
    public WalRecord operation;
    public String pipeline;
    /** The pipelines to run, in order: the request's or the index's default, then the index's final. */
    List<String> pipelines = List.of();
    public int shard = -1;
    public RestStatus status;
    public String result;
    public String nodeId;
    public String errorType;
    public String errorReason;
    public long seqNo = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
    public long primaryTerm = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
    public long version = org.opensearch.common.lucene.uid.Versions.NOT_FOUND;
    public boolean requireAbsent;
    long ifSeqNo = org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
    long ifPrimaryTerm = 0L;
    /** Whether what this item writes is pushed to its shard's search nodes, or left to the shard's next commit. */
    public boolean tailPush = true;

    BulkItem(String action, String index, WalRecord operation) {
        this.action = action;
        this.index = index;
        this.operation = operation;
    }

    public boolean failed() {
        return errorType != null;
    }

    void fail(RestStatus status, String type, String reason) {
        if (failed()) {
            return;
        }
        this.status = status;
        this.errorType = type;
        this.errorReason = reason;
    }

    /** Forgets a refusal that applied nothing, so the operation can be sent again. */
    public void clearRefusal() {
        this.status = null;
        this.errorType = null;
        this.errorReason = null;
    }

    /** Dropped by its pipeline: answered, not written. */
    void noop(String nodeId) {
        this.nodeId = nodeId;
        this.status = RestStatus.OK;
        this.result = "noop";
    }

    void succeed(BulkOutcome outcome, String nodeId) {
        if (outcome.failure() != null) {
            // A lost condition is a 409, not a 400, and carries the type classic OpenSearch uses --
            // a client retrying a compare-and-swap needs to tell "your condition did not hold" from
            // "your document was malformed", and only one of those is worth retrying.
            if (outcome.conflict()) {
                fail(RestStatus.CONFLICT, "version_conflict_engine_exception", outcome.failure());
            } else if (outcome.retryable()) {
                // The mapping was being grown by another node at the same moment. The document is
                // fine and a retry lands it; a 400 told the client the opposite.
                fail(RestStatus.SERVICE_UNAVAILABLE, "mapping_contention", outcome.failure());
            } else {
                fail(RestStatus.BAD_REQUEST, "operation_failed", outcome.failure());
            }
            return;
        }
        this.nodeId = nodeId;
        this.seqNo = outcome.seqNo();
        this.primaryTerm = outcome.primaryTerm();
        this.version = outcome.version();
        if (outcome.isDeletion()) {
            this.result = outcome.found() ? "deleted" : "not_found";
            this.status = outcome.found() ? RestStatus.OK : RestStatus.NOT_FOUND;
        } else {
            // Told apart honestly now, the same as the single-document path: the engine reports
            // whether the document existed, and this used to say "created" regardless.
            this.result = outcome.created() ? "created" : "updated";
            this.status = outcome.created() ? RestStatus.CREATED : RestStatus.OK;
        }
    }
}
