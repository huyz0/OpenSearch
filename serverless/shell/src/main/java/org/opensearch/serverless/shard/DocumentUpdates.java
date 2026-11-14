/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shard;

import org.opensearch.serverless.shell.Document;

import java.io.IOException;

/**
 * Runs a document update for {@link ShardOperations}: reads the current document, applies the partial document or
 * script, and writes the result back through the shard's owner, retrying on a version conflict.
 */
final class DocumentUpdates {

    private final ShardOperations operations;

    DocumentUpdates(ShardOperations operations) {
        this.operations = operations;
    }

    /**
     * Reads a document, merges a partial update into it, and writes the result back.
     *
     * <p>The read and write use the document's sequence identity as a compare-and-swap. A concurrent
     * change causes a conflict rather than losing a merge. Native requests may retry an unconditional
     * update from a fresh read up to their admitted {@code retry_on_conflict} count; a caller-supplied
     * sequence condition is never retried against a different identity.
     *
     * <p><b>The merge itself is core's own</b> ({@link org.opensearch.common.xcontent.XContentHelper#update}),
     * used by core's own {@code _update} internally — recursing into nested objects, overwriting everything
     * else, and reporting whether anything actually changed. Reusing it means the merge semantics are
     * exactly what a classic client already expects, and {@code detect_noop} falls out of it for free rather
     * than needing its own comparison.
     *
     * @param index the index
     * @param id the document id
     * @param doc the partial document to merge in
     * @param upsert the document to write if none exists, or null
     * @param docAsUpsert if true, write {@code doc} itself when none exists
     * @param detectNoop if true, a merge that changed nothing is not written
     * @param refresh whether to make the result visible to search before returning
     * @param ifSeqNo the sequence number the document must currently be at, or unassigned for none
     * @param ifPrimaryTerm the primary term the document must currently be at, ignored without {@code ifSeqNo}
     * @return what happened
     * @throws DocumentMissingException if nothing exists and neither {@code upsert} nor {@code docAsUpsert}
     *     was given
     * @throws IOException if the read or the write fails
     */
    public UpdateOutcome update(
        String index,
        String id,
        java.util.Map<String, Object> doc,
        java.util.Map<String, Object> upsert,
        boolean docAsUpsert,
        boolean detectNoop,
        boolean refresh,
        long ifSeqNo,
        long ifPrimaryTerm
    ) throws IOException {
        return update(index, id, doc, upsert, docAsUpsert, detectNoop, refresh, ifSeqNo, ifPrimaryTerm, null, false);
    }

    /**
     * Updates a document, optionally by running a script over it.
     *
     * <p><b>The script decides, and core runs it.</b> A scripted update compiles through the node's own
     * {@code ScriptService} under core's {@code UpdateScript} context and is handed the same {@code ctx} map
     * classic OpenSearch hands it — {@code ctx._source}, {@code ctx.op}, {@code ctx._index}, {@code ctx._id}.
     * Nothing here interprets the script or re-implements the context; a script that works against a classic
     * node works here because it is the same context object and the same engine.
     *
     * <p>{@code ctx.op} is honoured: a script setting it to {@code "noop"} writes nothing and a script
     * setting it to {@code "delete"} removes the document, which is the whole reason a caller reaches for a
     * scripted update rather than a partial document.
     *
     * @param index the index
     * @param id the document id
     * @param doc the partial document, or null for a scripted update
     * @param upsert what to write when the document is absent, or null
     * @param docAsUpsert whether the partial document is also the upsert
     * @param detectNoop whether an update that changes nothing is reported as a noop
     * @param refresh whether to make the write visible before answering
     * @param ifSeqNo the sequence number the document must be at, or unassigned
     * @param ifPrimaryTerm the primary term the document must be at, or 0
     * @param script the script to run, or null
     * @param scriptedUpsert whether the script also runs when the document is absent
     * @return what happened
     * @throws IOException if the read or the write fails
     */
    public UpdateOutcome update(
        String index,
        String id,
        java.util.Map<String, Object> doc,
        java.util.Map<String, Object> upsert,
        boolean docAsUpsert,
        boolean detectNoop,
        boolean refresh,
        long ifSeqNo,
        long ifPrimaryTerm,
        org.opensearch.script.Script script,
        boolean scriptedUpsert
    ) throws IOException {
        final var request = new org.opensearch.action.update.UpdateRequest(index, id).docAsUpsert(docAsUpsert)
            .detectNoop(detectNoop)
            .script(script)
            .scriptedUpsert(scriptedUpsert)
            .setIfSeqNo(ifSeqNo)
            .setIfPrimaryTerm(ifPrimaryTerm)
            .setRefreshPolicy(
                refresh
                    ? org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE
                    : org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE
            );
        if (doc != null) {
            request.doc(doc);
        }
        if (upsert != null) {
            request.upsert(upsert);
        }
        return update(request);
    }

    /**
     * Runs an update using the native request admitted by action filters.
     * @param request the update, including its source, script and retry policy
     * @return the native result and the serving node when the response was not replaced
     * @throws IOException if placement, reading or writing fails
     */
    public UpdateOutcome update(org.opensearch.action.update.UpdateRequest request) throws IOException {
        return operations.gated(org.opensearch.action.update.UpdateAction.NAME, request, admitted -> {
            final var selected = (org.opensearch.action.update.UpdateRequest) admitted;
            final var invalid = selected.validate();
            if (invalid != null) {
                throw invalid;
            }
            if (selected.retryOnConflict() < 0) {
                throw new IllegalArgumentException("retry_on_conflict must be non negative");
            }
            if (selected.scriptedUpsert() && selected.script() == null) {
                throw new IllegalArgumentException("scripted_upsert was given without a script");
            }
            if (selected.isRequireAlias() || selected.waitForActiveShards().validate(0) == false) {
                throw new org.opensearch.OpenSearchStatusException(
                    "require_alias and active-shard counts above one are not supported on updates",
                    org.opensearch.core.rest.RestStatus.NOT_IMPLEMENTED
                );
            }
            if ((selected.doc() != null && selected.doc().extraFieldValues().isEmpty() == false)
                || (selected.upsertRequest() != null && selected.upsertRequest().extraFieldValues().isEmpty() == false)) {
                throw new org.opensearch.OpenSearchStatusException(
                    "extra field values are not supported on updates",
                    org.opensearch.core.rest.RestStatus.NOT_IMPLEMENTED
                );
            }
            final java.util.Map<String, Object> doc = selected.doc() == null ? null : selected.doc().sourceAsMap();
            final java.util.Map<String, Object> upsert = selected.upsertRequest() == null ? null : selected.upsertRequest().sourceAsMap();
            final boolean refresh = selected.getRefreshPolicy() != org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE;
            final long attempts = 1L + selected.retryOnConflict();
            for (long attempt = 0L;; attempt++) {
                try {
                    return withFetchedSource(
                        selected,
                        executeAcceptedUpdate(
                            selected.index(),
                            selected.id(),
                            selected.routing(),
                            doc,
                            upsert,
                            selected.docAsUpsert(),
                            selected.detectNoop(),
                            refresh,
                            selected.ifSeqNo(),
                            selected.ifPrimaryTerm(),
                            selected.script(),
                            selected.scriptedUpsert()
                        )
                    );
                } catch (org.opensearch.index.engine.VersionConflictEngineException e) {
                    if (attempt >= attempts - 1L) {
                        throw e;
                    }
                }
            }
        }, new org.opensearch.serverless.shell.ActionGate.ResponseView<UpdateOutcome>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(UpdateOutcome result) {
                return result.response();
            }

            @Override
            public UpdateOutcome read(org.opensearch.core.action.ActionResponse response, UpdateOutcome original) {
                return original != null && response == original.response()
                    ? original
                    : new UpdateOutcome((org.opensearch.action.update.UpdateResponse) response, null);
            }
        });
    }

    /**
     * Adds the document as the update left it when the request asked for its source, as core's update does: a plugin
     * keeping a lock document asks for it on every release. Read back realtime from the shard, and attached only when the read
     * is this update's own result (same sequence number); a write that landed in between leaves the response without one
     * rather than with someone else's document.
     */
    private UpdateOutcome withFetchedSource(org.opensearch.action.update.UpdateRequest request, UpdateOutcome outcome) throws Exception {
        if (request.fetchSource() == null || request.fetchSource().fetchSource() == false) {
            return outcome;
        }
        final org.opensearch.action.update.UpdateResponse response = outcome.response();
        if (response.getResult() == org.opensearch.action.DocWriteResponse.Result.DELETED
            || response.getResult() == org.opensearch.action.DocWriteResponse.Result.NOT_FOUND) {
            return outcome;
        }
        final org.opensearch.action.get.GetRequest get = new org.opensearch.action.get.GetRequest(response.getIndex(), response.getId())
            .routing(request.routing());
        final org.opensearch.action.get.GetResponse read = (org.opensearch.action.get.GetResponse) DocumentReads.getView(
            response.getIndex()
        ).show(operations.reads.readAcceptedGet(get));
        if (read.isExists() && read.getSeqNo() == response.getSeqNo() && read.getPrimaryTerm() == response.getPrimaryTerm()) {
            response.setGetResult(
                org.opensearch.action.update.UpdateHelper.extractGetResult(
                    request,
                    response.getIndex(),
                    read.getSeqNo(),
                    read.getPrimaryTerm(),
                    read.getVersion(),
                    read.getSourceAsMap(),
                    org.opensearch.common.xcontent.XContentType.JSON,
                    read.getSourceAsBytesRef()
                )
            );
        }
        return outcome;
    }

    /** Applies one read/compare-and-swap attempt without invoking a nested action filter chain. */
    UpdateOutcome executeAcceptedUpdate(
        String index,
        String id,
        String customRouting,
        java.util.Map<String, Object> doc,
        java.util.Map<String, Object> upsert,
        boolean docAsUpsert,
        boolean detectNoop,
        boolean refresh,
        long ifSeqNo,
        long ifPrimaryTerm,
        org.opensearch.script.Script script,
        boolean scriptedUpsert
    ) throws IOException {
        // doGet, not get: this whole method is one gated operation under UpdateAction's own name, and
        // calling the public get() here would run the filter chain a second time under GetAction's --
        // which is not what a filter checking "may this caller write" expects to see for an update.
        final Read current = operations.reads.doGet(index, id, customRouting);
        final Document existing = current.document();
        if (existing.found()
            && ifSeqNo != org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO
            && (existing.seqNo() != ifSeqNo || existing.primaryTerm() != ifPrimaryTerm)) {
            // Core checks the condition on its update read, before a script or noop can bypass
            // the write path. A stale condition must also fail when no write would be emitted.
            throw new org.opensearch.index.engine.VersionConflictEngineException(
                current.shardId(),
                id,
                ifSeqNo,
                ifPrimaryTerm,
                existing.seqNo(),
                existing.primaryTerm()
            );
        }

        final java.util.Map<String, Object> merged;
        final String result;
        if (existing.found() == false) {
            if (docAsUpsert) {
                merged = org.opensearch.ingest.IngestDocument.deepCopyMap(doc);
                result = "created";
            } else if (upsert != null) {
                merged = org.opensearch.ingest.IngestDocument.deepCopyMap(upsert);
                result = "created";
            } else {
                throw new DocumentMissingException(index, id);
            }
        } else {
            merged = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                new org.opensearch.core.common.bytes.BytesArray(existing.source()),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
            if (script != null) {
                result = "updated";
            } else {
                final boolean changed = org.opensearch.common.xcontent.XContentHelper.update(merged, doc, detectNoop);
                result = (detectNoop && changed == false) ? "noop" : "updated";
            }
        }

        String operation = result;
        if (script != null && (existing.found() || scriptedUpsert)) {
            // ctx as classic OpenSearch builds it, so a script written against a classic node behaves
            // the same here. The map is mutable and the script edits it in place, which is the contract
            // UpdateScript defines rather than one chosen here.
            final java.util.Map<String, Object> ctx = new java.util.HashMap<>();
            ctx.put("_source", merged);
            ctx.put("_now", operations.node.threadPool().absoluteTimeInMillis());
            ctx.put("op", existing.found() ? "index" : "create");
            if (existing.found()) {
                ctx.put("_index", index);
                ctx.put("_id", id);
                ctx.put("_version", existing.version());
                ctx.put("_routing", existing.routing() != null ? existing.routing() : customRouting);
            }
            final var factory = operations.node.services().scriptService().compile(script, org.opensearch.script.UpdateScript.CONTEXT);
            factory.newInstance(script.getParams(), ctx).execute();
            operation = (String) ctx.get("op");
            // Core permits only create for an upsert, and index/delete for an existing
            // document. Every other script operation is a noop, including unknown values.
            if (existing.found()
                ? "index".equals(operation) == false && "delete".equals(operation) == false
                : "create".equals(operation) == false) {
                operation = "noop";
            }
            @SuppressWarnings("unchecked")
            final java.util.Map<String, Object> edited = (java.util.Map<String, Object>) ctx.get("_source");
            // A script usually mutates ctx._source in place, in which case it *is* this map and there is
            // nothing to copy. It may also replace it wholesale. Clearing first and copying second
            // handles the second case and silently empties the document in the first, because the source
            // and the destination are the same object -- which is what the assertion on the document's
            // contents, rather than on the response, caught.
            if (edited != null && edited != merged) {
                final java.util.Map<String, Object> replacement = new java.util.LinkedHashMap<>(edited);
                merged.clear();
                merged.putAll(replacement);
            }
        }

        if ("noop".equals(operation) || "none".equals(operation)) {
            return new UpdateOutcome(
                "noop",
                current.servedBy(),
                existing.seqNo(),
                existing.primaryTerm(),
                existing.version(),
                current.shardId(),
                id,
                refresh
            );
        }
        if ("delete".equals(operation)) {
            if (existing.found() == false) {
                // A scripted upsert cannot delete a document another writer created after this read.
                return new UpdateOutcome(
                    "noop",
                    current.servedBy(),
                    existing.seqNo(),
                    existing.primaryTerm(),
                    existing.version(),
                    current.shardId(),
                    id,
                    refresh
                );
            }
            // The script decided against the document we read. Deleting unconditionally here
            // removed a newer concurrent write; use the same compare-and-swap as an indexed update.
            final DocumentWrites.Written removed = operations.writes.written(
                index,
                id,
                customRouting,
                "",
                refresh,
                true,
                existing.seqNo(),
                existing.primaryTerm(),
                false
            );
            return new UpdateOutcome(
                "deleted",
                removed.servedBy(),
                removed.outcome().seqNo(),
                removed.outcome().primaryTerm(),
                removed.outcome().version(),
                removed.shardId,
                id,
                refresh
            );
        }

        if ("noop".equals(result)) {
            // Nothing written, so the document keeps the identity the read just observed. Reporting
            // that identity rather than an unassigned one is what lets a caller chain a conditional
            // write after a noop without having to re-read.
            return new UpdateOutcome(
                result,
                current.servedBy(),
                existing.seqNo(),
                existing.primaryTerm(),
                existing.version(),
                current.shardId(),
                id,
                refresh
            );
        }
        final String mergedJson;
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.map(merged);
            mergedJson = org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
        }
        // The compare-and-swap an update is: against what was just read, unless the caller brought a
        // condition of their own. This used to pass the caller's (unassigned) condition, so two
        // concurrent partial updates each read, each wrote, and one merge was lost -- and the
        // retry_on_conflict loop could never fire. A missing document is written as a
        // create, so two concurrent upserts cannot both "create".
        final boolean unconditional = ifSeqNo == org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
        final DocumentWrites.Written written = operations.writes.written(
            index,
            id,
            customRouting,
            mergedJson,
            refresh,
            false,
            unconditional && existing.found() ? existing.seqNo() : ifSeqNo,
            unconditional && existing.found() ? existing.primaryTerm() : ifPrimaryTerm,
            unconditional && existing.found() == false
        );
        return new UpdateOutcome(
            result,
            written.servedBy(),
            written.outcome().seqNo(),
            written.outcome().primaryTerm(),
            written.outcome().version(),
            written.shardId,
            id,
            refresh
        );
    }
}
