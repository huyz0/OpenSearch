/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.Document;
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;

/**
 * Where a document lives, who may serve it, and the four things you can do to it.
 *
 * <p><b>This exists because the REST handlers were about to be forked.</b> Routing a single document —
 * find the descriptor, hash to a shard, serve it here or forward it to the owner, and say something
 * useful when nobody can — was written inside {@code DocumentHandler} and again inside {@code GetHandler},
 * and a {@code Client} façade for plugins would have written it a third time. Three copies of a routing
 * rule is three chances for them to disagree about who owns a shard, which is the one thing this system
 * must not be vague about.
 *
 * <p><b>Failures are typed, not rendered.</b> Each caller says what a lost race means in its own
 * vocabulary: REST turns "not the writer" into a 421 naming the owner, and a plugin's {@code Client} turns
 * the same thing into an exception it can retry. Neither meaning belongs down here.
 */
public final class ShardOperations {

    private final ServerlessNode node;
    private final MetadataPlane plane;
    private final boolean refuseSystemIndices;
    private final RequestRouting routing;
    private final DocumentReads reads;
    private final DocumentWrites writes;
    private final DeleteByQueryExecution deletes;

    /**
     * Creates the operations view for a caller inside the node -- a plugin's {@code Client}, which may
     * read and write its own system index.
     *
     * @param node the node serving requests
     * @param plane the metadata plane
     */
    public ShardOperations(ServerlessNode node, MetadataPlane plane) {
        this(node, plane, false);
    }

    /**
     * Creates the operations view.
     *
     * <p><b>The system-index rule lives here, once, for every request that arrives over REST.</b> The
     * registration-time guard reads only the index in the request path, and a body-addressed request --
     * {@code _mget} naming {@code .serverless_auth} in {@code docs} -- walked straight past it to the
     * credential records. Placing the refusal in {@link #place} means the next handler that resolves an
     * index from a body cannot miss it, because every operation here places before it does anything.
     *
     * @param node the node serving requests
     * @param plane the metadata plane
     * @param refuseSystemIndices true for a REST caller, which may not touch a plugin's index by any spelling
     */
    public ShardOperations(ServerlessNode node, MetadataPlane plane, boolean refuseSystemIndices) {
        this.node = node;
        this.plane = plane;
        this.refuseSystemIndices = refuseSystemIndices;
        this.routing = new RequestRouting(node, plane, refuseSystemIndices);
        this.reads = new DocumentReads(node, plane, routing);
        this.writes = new DocumentWrites(node, routing);
        this.deletes = new DeleteByQueryExecution(node, plane, refuseSystemIndices, routing, writes);
    }

    /**
     * Seeds this instance with a descriptor the caller has already read, so it is not read again.
     *
     * @param descriptor the descriptor
     */
    public void assumeDescribed(IndexDescriptor descriptor) {
        routing.assumeDescribed(descriptor);
    }

    /**
     * Works out where a document belongs and who can serve it.
     *
     * @param index the index
     * @param id the document id
     * @return the placement
     * @throws IOException if the metadata plane cannot be read
     * @throws NoSuchIndexException if the index does not exist
     */
    public Placement place(String index, String id) throws IOException {
        return routing.place(index, id);
    }

    /**
     * Reads one document by id, from the copy that has everything acknowledged.
     *
     * <p>Answered by the shard's owner, or from a published commit when nobody owns it. See
     * {@code GetHandler} for why a get is routed to the writer and a search is not.
     *
     * <p>Returns who answered rather than only what they said, because the caller cannot work that out
     * afterwards: a forwarded read is served by a node this one merely asked. A first version had the
     * handler re-derive it from the placement and quietly name the local node for every forwarded get.
     *
     * @param index the index
     * @param id the document id
     * @return what was found and who found it
     * @throws IOException if the read fails or no copy can answer
     */
    public Read get(String index, String id) throws IOException {
        return gated(
            org.opensearch.action.get.GetAction.NAME,
            new org.opensearch.action.get.GetRequest(index, id),
            admitted -> reads.readAcceptedGet((org.opensearch.action.get.GetRequest) admitted),
            DocumentReads.getView(index)
        );
    }

    /**
     * Reads a native request with its version and source selection intact.
     *
     * @param request the native get request
     * @return the document response
     * @throws IOException if the read fails
     */
    public org.opensearch.action.get.GetResponse get(org.opensearch.action.get.GetRequest request) throws IOException {
        return gatedNative(
            org.opensearch.action.get.GetAction.NAME,
            request,
            org.opensearch.action.get.GetResponse.class,
            admitted -> (org.opensearch.action.get.GetResponse) DocumentReads.getView(request.index())
                .show(reads.readAcceptedGet((org.opensearch.action.get.GetRequest) admitted))
        );
    }

    /**
     * Runs one operation through the plugins' action filters.
     *
     * <p>Wrapped here rather than at each REST handler because this is the funnel: a get through the REST
     * surface, a get through a plugin's {@code Client} and a get from the shell's own code are all this
     * method. A guard placed in a handler is a guard the next handler forgets.
     *
     * <p>The checked-exception dance exists because these operations declare {@code IOException} and the
     * gate declares {@code Exception}; a filter's own refusal is a runtime exception and passes through
     * unchanged, carrying the plugin's status rather than one invented here.
     */
    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws IOException {
        return gated(action, request, work, org.opensearch.serverless.shell.ActionGate.opaque());
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work,
        org.opensearch.serverless.shell.ActionGate.ResponseView<T> view
    ) throws IOException {
        return gated(action, request, admitted -> work.get(), view);
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedFunction<org.opensearch.action.ActionRequest, T, Exception> work,
        org.opensearch.serverless.shell.ActionGate.ResponseView<T> view
    ) throws IOException {
        try {
            return node.services().actionGate().run(action, request, work, view);
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Explains why a document does or does not match a query.
     *
     * <p>Routed exactly as a get is, and for the same reason: an explain names one document, and the copy
     * that has every acknowledged write is the owner. Explaining from a published commit while a writer holds
     * newer segments would produce a plausible number computed from the wrong statistics — a score is a
     * function of the whole shard's term and document frequencies, not just of the document being scored.
     *
     * <p>Which copy answered is returned rather than inferred, for the reason {@link Read} returns it: when
     * nobody owns the shard this reads a published commit, and a caller comparing this score against a search
     * result needs to know the two were computed over the same segments.
     *
     * @param index the index
     * @param id the document id
     * @param query the query to score against
     * @return the explanation and who produced it
     * @throws IOException if the explain fails or no copy can answer
     */
    public Explained explain(String index, String id, org.opensearch.index.query.QueryBuilder query) throws IOException {
        return gated(
            org.opensearch.action.explain.ExplainAction.NAME,
            new org.opensearch.action.explain.ExplainRequest(index, id),
            () -> reads.doExplain(index, id, query)
        );
    }

    /**
     * Runs a search across every shard of an index and merges the answers.
     *
     * <p>Lifted out of {@code SearchHandler} unchanged so that the REST layer and a plugin's
     * {@code Client} run the same fan-out, the same merge and the same window. A search that behaved one
     * way for a user and another for a plugin would be two search engines wearing one name.
     *
     * @param index the index
     * @param source the query, with its own from and size
     * @return what was found and how completely
     * @throws IOException if the search fails outright
     */
    public SearchOutcome search(String index, org.opensearch.search.builder.SearchSourceBuilder source) throws IOException {
        return org.opensearch.serverless.rest.SearchHandler.gated(node, java.util.List.of(index), source, accepted -> {
            if (refuseSystemIndices && node.isSystemIndex(index)) {
                throw new SystemIndexException(index);
            }
            final IndexDescriptor descriptor = routing.describeOnce(index).orElseThrow(() -> new NoSuchIndexException(index));
            return org.opensearch.serverless.rest.SearchFanout.run(node, plane, descriptor, accepted);
        });
    }

    /**
     * Writes one document, durably, wherever it belongs.
     *
     * @param index the index
     * @param id the document id
     * @param source the document body
     * @param refresh whether to make it visible to search before returning
     * @return the node that performed the write, or null when a filter supplies a cached response without executing it
     * @throws IOException if the write fails or no node can take it
     */
    public String index(String index, String id, String source, boolean refresh) throws IOException {
        final var request = new org.opensearch.action.index.IndexRequest(index).id(id)
            .source(source, org.opensearch.common.xcontent.XContentType.JSON)
            .setRefreshPolicy(
                refresh
                    ? org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE
                    : org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE
            );
        return gated(
            org.opensearch.action.index.IndexAction.NAME,
            request,
            admitted -> writes.executeNativeIndex((org.opensearch.action.index.IndexRequest) admitted),
            new org.opensearch.serverless.shell.ActionGate.ResponseView<DocumentWrites.NativeIndexResult>() {
                @Override
                public org.opensearch.core.action.ActionResponse show(DocumentWrites.NativeIndexResult result) {
                    return result.response();
                }

                @Override
                public DocumentWrites.NativeIndexResult read(
                    org.opensearch.core.action.ActionResponse response,
                    DocumentWrites.NativeIndexResult original
                ) {
                    return new DocumentWrites.NativeIndexResult(
                        (org.opensearch.action.index.IndexResponse) response,
                        original == null ? null : original.servedBy()
                    );
                }
            }
        ).servedBy();
    }

    /**
     * Executes a native index request with its conditions and returns the engine's actual result.
     *
     * @param request the plugin's request
     * @return the native acknowledgement
     * @throws IOException if the write cannot complete
     */
    public org.opensearch.action.index.IndexResponse index(org.opensearch.action.index.IndexRequest request) throws IOException {
        return gatedNative(
            org.opensearch.action.index.IndexAction.NAME,
            request,
            org.opensearch.action.index.IndexResponse.class,
            admitted -> writes.executeNativeIndex((org.opensearch.action.index.IndexRequest) admitted).response()
        );
    }

    /**
     * Executes a native delete request with its conditions and returns its actual found-ness and sequence identity.
     *
     * @param request the plugin's request
     * @return the native acknowledgement
     * @throws IOException if the deletion cannot complete
     */
    public org.opensearch.action.delete.DeleteResponse delete(org.opensearch.action.delete.DeleteRequest request) throws IOException {
        return gatedNative(
            org.opensearch.action.delete.DeleteAction.NAME,
            request,
            org.opensearch.action.delete.DeleteResponse.class,
            admitted -> {
                final var accepted = (org.opensearch.action.delete.DeleteRequest) admitted;
                final var invalid = accepted.validate();
                if (invalid != null) {
                    throw invalid;
                }
                DocumentWrites.validateNativeWrite(
                    accepted.routing(),
                    accepted.version(),
                    accepted.versionType(),
                    accepted.waitForActiveShards(),
                    false
                );
                final boolean refresh = accepted.getRefreshPolicy() != org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE;
                try (
                    var ignored = node.services()
                        .indexingPressure()
                        .markCoordinatingOperationStarted(
                            org.opensearch.serverless.store.WalRecord.contentBytes(accepted.id(), null),
                            false
                        )
                ) {
                    final DocumentWrites.Written written = writes.written(
                        accepted.index(),
                        accepted.id(),
                        "",
                        refresh,
                        true,
                        accepted.ifSeqNo(),
                        accepted.ifPrimaryTerm()
                    );
                    final var outcome = written.outcome();
                    final var response = new org.opensearch.action.delete.DeleteResponse(
                        written.shardId,
                        accepted.id(),
                        outcome.seqNo(),
                        outcome.primaryTerm(),
                        outcome.version(),
                        outcome.found()
                    );
                    DocumentWrites.completeNativeWriteResponse(response, refresh);
                    return response;
                }
            }
        );
    }

    private <T extends org.opensearch.core.action.ActionResponse> T gatedNative(
        String action,
        org.opensearch.action.ActionRequest request,
        Class<T> responseType,
        org.opensearch.common.CheckedFunction<org.opensearch.action.ActionRequest, T, Exception> work
    ) throws IOException {
        try {
            return node.services()
                .actionGate()
                .run(action, request, work, org.opensearch.serverless.shell.ActionGate.identity(responseType));
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Removes one document, wherever it lives.
     *
     * @param index the index
     * @param id the document id
     * @param refresh whether to make the removal visible before returning
     * @return true if the document was there
     * @throws IOException if the delete fails or no node can take it
     */
    public boolean delete(String index, String id, boolean refresh) throws IOException {
        final var request = new org.opensearch.action.delete.DeleteRequest(index, id).setRefreshPolicy(
            refresh
                ? org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE
                : org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE
        );
        return delete(request).getResult() == org.opensearch.action.DocWriteResponse.Result.DELETED;
    }

    /**
     * Deletes every document a query matches.
     *
     * <p><b>Evaluated against a frozen view, not live state.</b> The set of documents to delete is decided
     * once, against the last published commit as of when the operation starts — the same point-in-time
     * mechanism {@code search_after} paging uses, and for the same reason: a query re-evaluated against a
     * moving index could match a document twice, or never, depending on when a write happened to land
     * relative to which page was being read. A write acknowledged after the operation starts, published or
     * not, is not touched by it — the same boundary a plain search_after export of matching ids would have.
     *
     * <p><b>Walked one shard at a time, sorted by {@code _doc}</b> — native Lucene document order, always
     * available with no fielddata, and exhaustive within one shard's frozen reader — rather than through
     * the cross-shard {@code search_after} merge a multi-index search uses. That merge exists to produce
     * one globally ordered page across shards, which needs a sort whose values are comparable across
     * shards; this needs only that every match is visited exactly once, which a per-shard order already
     * gives without needing to be meaningful across shards at all.
     *
     * <p><b>Deletes go to live state, not the frozen view.</b> Matching is frozen; deleting is not — each
     * matched id is removed through the ordinary write path, conditional on the sequence identity the
     * frozen view saw. A document rewritten since that view is a version conflict and is left alone.
     *
     * <p><b>Gated once, under {@code indices:data/write/delete/byquery}</b>, not once per document under
     * {@code indices:data/write/delete} — the same reasoning {@code BulkHandler} gives for gating a whole
     * batch once: a privilege evaluator registered for the by-query action would otherwise never see it, and
     * one registered for plain deletes would see thousands of calls a caller never made individually.
     *
     * @param index the index
     * @param query the query; required, there is no implicit match_all
     * @param maxDocs the most documents to visit, or {@code Long.MAX_VALUE} for no cap
     * @param refresh whether to make the deletions visible to search before returning
     * @return how many matched and how many were removed
     * @throws NoSuchIndexException if the index does not exist
     * @throws IOException if the query, a delete, or the point-in-time machinery fails
     */
    public DeleteByQueryOutcome deleteByQuery(String index, org.opensearch.index.query.QueryBuilder query, long maxDocs, boolean refresh)
        throws IOException {
        return deleteByQuery(index, query, maxDocs, refresh, true);
    }

    /**
     * Deletes every document a query matches, leaving alone any that changed after the query was frozen.
     *
     * <p>Each delete is conditional on the sequence number the frozen view saw, so a document rewritten
     * between the view and the delete is a version conflict rather than a silent loss of the rewrite --
     * which is what core's {@code conflicts=abort} and {@code conflicts=proceed} are about.
     *
     * @param index the index
     * @param query the query
     * @param maxDocs the most documents to visit
     * @param refresh whether to make the deletions visible before returning
     * @param abortOnConflict whether the first conflict stops the operation, as core does by default
     * @return the outcome
     * @throws NoSuchIndexException if the index does not exist
     * @throws IOException if the query, a delete, or the point-in-time machinery fails
     */
    public DeleteByQueryOutcome deleteByQuery(
        String index,
        org.opensearch.index.query.QueryBuilder query,
        long maxDocs,
        boolean refresh,
        boolean abortOnConflict
    ) throws IOException {
        final var request = new org.opensearch.index.reindex.DeleteByQueryRequest(index).setQuery(query)
            .setRefresh(refresh)
            .setAbortOnVersionConflict(abortOnConflict);
        if (maxDocs != Long.MAX_VALUE) {
            if (maxDocs < 1 || maxDocs > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("max_docs must be between 1 and " + Integer.MAX_VALUE);
            }
            request.setMaxDocs(Math.toIntExact(maxDocs));
        }
        return gated(org.opensearch.index.reindex.DeleteByQueryAction.NAME, request, admitted -> {
            if ((admitted instanceof org.opensearch.index.reindex.DeleteByQueryRequest) == false) {
                throw new IllegalArgumentException("a delete-by-query filter must provide DeleteByQueryRequest");
            }
            final var accepted = (org.opensearch.index.reindex.DeleteByQueryRequest) admitted;
            final var validation = accepted.validate();
            if (validation != null) {
                throw validation;
            }
            if (accepted.indices().length != 1 || accepted.getRouting() != null) {
                throw new UnsupportedRequestOptionException("delete-by-query requires one concrete index and no routing");
            }
            final var search = accepted.getSearchRequest();
            final var acceptedQuery = search.source().query();
            final var supportedSource = new org.opensearch.index.reindex.DeleteByQueryRequest(accepted.indices()).setQuery(acceptedQuery)
                .getSearchRequest()
                .source();
            if (search.source().equals(supportedSource) == false
                || search.searchType() != org.opensearch.action.search.SearchType.QUERY_THEN_FETCH
                || accepted.getSlices() != 1
                || accepted.getRequestsPerSecond() != Float.POSITIVE_INFINITY
                || accepted.getShouldStoreResult()) {
                throw new UnsupportedRequestOptionException(
                    "delete-by-query search options, slicing, throttling and stored tasks are not supported"
                );
            }
            final long started = System.nanoTime();
            final var result = deletes.doDeleteByQuery(
                accepted.indices()[0],
                acceptedQuery,
                accepted.getMaxDocs() == org.opensearch.index.reindex.AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES
                    ? Long.MAX_VALUE
                    : accepted.getMaxDocs(),
                accepted.isRefresh(),
                accepted.isAbortOnVersionConflict()
            );
            return result.withResponse(accepted.indices()[0], System.nanoTime() - started);
        }, new org.opensearch.serverless.shell.ActionGate.ResponseView<DeleteByQueryOutcome>() {
            @Override
            public org.opensearch.core.action.ActionResponse show(DeleteByQueryOutcome result) {
                return result.response();
            }

            @Override
            public DeleteByQueryOutcome read(org.opensearch.core.action.ActionResponse response, DeleteByQueryOutcome original) {
                final var answered = (org.opensearch.index.reindex.BulkByScrollResponse) response;
                if (original != null && answered == original.response()) {
                    return original;
                }
                final String abortedOn = answered.getBulkFailures()
                    .stream()
                    .filter(failure -> failure.getStatus() == org.opensearch.core.rest.RestStatus.CONFLICT)
                    .map(org.opensearch.action.bulk.BulkItemResponse.Failure::getId)
                    .filter(java.util.Objects::nonNull)
                    .findFirst()
                    .orElse(null);
                return new DeleteByQueryOutcome(
                    answered.getTotal(),
                    answered.getDeleted(),
                    answered.getVersionConflicts(),
                    abortedOn,
                    answered,
                    null,
                    answered.getBatches()
                );
            }
        });
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
        return gated(org.opensearch.action.update.UpdateAction.NAME, request, admitted -> {
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
            if (selected.routing() != null
                || selected.isRequireAlias()
                || selected.waitForActiveShards().validate(0) == false
                || (selected.fetchSource() != null && selected.fetchSource().fetchSource())) {
                throw new org.opensearch.OpenSearchStatusException(
                    "routing, require_alias, source fetching and active-shard counts above one are not supported on updates",
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
                    return executeAcceptedUpdate(
                        selected.index(),
                        selected.id(),
                        doc,
                        upsert,
                        selected.docAsUpsert(),
                        selected.detectNoop(),
                        refresh,
                        selected.ifSeqNo(),
                        selected.ifPrimaryTerm(),
                        selected.script(),
                        selected.scriptedUpsert()
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

    /** Applies one read/compare-and-swap attempt without invoking a nested action filter chain. */
    private UpdateOutcome executeAcceptedUpdate(
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
        // doGet, not get: this whole method is one gated operation under UpdateAction's own name, and
        // calling the public get() here would run the filter chain a second time under GetAction's --
        // which is not what a filter checking "may this caller write" expects to see for an update.
        final Read current = reads.doGet(index, id);
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
            ctx.put("_now", node.threadPool().absoluteTimeInMillis());
            ctx.put("op", existing.found() ? "index" : "create");
            if (existing.found()) {
                ctx.put("_index", index);
                ctx.put("_id", id);
                ctx.put("_version", existing.version());
                ctx.put("_routing", null);
            }
            final var factory = node.services().scriptService().compile(script, org.opensearch.script.UpdateScript.CONTEXT);
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
            final DocumentWrites.Written removed = writes.written(index, id, "", refresh, true, existing.seqNo(), existing.primaryTerm());
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
        final DocumentWrites.Written written = writes.written(
            index,
            id,
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
