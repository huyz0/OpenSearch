/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Plans and answers a parsed search for {@link SearchHandler}: refusals, defaults, the point in time, name resolution,
 * the fan-out, and the response.
 */
final class SearchPlanner {

    /** The handler's own logger, so moving the planning here changes no logger name. */
    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(SearchHandler.class);

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    SearchPlanner(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    /**
     * Everything after parsing: refusals, defaults, the point in time, name resolution and the fan-out.
     *
     * <p>Split from {@link SearchHandler#prepareRequest} so a search that arrives some other way -- rendered from a
     * template, say -- runs exactly the search a plain body would, rather than a second copy of this.
     *
     * @param request the request, for its remaining parameters and its rendering parameters
     * @param searchRequest what core parsed into the request
     * @param parsedSource what core parsed into the source
     * @param bodyPit the body's point-in-time block, or null
     * @param counting whether this is a count
     * @return what to run
     * @throws IOException if a register cannot be read
     */
    org.opensearch.common.CheckedConsumer<org.opensearch.rest.RestChannel, Exception> plan(
        RestRequest request,
        org.opensearch.action.search.SearchRequest searchRequest,
        SearchSourceBuilder parsedSource,
        org.opensearch.search.builder.PointInTimeBuilder bodyPit,
        boolean counting
    ) throws IOException {
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        // Parameters core parsed into the request rather than into the source, each answered on its own
        // terms. Three change what the answer would be, and are refused. The rest are hints a classic
        // coordinator may already decline to follow -- which replica, whether to cache, how many shards
        // to ask at once -- and are consumed without effect: there is one copy of every shard here, the
        // fan-out's width is fixed, and a hint that cannot be followed is not a reason to refuse the
        // search that carried it.
        if (searchRequest.searchType() != org.opensearch.action.search.SearchType.QUERY_THEN_FETCH) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "DFS searches are not supported: only query_then_fetch is available"
                )
            );
        }
        if (SearchGate.selectsShards(searchRequest.preference())) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "shard-selecting search preferences are not supported"
                )
            );
        }
        if (searchRequest.scroll() != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "scroll holds a search context open on a node; use a point in time (POST /{index}/_search/point_in_time) "
                        + "with search_after, which is held in the object store and is not tied to one node"
                )
            );
        }
        final Boolean partialResults = searchRequest.allowPartialSearchResults();
        // Its own opt-in, not allow_partial_search_results: that defaults to true, and a search wider than
        // the activation budget must be refused unless the caller asked, in so many words, for part of it.
        final SearchFanout.Options fanoutOptions = new SearchFanout.Options(request.paramAsBoolean("allow_partial_activation", false))
            .withRouting(DocumentRouting.searchRoutings(searchRequest.routing()));
        // Two more core parses into the request and nothing here reads: cancel_after_time_interval cancels
        // a task, and a search here is not a task -- each shard's query is bounded by the request's own
        // timeout and the fan-out's own deadline, which are the bounds that exist; phase_took asks for
        // per-phase timings that a fan-out over shard results does not measure. Both are accepted for the
        // same reason preference is: a hint that cannot be followed is not a reason to refuse the search.

        // A search pipeline, named by the search_pipeline parameter or the body, or defined inline in the
        // body. Its request processors run here, over the request core parsed, before anything is
        // resolved or fanned out -- so a filter_query processor narrows what every shard is asked -- and
        // its response processors run over the response the fan-out builds, before it is rendered.
        final org.opensearch.search.pipeline.PipelinedRequest pipelined;
        if (searchRequest.pipeline() != null || parsedSource.searchPipelineSource() != null) {
            try {
                if (parsedSource.searchPipelineSource() != null) {
                    pipelined = serving.services()
                        .searchPipelines()
                        .transformRequest("_ad_hoc_pipeline", parsedSource.searchPipelineSource(), searchRequest);
                } else {
                    final Optional<String> stored = metadata.searchPipelines().get(searchRequest.pipeline());
                    if (stored.isEmpty()) {
                        return channel -> channel.sendResponse(
                            IndexAdminHandler.error(
                                channel,
                                RestStatus.BAD_REQUEST,
                                "pipeline_missing",
                                "no such search pipeline: " + searchRequest.pipeline()
                            )
                        );
                    }
                    pipelined = serving.services()
                        .searchPipelines()
                        .transformRequest(searchRequest.pipeline(), stored.get(), searchRequest);
                }
            } catch (Exception e) {
                return channel -> channel.sendResponse(IndexAdminHandler.failure(channel, e));
            }
        } else {
            pipelined = null;
        }
        final java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess = pipelined == null
            ? response -> response
            : response -> serving.services().searchPipelines().transformResponse(pipelined, response);
        // What the request processors left, when there were any: the pipelined request is a copy, and its
        // source is the one every shard must be asked with.
        final SearchSourceBuilder source = pipelined == null ? parsedSource : pipelined.source();

        SearchHandler.applyDefaults(source, counting);

        final String unsupported = SearchHandler.whatCannotBeMerged(source);
        if (unsupported != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_search", unsupported)
            );
        }

        // A frozen view answers for itself: it already knows which index and which shards, so none of the
        // name resolution below applies to it. Two spellings: this shell's ?pit= and OpenSearch's own body
        // block. The body block used to parse and then be ignored, so a client paging a point in time by
        // the book got a live search that looked frozen -- the moving result set the feature exists to
        // prevent.
        final String pitParam = request.param("pit");
        final String pitId;
        if (bodyPit != null) {
            if (pitParam != null && pitParam.equals(bodyPit.getId()) == false) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "the pit named in the body and in ?pit= differ")
                );
            }
            pitId = bodyPit.getId();
        } else {
            pitId = pitParam;
        }
        if (pitId != null) {
            final var pit = metadata.pointInTime(pitId);
            if (pit.isEmpty() || pit.get().expiredAt(metadata.clock().getAsLong())) {
                // Expired and never-existed are the same answer to a caller: the view is not there. Saying
                // which would be more useful and would also be a way to find out whether an id was ever
                // valid, and there is no reason to offer that.
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_FOUND,
                        "pit_not_found",
                        "no such point in time, or it has expired: " + pitId
                    )
                );
            }
            org.opensearch.serverless.metadata.PointInTime frozen = pit.get();
            final String viewIndex = frozen.index();
            if (frozen.indexUuid() != null
                && metadata.describe(viewIndex).map(IndexDescriptor::uuid).filter(frozen.indexUuid()::equals).isEmpty()) {
                // The name is gone, or names a different index now. A record written before the uuid was
                // recorded (indexUuid null) cannot be checked and is left to the manifest's own lengths.
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_FOUND,
                        "pit_not_found",
                        "the point in time " + pitId + " was taken over an earlier index of that name, which no longer exists"
                    )
                );
            }
            if (searchRequest.indices().length > 0
                && java.util.List.of(searchRequest.indices()).equals(java.util.List.of(viewIndex)) == false) {
                // A view already names its index, and the path named something else. Core refuses the
                // combination outright; this shell's own callers write /{index}/_search?pit= with the
                // view's own index and are left alone, but /orders/_search over a view of logs used to
                // answer hits from logs under the orders path -- a lie in the URL.
                final String named = java.util.Arrays.toString(searchRequest.indices());
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "bad_query",
                        "[indices] cannot be used with point in time: the view "
                            + pitId
                            + " is over ["
                            + viewIndex
                            + "], and the path named "
                            + named
                    )
                );
            }
            if (bodyPit != null && bodyPit.getKeepAlive() != null) {
                if (bodyPit.getKeepAlive().millis() > PointInTimeHandler.MAX_KEEP_ALIVE_MILLIS) {
                    // The same refusal creating the view gives, for the same reason: a caller asking for a
                    // day and being quietly handed an hour finds out at minute sixty-one.
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "keep_alive_too_long",
                            PointInTimeHandler.keepAliveTooLong()
                        )
                    );
                }
                // OpenSearch's keep_alive on a search extends the view from now. This shell's expiry is
                // absolute, so extending it is a new record with a later deadline -- one write, and only
                // when it actually moves the deadline later, since a keep_alive shorter than what remains
                // asks for nothing.
                final long until = metadata.clock().getAsLong() + bodyPit.getKeepAlive().millis();
                // Every client sends keep_alive on every page. Written only when it moves the deadline by
                // more than a quarter of itself, so a page a second is a write per quarter-lifetime rather
                // than a write per page; the view is still extended long before it could lapse.
                final long materially = Math.max(1L, bodyPit.getKeepAlive().millis() / 4);
                if (until - frozen.expiresAtMillis() > materially) {
                    // Conditional on the deadline this request read: a release that landed in between, or
                    // another node's extension, makes this a no-write, and a released view is answered as
                    // gone rather than written back into existence -- which an unconditional write used
                    // to do, resurrecting a view every node had already let go of.
                    final long expected = frozen.expiresAtMillis();
                    frozen = frozen.withExpiry(until);
                    if (metadata.extendPointInTime(frozen, expected) == false) {
                        return channel -> channel.sendResponse(
                            IndexAdminHandler.error(
                                channel,
                                RestStatus.NOT_FOUND,
                                "pit_not_found",
                                "no such point in time, or it has expired: " + pitId
                            )
                        );
                    }
                }
            }
            final var view = frozen;
            return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    respondFrozen(channel, serving, metadata, view, source, counting, request, partialResults, postProcess);
                } catch (Exception e) {
                    try {
                        channel.sendResponse(IndexAdminHandler.failure(channel, e));
                    } catch (IOException nested) {
                        logger.error("failed to report a frozen search failure", nested);
                    }
                }
            });
        }

        // A bare /_search names no index, and means every index -- which this shell already expresses as a
        // prefix pattern, resolved by one bounded listing.
        final String index = searchRequest.indices().length == 0 ? "*" : String.join(",", searchRequest.indices());
        // ignore_unavailable and allow_no_indices, from core's own IndicesOptions parsing. allow_no_indices
        // defaults to true in core and is honoured here only when the caller wrote it: a pattern matching
        // nothing answering 200 with no hits is the confident empty answer this surface refuses by default,
        // and a default nobody chose is not the caller saying they mean it.
        final boolean ignoreUnavailable = searchRequest.indicesOptions().ignoreUnavailable()
            || (request.hasParam("allow_no_indices") && searchRequest.indicesOptions().allowNoIndices());
        // One instant for the whole request: the rollups judge date math against it here, and the fan-out
        // must judge against the same one, or a range ending at "now" could be ruled out a millisecond early.
        final long nowMillis = System.currentTimeMillis();
        // Filters can replace or broaden the query. Pruning on the original query before they run
        // would silently remove indices the admitted query needs, so defer query-dependent pruning.
        final SearchHandler.Resolution resolved = SearchHandler.resolveIndices(
            metadata,
            serving,
            index,
            ignoreUnavailable,
            true,
            serving.services().actionGate().isEmpty() ? source : null,
            nowMillis
        );
        final SearchFanout.Options searchOptions = new SearchFanout.Options(
            fanoutOptions.allowPartialActivation(),
            resolved.refused() ? 0 : resolved.prunedShards(),
            nowMillis,
            resolved.refused() ? java.util.Set.of() : resolved.judged(),
            fanoutOptions.routing(),
            java.util.Set.of()
        );
        if (resolved.refused()) {
            return channel -> channel.sendResponse(IndexAdminHandler.error(channel, resolved.status(), resolved.type(), resolved.reason()));
        }

        // Off the HTTP thread. executeQueryPhase hands work to the search pool and this waits for it;
        // waiting on the transport thread that is meant to be reading the next request resets the
        // connection, which surfaces to the client as RST_STREAM rather than as anything diagnosable.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            refreshMembership(metadata);
            try {
                respond(
                    channel,
                    serving,
                    metadata,
                    resolved.indices(),
                    resolved.skipped(),
                    source,
                    counting,
                    request,
                    partialResults,
                    postProcess,
                    searchOptions
                );
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a search failure", nested);
                }
            }
        });
    }

    /**
     * Brings the membership snapshot up to date before a search fans out, at a bounded cost.
     *
     * <p>Membership is the address book and the placement input. The snapshot is good for as long as a
     * lease is, and a join moves the members index's generation, which one register read detects -- but
     * one read per search is one object-store round trip on the critical path of every search, which at a
     * thousand searches a second is millions of reads an hour for a fact that changes at join and leave.
     * So the read is made at most once per {@code BlobLeaseMembership#GENERATION_PROBE_INTERVAL_MILLIS},
     * wall-clock, and the searches in between are served from the snapshot. That cap used to live here,
     * and so covered search alone; it lives in the membership now, where every request path gets it. A
     * node that joined inside that second is reached a second late; a node already known is reached at
     * once, and a peer missing from the snapshot is still looked up by its lease when a forward needs its
     * address.
     */
    void refreshMembership(MetadataPlane metadata) {
        try {
            metadata.membership().refreshIfOlderThan(Math.max(1_000L, metadata.leaseTtlMillis() / 2));
        } catch (Exception e) {
            logger.warn("could not refresh membership before searching", e);
        }
    }

    /** Whether a frozen shard failed to open because its record had gone, anywhere in the cause chain. */
    static boolean noLongerHeld(Throwable e) {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("no longer held")) {
                return true;
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return false;
    }

    void respondFrozen(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.metadata.PointInTime pit,
        SearchSourceBuilder source,
        boolean counting,
        org.opensearch.core.xcontent.ToXContent.Params params,
        Boolean partialResults,
        java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess
    ) throws IOException {
        final long startNanos = System.nanoTime();
        final org.opensearch.serverless.shard.SearchOutcome outcome;
        try {
            outcome = SearchHandler.gated(
                serving,
                java.util.List.of(pit.index()),
                source,
                partialResults,
                admitted -> SearchFanout.runFrozen(serving, metadata, pit, admitted)
            );
        } catch (Exception e) {
            if (noLongerHeld(e)) {
                // Released or expired between this request resolving the record and a shard opening it.
                // To the caller that is the view being gone, not the node failing.
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_FOUND,
                        "pit_not_found",
                        "no such point in time, or it has expired: " + pit.id()
                    )
                );
                return;
            }
            throw e;
        }
        SearchRendering.render(
            channel,
            java.util.List.of(),
            outcome,
            source,
            SearchRendering.tookMillis(startNanos),
            counting,
            pit.id(),
            params,
            postProcess
        );
    }

    void respond(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Map<String, IndexDescriptor> indices,
        java.util.List<String> skipped,
        SearchSourceBuilder source,
        boolean counting,
        org.opensearch.core.xcontent.ToXContent.Params params,
        Boolean partialResults,
        java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess,
        SearchFanout.Options fanoutOptions
    ) throws IOException {
        final long startNanos = System.nanoTime();
        // Filtered here for the same reason DocumentHandler is: this handler formats over the shared
        // fan-out rather than going through ShardOperations, so the gate has to meet it where it works.
        final var outcome = SearchHandler.gated(
            serving,
            indices.keySet(),
            source,
            partialResults,
            admitted -> SearchFanout.run(serving, metadata, indices, admitted, fanoutOptions)
        );
        SearchRendering.render(
            channel,
            skipped,
            outcome,
            source,
            SearchRendering.tookMillis(startNanos),
            counting,
            null,
            params,
            postProcess
        );
    }
}
