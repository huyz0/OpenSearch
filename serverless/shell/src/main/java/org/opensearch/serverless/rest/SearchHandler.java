/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code GET|POST /{index}/_search} — querying the shards this node can actually reach.
 *
 * <p><b>The response always reports shard coverage, and that is the point of this class.</b> A node
 * serves the shards it holds; with no cross-node fan-out yet it may hold some of an index and not the
 * rest. Returning a hit count without saying how much of the index it came from would be the exact
 * failure {@code HANDOFF.md} records eight times — a confident answer computed over a fraction of the
 * data, indistinguishable from a complete one.
 *
 * <p>So {@code _shards.total} and {@code _shards.successful} are always present, and a partial search
 * additionally sets {@code "complete": false} — a field with no real-OpenSearch equivalent, because a
 * classic coordinating node always fans out to every shard it can resolve and so never has this question
 * to answer. A caller that ignores those is choosing to; a caller that reads them cannot be misled.
 */
public final class SearchHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public SearchHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_search_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/{index}/_search"),
            new Route(RestRequest.Method.POST, "/{index}/_search"),
            // Searching every index. Without these the request fell through to GET /{index}, which read
            // "_search" as an index name and answered "no such index: _search" -- a confusing refusal of a
            // request the shell can in fact serve, since a bare "*" is the prefix pattern it already
            // resolves with one bounded listing.
            new Route(RestRequest.Method.GET, "/_search"),
            new Route(RestRequest.Method.POST, "/_search"),
            // Counting is a search with size=0 whose total is already exact here, because this shell always
            // tracks the total. The refusal that used to stand here told callers exactly that and left them
            // to do it -- which is work this handler was already doing and could simply report.
            new Route(RestRequest.Method.GET, "/{index}/_count"),
            new Route(RestRequest.Method.POST, "/{index}/_count"),
            new Route(RestRequest.Method.GET, "/_count"),
            new Route(RestRequest.Method.POST, "/_count")
        );
    }

    /**
     * The parameters core's own search action exempts from the unconsumed-parameter check, because they
     * are read while the response is rendered rather than while the request is parsed. {@code typed_keys}
     * is the one that matters most: the official Java client sets it on every search unconditionally, so a
     * handler that did not know the name could not serve that client at all.
     */
    private static final java.util.Set<String> RESPONSE_PARAMS = java.util.Set.of(
        org.opensearch.rest.action.search.RestSearchAction.TYPED_KEYS_PARAM,
        org.opensearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM,
        org.opensearch.rest.action.search.RestSearchAction.INCLUDE_NAMED_QUERIES_SCORE_PARAM
    );

    @Override
    protected java.util.Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    /**
     * What resolving the names in a request produced: the indices to fan out over, or the refusal.
     *
     * @param indices each index and its shard count, in request order
     * @param skipped names {@code ignore_unavailable} dropped, so the answer can say so
     * @param status the refusal's status, or null when the names resolved
     * @param type the refusal's error type, or null
     * @param reason the refusal's reason, or null
     */
    record Resolution(java.util.LinkedHashMap<String, IndexDescriptor> indices, java.util.List<String> skipped, RestStatus status,
        String type, String reason) {
        static Resolution refuse(RestStatus status, String type, String reason) {
            return new Resolution(null, null, status, type, reason);
        }

        boolean refused() {
            return status != null;
        }
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean counting = request.path().endsWith("/_count");
        // Read here as well as in plan(): plan runs after this method returns, and BaseRestHandler checks
        // for unread parameters in between.
        request.param("pit");

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final ServerlessNode serving = node.get();

        // Core's own request parser, for the same reason q= uses core's own query-string parser: it is
        // what these parameters mean in OpenSearch. This handler used to read six of them by hand and let
        // BaseRestHandler reject the other forty as "unrecognized parameter", which is how the official
        // Java client (typed_keys on every search) and OpenSearch Dashboards (track_total_hits and
        // max_concurrent_shard_requests on every search) came to be unable to search this shell at all.
        // Everything RestSearchAction parses into the SearchSourceBuilder -- sort, _source, stored_fields,
        // docvalue_fields, track_total_hits, track_scores, timeout, terminate_after, explain, version,
        // seq_no_primary_term, df, default_operator, analyzer, lenient -- is then forwarded to every shard
        // exactly as a body field would be, so it is honoured rather than merely accepted. The URL wins over
        // the body for size, from and q, which is core's precedence; this handler had it the other way
        // round.
        final org.opensearch.action.search.SearchRequest searchRequest = new org.opensearch.action.search.SearchRequest();
        final SearchSourceBuilder source = new SearchSourceBuilder();
        searchRequest.source(source);
        final org.opensearch.search.builder.PointInTimeBuilder bodyPit;
        try {
            if (request.hasContent()) {
                try (
                    XContentParser parser = XContentType.JSON.xContent()
                        .createParser(
                            serving.searchXContentRegistry(),
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            request.content().streamInput()
                        )
                ) {
                    source.parseXContent(parser, true);
                }
            }
            // The body's pit block is taken out before core sees the request. Core's preparePointInTime
            // decodes the id as a SearchContextId -- a base64 record of shard contexts on nodes -- and
            // this shell's ids are the names of its own register entries. Read here, honoured below.
            bodyPit = source.pointInTimeBuilder();
            source.pointInTimeBuilder(null);
            org.opensearch.rest.action.search.RestSearchAction.parseSearchRequest(
                searchRequest,
                request,
                null,
                null,
                size -> source.size(size)
            );
            if (counting) {
                // _count's own parameter, which _search does not have.
                final float minScore = request.paramAsFloat("min_score", -1f);
                if (minScore != -1f) {
                    source.minScore(minScore);
                }
            }
        } catch (Exception e) {
            // Everything else is consumed on the way out, or BaseRestHandler reports the parameter it did
            // not get to instead of the parse failure that stopped it getting there.
            IndexAdminHandler.consumeAllParams(request);
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "could not parse the search: " + e.getMessage())
            );
        }

        // Off the HTTP thread from here: resolving names, reading a point in time and refreshing membership
        // are object-store round trips, and they used to run on the Netty worker that should have been
        // reading the next request.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                plan(request, searchRequest, source, bodyPit, counting).accept(channel);
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
     * Everything after parsing: refusals, defaults, the point in time, name resolution and the fan-out.
     *
     * <p>Split from {@link #prepareRequest} so a search that arrives some other way -- rendered from a
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
    RestChannelConsumer plan(
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
        if (searchRequest.routing() != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "routing is not supported: a document here is placed by its id alone, so there is no routing "
                        + "value by which a search could narrow to fewer shards"
                )
            );
        }
        final boolean failOnPartial = Boolean.FALSE.equals(searchRequest.allowPartialSearchResults());

        // A search pipeline, named by the search_pipeline parameter or the body, or defined inline in the
        // body. Its request processors run here, over the request core parsed, before anything is
        // resolved or fanned out -- so a filter_query processor narrows what every shard is asked -- and
        // its response processors run over the response the fan-out builds, before it is rendered.
        final org.opensearch.search.pipeline.PipelinedRequest pipelined;
        if (searchRequest.pipeline() != null || parsedSource.searchPipelineSource() != null) {
            try {
                if (parsedSource.searchPipelineSource() != null) {
                    pipelined = serving.searchPipelines()
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
                    pipelined = serving.searchPipelines().transformRequest(searchRequest.pipeline(), stored.get(), searchRequest);
                }
            } catch (Exception e) {
                return channel -> channel.sendResponse(IndexAdminHandler.failure(channel, e));
            }
        } else {
            pipelined = null;
        }
        final java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess = pipelined == null
            ? response -> response
            : response -> serving.searchPipelines().transformResponse(pipelined, response);
        // What the request processors left, when there were any: the pipelined request is a copy, and its
        // source is the one every shard must be asked with.
        final SearchSourceBuilder source = pipelined == null ? parsedSource : pipelined.source();

        applyDefaults(source, counting);

        final String unsupported = whatCannotBeMerged(source);
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
            if (bodyPit != null && bodyPit.getKeepAlive() != null) {
                // OpenSearch's keep_alive on a search extends the view from now. This shell's expiry is
                // absolute, so extending it is a new record with a later deadline -- one write, and only
                // when it actually moves the deadline later, since a keep_alive shorter than what remains
                // asks for nothing.
                final long until = metadata.clock().getAsLong() + Math.min(
                    bodyPit.getKeepAlive().millis(),
                    PointInTimeHandler.MAX_KEEP_ALIVE_MILLIS
                );
                if (until > frozen.expiresAtMillis()) {
                    frozen = new org.opensearch.serverless.metadata.PointInTime(frozen.id(), frozen.index(), until, frozen.shards());
                    metadata.extendPointInTime(frozen);
                }
            }
            final var view = frozen;
            return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    respondFrozen(channel, serving, metadata, view, source, counting, request, failOnPartial, postProcess);
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
        final Resolution resolved = resolveIndices(metadata, serving, index, ignoreUnavailable);
        if (resolved.refused()) {
            return channel -> channel.sendResponse(IndexAdminHandler.error(channel, resolved.status(), resolved.type(), resolved.reason()));
        }

        // Off the HTTP thread. executeQueryPhase hands work to the search pool and this waits for it;
        // waiting on the transport thread that is meant to be reading the next request resets the
        // connection, which surfaces to the client as RST_STREAM rather than as anything diagnosable.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            // Membership is the address book and the placement input. Refreshed when the snapshot is
            // older than a fraction of a lease, off the HTTP thread: refreshing on every search was one
            // listing and a read per node per search, on the thread that should be reading the next one.
            try {
                metadata.membership().refreshIfOlderThan(Math.max(1_000L, metadata.leaseTtlMillis() / 2));
            } catch (Exception e) {
                logger.warn("could not refresh membership before searching", e);
            }
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
                    failOnPartial,
                    postProcess
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
     * The defaults a request gets where its body did not speak.
     *
     * <p>Shared with {@code _msearch}, so a search inside a batch is the same search it would be on its
     * own. {@code track_total_hits} is left exactly as the caller set it -- this used to force it on after
     * parsing, so a caller asking for a cheaper count got the expensive one and was told {@code eq}.
     *
     * @param source the parsed search
     * @param counting whether this is a {@code _count}, which fetches nothing and counts everything
     */
    static void applyDefaults(SearchSourceBuilder source, boolean counting) {
        if (counting) {
            // A count asks how many, not which. Fetching hits to throw them away would make the cheap
            // question cost the same as the expensive one, and a count is exact by definition.
            source.size(0);
            source.from(0);
            source.trackTotalHits(true);
            return;
        }
        if (source.size() < 0) {
            source.size(10);
        }
        if (source.from() < 0) {
            source.from(0);
        }
    }

    /**
     * Turns the names in a request into the indices to fan out over.
     *
     * <p>Extracted from the request path so {@code _msearch} resolves a line the way {@code _search}
     * resolves a request -- aliases, prefix patterns, {@code ignore_unavailable} and the refusals all
     * included. Before this, a multi-search silently dropped a named index that did not exist, which is the
     * confident narrower answer everything else on this surface refuses.
     *
     * <p>A comma-separated list is resolved by looking each name up, which costs one register read per
     * name and no listing. A prefix pattern costs one bounded listing on top of that -- a single
     * ListObjectsV2 with a maximum key count, whose cost is set by the cap rather than by the
     * population, so a deployment with a hundred million indices pays what one with ten pays.
     *
     * <p>That is why §6.3's refusal of enumeration does not refuse this. The rule was never "listings are
     * forbidden"; it was that a request must not cost the size of the deployment and must not answer
     * from a subset while looking complete. A capped listing costs a fixed amount, and a pattern that
     * matches more than the cap is refused rather than truncated.
     *
     * <p>A pattern that is not a prefix stays refused, and the reason is the same one: {@code *-2026}
     * cannot be answered by a listing at all, only by reading every name in the deployment and matching
     * each.
     *
     * @param metadata the metadata plane
     * @param serving the node, for its pattern cap
     * @param index the names as the caller wrote them, comma-separated
     * @param ignoreUnavailable whether a name that is not there is an expectation rather than a mistake
     * @return the indices and their shard counts, or the refusal
     * @throws IOException if a register cannot be read
     */
    static Resolution resolveIndices(MetadataPlane metadata, ServerlessNode serving, String index, boolean ignoreUnavailable)
        throws IOException {
        final java.util.List<String> requested = java.util.List.of(index.split(",", -1));
        for (String name : requested) {
            if (name.isEmpty()) {
                return Resolution.refuse(RestStatus.BAD_REQUEST, "unsupported_search", "an empty index name was given");
            }
            if (name.indexOf('?') >= 0 || isPrefixPattern(name) == false && name.indexOf('*') >= 0) {
                return Resolution.refuse(
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_search",
                    "only a trailing-wildcard pattern is supported, and ["
                        + name
                        + "] is not one: matching it would mean reading every index name in the "
                        + "deployment, which this system does not offer on a request path. Use a "
                        + "prefix like [logs-*], or name the indices you want."
                );
            }
        }

        // What each pattern matched, so a name that came from a pattern can be told from one the caller
        // typed. The distinction decides what a missing index means: a named index that is not there is a
        // mistake, and a pattern is a filter over what exists rather than an assertion that anything does.
        final java.util.List<String> names = new java.util.ArrayList<>();
        final java.util.Set<String> fromPattern = new java.util.HashSet<>();
        for (String name : requested) {
            if (isPrefixPattern(name) == false) {
                names.add(name);
                continue;
            }
            final String prefix = name.substring(0, name.length() - 1);
            final java.util.List<String> matched;
            try {
                matched = metadata.namesWithPrefix(prefix, serving.patternCap());
            } catch (org.opensearch.serverless.metadata.DescriptorStore.TooManyMatchesException e) {
                return Resolution.refuse(RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage());
            }
            for (String match : matched) {
                if (names.contains(match) == false) {
                    names.add(match);
                }
                fromPattern.add(match);
            }
        }
        // Whether a name that is not there is a mistake or an expectation.
        //
        // Refusing is the default and stays the default: a search over three indices where one does not
        // exist, answering 200 and looking complete, is the confident empty answer this surface exists to
        // avoid, and a typo is far more likely than an absence somebody planned for.
        //
        // But a caller searching yesterday's and today's index, on a day that has only just started, is
        // saying something different -- they know one of these may not exist yet and they mean it. That is
        // exactly what ignore_unavailable means everywhere else, and refusing to offer it forces them to
        // either guess or make two requests. It has to be asked for explicitly, which is what makes the
        // distinction real rather than a default nobody chose.
        for (String name : requested) {
            if (isPrefixPattern(name) && fromPattern.stream().noneMatch(match -> match.startsWith(name.substring(0, name.length() - 1)))) {
                if (ignoreUnavailable) {
                    continue;
                }
                // A pattern that matches nothing is a search over no indices, and answering it with zero
                // hits and a complete flag is the confident empty answer this surface exists to avoid. A
                // caller who means "whatever is there, possibly nothing" says so with ignore_unavailable,
                // the same way they do for a named index that may not exist yet.
                return Resolution.refuse(RestStatus.NOT_FOUND, "index_not_found", "no index matches [" + name + "]");
            }
        }
        final java.util.LinkedHashMap<String, IndexDescriptor> indices = new java.util.LinkedHashMap<>();
        final java.util.List<String> skipped = new java.util.ArrayList<>();
        for (String name : names) {
            // One read tells us whether the name is an index or an alias, because both live in the same
            // register. An alias is expanded here rather than deeper down, so everything below this line
            // deals only in indices and a shard count -- searching through an alias and searching the
            // indices it stands for are the same code path, which is the only way they cannot drift.
            final var resolved = metadata.resolve(name);
            if (resolved.absent()) {
                if (fromPattern.contains(name)) {
                    // A name the listing returned and the register does not describe: a tombstone left by
                    // a deletion, or an index deleted between the listing and the read. A pattern filters
                    // what exists, so this is not a match rather than a missing index -- and reporting it
                    // as skipped would tell the caller something is absent from an answer they never asked
                    // to include.
                    continue;
                }
                if (ignoreUnavailable) {
                    skipped.add(name);
                    continue;
                }
                return Resolution.refuse(RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name);
            }
            if (resolved.index() != null) {
                if (serving.isSystemIndex(name)) {
                    // Checked here, on the resolved name, and not only on the path parameter: a comma list,
                    // a prefix pattern or a body index used to walk straight past the guard.
                    return Resolution.refuse(
                        RestStatus.FORBIDDEN,
                        "system_index",
                        "[" + name + "] belongs to a plugin and is not reachable through the request path"
                    );
                }
                indices.put(name, resolved.index());
                continue;
            }
            for (String target : resolved.alias().indices()) {
                final Optional<IndexDescriptor> behind = metadata.describe(target);
                if (behind.isEmpty()) {
                    // An alias outliving one of its indices is ordinary: somebody deleted an index and did
                    // not update the alias. Skipping it silently would make a search over an alias quietly
                    // narrower than the caller believes, so it is refused unless they said to ignore what
                    // is missing -- the same rule a named index gets, since an alias is a way of naming
                    // indices and not a way of relaxing what naming one means.
                    if (ignoreUnavailable) {
                        skipped.add(target);
                        continue;
                    }
                    return Resolution.refuse(
                        RestStatus.NOT_FOUND,
                        "index_not_found",
                        "alias [" + name + "] names [" + target + "], which does not exist"
                    );
                }
                if (serving.isSystemIndex(target)) {
                    return Resolution.refuse(
                        RestStatus.FORBIDDEN,
                        "system_index",
                        "[" + target + "] belongs to a plugin and is not reachable through the request path"
                    );
                }
                indices.put(target, behind.get());
            }
        }
        if (indices.isEmpty()) {
            // Every name was absent. Answering 200 with no hits here would be the very thing the flag is
            // not for: the caller allowed for *some* of their indices to be missing, not all of them, and
            // an empty answer over nothing at all is indistinguishable from an empty answer over
            // everything.
            return Resolution.refuse(
                RestStatus.NOT_FOUND,
                "index_not_found",
                "none of the indices named exist: " + String.join(", ", names)
            );
        }
        return new Resolution(indices, skipped, null, null, null);
    }

    /**
     * Names the part of a search this cannot answer correctly across shards, or null if it can.
     *
     * <p><b>Refused rather than ignored, and the distinction is the whole of D2.</b> Every feature below
     * parses happily and runs happily on a single shard; what none of them survives is being run on
     * several shards and having the answers concatenated. An aggregation needs a reduce step this does
     * not have — returning one shard's buckets as if they were the index's would be a number that is
     * wrong and looks right, which is the worst thing this system could hand anybody. A sort needs the
     * merge to compare sort values rather than scores. {@code search_after} and {@code collapse} need
     * coordination across shards that does not exist here.
     *
     * <p>Cheap to lift later, one at a time, and each will need its own merge and its own test. Until
     * then the honest answer to "can you do this" is no.
     *
     * @param source the parsed search
     * @return why it cannot be served, or null
     */
    static String whatCannotBeMerged(SearchSourceBuilder source) {
        if (source.searchAfter() != null) {
            // The two things that make a cursor a cursor. Without a sort there is no order for "after" to
            // be after, and OpenSearch's own answer to search_after with from is the same refusal: the
            // cursor is the offset, and having both is a request that means two different things at once.
            if (source.sorts() == null || source.sorts().isEmpty()) {
                return "search_after needs a sort: without one there is no order for a cursor to be a position in";
            }
            if (source.from() > 0) {
                return "search_after and from cannot both be given: the cursor is the offset";
            }
            if (source.searchAfter().length != source.sorts().size()) {
                return "search_after must carry one value per sort key, and this carries "
                    + source.searchAfter().length
                    + " for "
                    + source.sorts().size();
            }
        }
        if (source.collapse() != null) {
            return "collapse is not supported: it would have to be applied across shards, not within one";
        }
        if (source.suggest() != null) {
            return "suggest is not supported";
        }
        if (source.profile()) {
            return "profile is not supported";
        }
        if (source.slice() != null) {
            // Refused here rather than left to fail on the shard, where Lucene's own message -- "slice
            // cannot be used outside of a scroll context or PIT context" -- names two things this surface
            // does not have in the terms core has them.
            return "slice is not supported: a sliced search partitions one scroll or point-in-time context, and a "
                + "search here runs over each shard's own reader rather than over a context it could partition";
        }
        if (source.indexBoosts() != null && source.indexBoosts().isEmpty() == false) {
            // The per-index boost is fixed at 1.0 where each shard's request is built, so accepting one
            // would rank a multi-index search as if it had not been given.
            return "indices_boost is not supported: every shard is queried at the same boost";
        }
        return null;
    }

    /**
     * Runs the search on every shard and merges the answers.
     *
     * <p><b>Merged by score, then cut to {@code size}.</b> This used to append each shard's hits in the
     * order the shards were visited and return all of them, so a three-shard search for {@code size=10}
     * returned up to thirty hits ordered by shard number — the first page of a relevance-ranked search
     * that was neither the first page nor relevance-ranked. Each shard is asked for {@code from + size}
     * because any one of them might own the whole page, and the global window is applied here, once.
     */
    /**
     * Runs a search through the plugins' action filters.
     *
     * @throws IOException if the search fails; a filter's own refusal is a runtime exception and passes
     *     through carrying the plugin's status
     */
    static <T> T gated(
        ServerlessNode serving,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        try {
            return serving.actionGate()
                .run(
                    org.opensearch.action.search.SearchAction.NAME,
                    // Every index the search covers, so a filter evaluating privileges sees all of them
                    // rather than the first one named.
                    new org.opensearch.action.search.SearchRequest(indices.toArray(new String[0]), source),
                    // The source as the filters left it: a document-level security query a filter folded
                    // into the request reaches every shard, rather than being silently ignored.
                    admitted -> work.apply(
                        admitted instanceof org.opensearch.action.search.SearchRequest rewritten && rewritten.source() != null
                            ? rewritten.source()
                            : source
                    ),
                    searchView()
                );
        } catch (IOException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * How a search result is shown to a filter, and how the filter's answer is read back.
     *
     * <p><b>This is what makes field- and document-level security possible here.</b> A filter that wraps
     * the listener now receives a real {@code SearchResponse} and may hand back a different one — with
     * hits removed, or with fields redacted from their source — and that is the answer the caller gets.
     * Before this, every operation reported only that it had happened, so a filter could refuse a search
     * and could not change one.
     *
     * <p><b>Coverage is not the filter's to change, and is taken from the outcome either way.</b> How many
     * shards answered is a fact about this node's fan-out; a filter that dropped hits has not made an
     * index unreachable, and letting a rewritten response carry its own shard counts would let a redaction
     * masquerade as a partial answer.
     *
     * <p>The generic type is erased at the seam — the gate is generic over what the work returns, and only
     * a search passes this view — so the cast is checked by the one call site rather than by the compiler.
     */
    @SuppressWarnings("unchecked")
    private static <T> org.opensearch.serverless.shell.ActionGate.ResponseView<T> searchView() {
        return (org.opensearch.serverless.shell.ActionGate.ResponseView<T>) SEARCH_VIEW;
    }

    private static final org.opensearch.serverless.shell.ActionGate.ResponseView<
        org.opensearch.serverless.shard.ShardOperations.SearchOutcome> SEARCH_VIEW =
            new org.opensearch.serverless.shell.ActionGate.ResponseView<>() {

                @Override
                public org.opensearch.core.action.ActionResponse show(
                    org.opensearch.serverless.shard.ShardOperations.SearchOutcome outcome
                ) {
                    return toResponse(outcome, null, 0L, null);
                }

                @Override
                public org.opensearch.serverless.shard.ShardOperations.SearchOutcome read(
                    org.opensearch.core.action.ActionResponse response,
                    org.opensearch.serverless.shard.ShardOperations.SearchOutcome original
                ) {
                    if (response instanceof org.opensearch.action.search.SearchResponse answered) {
                        final org.apache.lucene.search.TotalHits total = answered.getHits().getTotalHits();
                        return new org.opensearch.serverless.shard.ShardOperations.SearchOutcome(
                            total == null ? original.total() : total.value(),
                            java.util.List.of(answered.getHits().getHits()),
                            original.shards(),
                            original.answered(),
                            (org.opensearch.search.aggregations.InternalAggregations) answered.getAggregations(),
                            total == null ? original.relation() : total.relation(),
                            answered.getHits().getMaxScore(),
                            answered.isTimedOut(),
                            answered.isTerminatedEarly(),
                            java.util.List.of(answered.getShardFailures())
                        );
                    }
                    // A filter that replaced the response with something that is not a search answer.
                    // Taking the original would silently undo whatever it meant to do, so this refuses
                    // instead: a filter doing something the shell cannot honour must be visible.
                    throw new IllegalStateException(
                        "an action filter answered a search with " + response.getClass().getName() + ", which is not a search response"
                    );
                }
            };

    /**
     * Reports whether a name is a pattern this system can answer.
     *
     * <p>One trailing star and nothing else, because that is exactly what a prefix listing can do. A star
     * anywhere else needs the whole population read and matched, which is the operation §6.3 refuses; a
     * bare {@code *} is a prefix of nothing at all and is allowed, since the cap bounds it like any other.
     *
     * @param name the name as the caller wrote it
     * @return true if it is a trailing-wildcard pattern
     */
    private static boolean isPrefixPattern(String name) {
        return name.endsWith("*") && name.indexOf('*') == name.length() - 1;
    }

    private void respondFrozen(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.metadata.PointInTime pit,
        SearchSourceBuilder source,
        boolean counting,
        org.opensearch.core.xcontent.ToXContent.Params params,
        boolean failOnPartial,
        java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess
    ) throws IOException {
        final long startNanos = System.nanoTime();
        final var outcome = gated(
            serving,
            java.util.List.of(pit.index()),
            source,
            admitted -> SearchFanout.runFrozen(serving, metadata, pit, admitted)
        );
        render(
            channel,
            java.util.List.of(),
            outcome,
            source,
            tookMillis(startNanos),
            counting,
            pit.id(),
            params,
            failOnPartial,
            postProcess
        );
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Map<String, IndexDescriptor> indices,
        java.util.List<String> skipped,
        SearchSourceBuilder source,
        boolean counting,
        org.opensearch.core.xcontent.ToXContent.Params params,
        boolean failOnPartial,
        java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess
    ) throws IOException {
        final long startNanos = System.nanoTime();
        // Filtered here for the same reason DocumentHandler is: this handler formats over the shared
        // fan-out rather than going through ShardOperations, so the gate has to meet it where it works.
        final var outcome = gated(serving, indices.keySet(), source, admitted -> SearchFanout.run(serving, metadata, indices, admitted));
        render(channel, skipped, outcome, source, tookMillis(startNanos), counting, null, params, failOnPartial, postProcess);
    }

    /**
     * Wall-clock elapsed since a search began, for the {@code took} field real OpenSearch always reports.
     *
     * <p>Measured around the fan-out only -- parsing and validating the request happens before this is
     * called, the same boundary a classic node's own {@code took} is measured from.
     *
     * @param startNanos {@link System#nanoTime()} at the moment the fan-out began
     * @return elapsed milliseconds
     */
    private static long tookMillis(long startNanos) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    /**
     * Builds the response a classic node would build from what the fan-out found.
     *
     * <p><b>Core's own response object, so core's own renderer.</b> This shell used to write a search
     * response field by field, and every field it did not write was silently absent: {@code highlight},
     * {@code _explanation}, {@code _version}, {@code matched_queries}, {@code inner_hits} were all
     * computed on the shard and dropped here. A real {@link org.opensearch.action.search.SearchResponse}
     * renders itself, honours {@code typed_keys} and {@code rest_total_hits_as_int}, writes
     * {@code _score: null} for an unscored hit the way every client's parser expects, and lists the shard
     * failures it was given. It is also what a plugin's {@code Client} hands back, so the two agree by
     * construction.
     *
     * <p>{@code hits.total} is omitted when the caller disabled counting, exactly as core omits it.
     *
     * @param outcome what the fan-out found
     * @param source what was asked, for whether the total was tracked; null means it was
     * @param tookMillis how long it took
     * @param pitId the view searched, or null
     * @return the response
     */
    public static org.opensearch.action.search.SearchResponse toResponse(
        org.opensearch.serverless.shard.ShardOperations.SearchOutcome outcome,
        SearchSourceBuilder source,
        long tookMillis,
        String pitId
    ) {
        final boolean tracked = source == null
            || source.trackTotalHitsUpTo() == null
            || source.trackTotalHitsUpTo() != org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED;
        final org.opensearch.search.SearchHits hits = new org.opensearch.search.SearchHits(
            outcome.hits().toArray(new SearchHit[0]),
            tracked ? new org.apache.lucene.search.TotalHits(outcome.total(), outcome.relation()) : null,
            outcome.maxScore()
        );
        final var internal = new org.opensearch.search.internal.InternalSearchResponse(
            hits,
            outcome.aggregations(),
            null,
            null,
            outcome.timedOut(),
            outcome.terminatedEarly(),
            1
        );
        return new org.opensearch.action.search.SearchResponse(
            internal,
            null,
            outcome.shards(),
            outcome.answered(),
            0,
            tookMillis,
            outcome.failures().toArray(new org.opensearch.action.search.ShardSearchFailure[0]),
            org.opensearch.action.search.SearchResponse.Clusters.EMPTY,
            pitId
        );
    }

    /**
     * Writes the answer, wherever it came from.
     *
     * <p>Shared by the ordinary search and the frozen one, because a caller must not be able to tell which
     * path answered from the shape of the response -- the difference between them is which commits were
     * read, not what a result looks like.
     */
    private void render(
        org.opensearch.rest.RestChannel channel,
        java.util.List<String> skipped,
        org.opensearch.serverless.shard.ShardOperations.SearchOutcome outcome,
        SearchSourceBuilder source,
        long tookMillis,
        boolean counting,
        String pitId,
        org.opensearch.core.xcontent.ToXContent.Params params,
        boolean failOnPartial,
        java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess
    ) throws IOException {
        if (failOnPartial && outcome.failures().isEmpty() == false) {
            // allow_partial_search_results=false: the caller said a partial answer is no answer. The
            // first shard's own failure is the response, with its own status, which is what a classic
            // coordinator does with the same flag.
            final Throwable cause = outcome.failures().get(0).getCause();
            channel.sendResponse(new BytesRestResponse(channel, cause instanceof Exception e ? e : new IOException(cause)));
            return;
        }
        final org.opensearch.action.search.SearchResponse response = postProcess.apply(toResponse(outcome, source, tookMillis, pitId));
        if (counting) {
            renderCount(channel, response, source, params);
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            renderInto(builder, skipped, response, outcome.complete(), params);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Writes a count, which is a search's total without its hits.
     *
     * <p>The {@code _shards} block is core's own broadcast header, failures included, and for the same
     * reason a search reports one: a count assembled from some of the shards is a different number from a
     * count assembled from all of them, and a caller must be able to tell.
     */
    private void renderCount(
        org.opensearch.rest.RestChannel channel,
        org.opensearch.action.search.SearchResponse response,
        SearchSourceBuilder source,
        org.opensearch.core.xcontent.ToXContent.Params params
    ) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            if (source.terminateAfter() != org.opensearch.search.internal.SearchContext.DEFAULT_TERMINATE_AFTER) {
                builder.field("terminated_early", response.isTerminatedEarly());
            }
            builder.field("count", response.getHits().getTotalHits().value());
            org.opensearch.rest.action.RestActions.buildBroadcastShardsHeader(
                builder,
                params,
                response.getTotalShards(),
                response.getSuccessfulShards(),
                0,
                response.getFailedShards(),
                response.getShardFailures()
            );
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Writes one search's answer into a builder the caller owns, without the enclosing braces.
     *
     * <p>Extracted so {@code _msearch} produces the same body inside its array that {@code _search} produces
     * on its own -- and can add its per-item {@code status} inside the same object. A second renderer would
     * agree with this one until somebody changed one of them, and the difference would surface as two
     * response shapes for the same query.
     *
     * <p>Two fields follow core's: {@code complete} has no real-OpenSearch equivalent -- a classic
     * coordinating node always fans out to every shard it can resolve, so "how much of the index answered"
     * is not a question it ever has to answer -- and {@code skipped_indices} names what
     * {@code ignore_unavailable} dropped, which {@code _shards.skipped} (a shard count) does not.
     *
     * @param builder the builder to write into, positioned inside an open object
     * @param skipped indices a pattern named that do not exist
     * @param response the response, as core would have built it
     * @param complete whether every shard answered
     * @param params the request's rendering parameters, for {@code typed_keys} and friends
     * @throws IOException if writing fails
     */
    static void renderInto(
        XContentBuilder builder,
        java.util.List<String> skipped,
        org.opensearch.action.search.SearchResponse response,
        boolean complete,
        org.opensearch.core.xcontent.ToXContent.Params params
    ) throws IOException {
        response.innerToXContent(builder, params);
        builder.field("complete", complete);
        if (skipped.isEmpty() == false) {
            builder.startArray("skipped_indices");
            for (String name : skipped) {
                builder.value(name);
            }
            builder.endArray();
        }
    }
}
