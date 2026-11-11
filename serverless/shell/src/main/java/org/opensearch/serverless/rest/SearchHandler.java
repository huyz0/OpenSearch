/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code GET|POST /{index}/_search} — querying the shards this node can actually reach.
 *
 * <p><b>The response always reports shard coverage, and that is the point of this class.</b> A node
 * serves the shards it holds; with no cross-node fan-out yet it may hold some of an index and not the
 * rest. Returning a hit count without saying how much of the index it came from would be the exact
 * "confident empty answer" failure — a confident answer computed over a fraction of the
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
    private final SearchPlanner planner;

    /**
     * Creates the handler.
     *
     * @param node supplies the node serving the request
     * @param plane supplies the metadata plane
     */
    public SearchHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
        this.planner = new SearchPlanner(node, plane);
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
     * @param prunedShards shards of pattern matches the digest rollups ruled out, which are not in {@code indices}
     * @param judged indices the rollups already found may match, which the fan-out need not check shard by shard
     */
    public record Resolution(java.util.LinkedHashMap<String, IndexDescriptor> indices, java.util.List<String> skipped, RestStatus status,
        String type, String reason, int prunedShards, java.util.Set<String> judged) {

        /**
         * A resolution with nothing ruled out by the rollups.
         *
         * @param indices each index and its shard count, in request order
         * @param skipped names {@code ignore_unavailable} dropped
         * @param status the refusal's status, or null
         * @param type the refusal's error type, or null
         * @param reason the refusal's reason, or null
         */
        public Resolution(
            java.util.LinkedHashMap<String, IndexDescriptor> indices,
            java.util.List<String> skipped,
            RestStatus status,
            String type,
            String reason
        ) {
            this(indices, skipped, status, type, reason, 0, java.util.Set.of());
        }

        static Resolution refuse(RestStatus status, String type, String reason) {
            return new Resolution(null, null, status, type, reason);
        }

        /**
         * Reports whether the names could not be resolved.
         *
         * @return true when this carries a refusal rather than indices
         */
        public boolean refused() {
            return status != null;
        }
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean counting = request.path().endsWith("/_count");
        // Read here as well as in plan(): plan runs after this method returns, and BaseRestHandler checks
        // for unread parameters in between.
        request.param("pit");
        request.param("allow_partial_activation");

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
            // The body, or the source= parameter core's own search action accepts in its place -- a GET
            // with the search in the URL, which some proxies and clients insist on. Parsed with the search
            // registry either way, since the request's own parser does not know the query names.
            if (request.hasContentOrSourceParam()) {
                final org.opensearch.common.collect.Tuple<
                    org.opensearch.core.xcontent.MediaType,
                    org.opensearch.core.common.bytes.BytesReference> body = request.contentOrSourceParam();
                try (
                    XContentParser parser = body.v1()
                        .xContent()
                        .createParser(
                            serving.services().searchXContentRegistry(),
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            body.v2().streamInput()
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

    RestChannelConsumer plan(
        RestRequest request,
        org.opensearch.action.search.SearchRequest searchRequest,
        SearchSourceBuilder parsedSource,
        org.opensearch.search.builder.PointInTimeBuilder bodyPit,
        boolean counting
    ) throws IOException {
        return planner.plan(request, searchRequest, parsedSource, bodyPit, counting)::accept;
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
    public static void applyDefaults(SearchSourceBuilder source, boolean counting) {
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
     * <p>That is why the rule against enumerating on a request path does not refuse this. The rule was never "listings are
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
    public static Resolution resolveIndices(MetadataPlane metadata, ServerlessNode serving, String index, boolean ignoreUnavailable)
        throws IOException {
        return SearchIndexResolver.resolveIndices(metadata, serving, index, ignoreUnavailable);
    }

    /**
     * Resolves names for a caller that may or may not be on the request path.
     *
     * <p>A plugin's index is unreachable through REST and is exactly what the plugin's own client exists
     * to reach, so the guard is the one thing that differs between the two callers of this method; the
     * resolution of lists, aliases and patterns is deliberately the same code for both.
     *
     * @param metadata the metadata plane
     * @param serving the node, for its pattern cap
     * @param index the names as the caller wrote them, comma-separated
     * @param ignoreUnavailable whether a name that is not there is an expectation rather than a mistake
     * @param throughRequestPath whether a plugin's index is out of reach for this caller
     * @return the indices and their shard counts, or the refusal
     * @throws IOException if a register cannot be read
     */
    public static Resolution resolveIndices(
        MetadataPlane metadata,
        ServerlessNode serving,
        String index,
        boolean ignoreUnavailable,
        boolean throughRequestPath
    ) throws IOException {
        return SearchIndexResolver.resolveIndices(metadata, serving, index, ignoreUnavailable, throughRequestPath);
    }

    /**
     * Resolves names, dropping the pattern matches the digest rollups prove the search cannot match.
     *
     * <p>A wide pattern used to cost a descriptor read per matching index before a shard was asked anything,
     * and then a head and a manifest read per shard to prune it. With the rollups, a pattern whose prefix spans
     * a whole group reads that group's {@value org.opensearch.serverless.metadata.DigestRollups#BUCKETS}
     * registers and drops every index they rule out -- no descriptor, head or manifest read for it at all --
     * counting its shards as skipped. At least one match is always kept, so a search that rules out everything
     * still has a shard to give its aggregations their shape.
     *
     * @param metadata the metadata plane
     * @param serving the node
     * @param index the names as the caller wrote them, comma-separated
     * @param ignoreUnavailable whether a name that is not there is an expectation rather than a mistake
     * @param throughRequestPath whether a plugin's index is out of reach for this caller
     * @param source the search, whose query the rollups judge; null to rule nothing out
     * @param nowMillis the request's single now, which the fan-out must use too
     * @return the indices and their shard counts, or the refusal
     * @throws IOException if a register cannot be read
     */
    public static Resolution resolveIndices(
        MetadataPlane metadata,
        ServerlessNode serving,
        String index,
        boolean ignoreUnavailable,
        boolean throughRequestPath,
        SearchSourceBuilder source,
        long nowMillis
    ) throws IOException {
        return SearchIndexResolver.resolveIndices(metadata, serving, index, ignoreUnavailable, throughRequestPath, source, nowMillis);
    }

    /**
     * Names the part of a search this cannot answer correctly across shards, or null if it can.
     *
     * <p><b>Refused rather than ignored, and the distinction is the whole of the allowlist rule.</b> Every feature below
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
    public static String whatCannotBeMerged(SearchSourceBuilder source) {
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
     * @param <T> what the work produces
     * @param serving the node whose filters run
     * @param indices every index the search covers
     * @param source the search as parsed
     * @param work the search, given the source as the filters left it
     * @return what the work produced, as the filters left it
     * @throws IOException if the search fails; a filter's own refusal is a runtime exception and passes
     *     through carrying the plugin's status
     */
    public static <T> T gated(
        ServerlessNode serving,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return SearchGate.gated(serving, indices, source, work);
    }

    /**
     * Runs filters over a native request while preserving its options and resolved authorization scope.
     *
     * @param serving the serving node
     * @param nativeRequest the original native request, or null for an already planned REST request
     * @param indices every resolved index the search covers
     * @param source the planned source
     * @param work the search receiving the admitted source
     * @param <T> the result type
     * @return the filtered search result
     * @throws IOException if the search fails
     */
    public static <T> T gated(
        ServerlessNode serving,
        org.opensearch.action.search.SearchRequest nativeRequest,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return SearchGate.gated(serving, nativeRequest, indices, source, work);
    }

    /**
     * Runs an already planned search with the caller's partial-results policy visible to filters.
     *
     * @param serving the serving node
     * @param indices the resolved authorization scope
     * @param source the planned source, including any already applied search pipeline
     * @param partialResults the caller's partial-results policy, or null for the default
     * @param work the search receiving the admitted source
     * @param <T> the result type
     * @return the filtered result
     * @throws IOException if the search fails
     */
    public static <T> T gated(
        ServerlessNode serving,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        Boolean partialResults,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return SearchGate.gated(serving, indices, source, partialResults, work);
    }

    static <T> T gated(
        ServerlessNode serving,
        org.opensearch.action.search.SearchRequest nativeRequest,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        Boolean partialResults,
        org.opensearch.common.CheckedFunction<SearchSourceBuilder, T, Exception> work
    ) throws IOException {
        return SearchGate.gated(serving, nativeRequest, indices, source, partialResults, work);
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
        org.opensearch.serverless.shard.SearchOutcome outcome,
        SearchSourceBuilder source,
        long tookMillis,
        String pitId
    ) {
        return SearchRendering.toResponse(outcome, source, tookMillis, pitId);
    }

}
