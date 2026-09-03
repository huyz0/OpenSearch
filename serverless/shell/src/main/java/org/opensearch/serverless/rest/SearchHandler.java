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

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter read before any early return, or BaseRestHandler turns a refusal into a 400
        // about an unconsumed parameter instead of the one being made here.
        // A bare /_search names no index, and means every index -- which this shell already expresses as a
        // prefix pattern, resolved by one bounded listing.
        final String index = request.param("index") == null ? "*" : request.param("index");
        final boolean counting = request.path().endsWith("/_count");
        final String q = request.param("q");
        final int sizeParam = request.paramAsInt("size", 10);
        final int fromParam = request.paramAsInt("from", 0);
        final boolean hasBody = request.hasContent();

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        final ServerlessNode serving = node.get();
        final SearchSourceBuilder source;
        if (hasBody) {
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(
                        serving.searchXContentRegistry(),
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        request.content().streamInput()
                    )
            ) {
                source = SearchSourceBuilder.fromXContent(parser, true);
            } catch (Exception e) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "bad_query",
                        "could not parse the search body: " + e.getMessage()
                    )
                );
            }
        } else if (q != null && q.isBlank() == false) {
            // Core's own query-string parser, which is what "q=" means in OpenSearch. It was a hand-rolled
            // split on the first colon, so "q=field:value" worked and "q=hello" -- a bare term across all
            // fields, the simplest thing a caller can type -- was a 400. The colon form still parses, and
            // now parses as the query-string syntax it always looked like rather than as a lookalike.
            source = new SearchSourceBuilder().query(org.opensearch.index.query.QueryBuilders.queryStringQuery(q));
        } else if (counting) {
            // A count with neither a body nor a q= is "how many documents are there", which is a
            // well-formed question and the most common way the endpoint is called.
            source = new SearchSourceBuilder().query(org.opensearch.index.query.QueryBuilders.matchAllQuery());
        } else {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "send a search body, or a q= query string")
            );
        }

        if (counting) {
            // A count asks how many, not which. Fetching hits to throw them away would make the cheap
            // question cost the same as the expensive one.
            source.size(0);
            source.from(0);
        }

        // Defaults that belong to the request rather than to the shard, applied only where the body did
        // not speak. A body's own size wins; the query parameter is the fallback it always was.
        if (counting == false && source.size() < 0) {
            source.size(sizeParam);
        }
        if (source.from() < 0) {
            source.from(fromParam);
        }
        source.trackTotalHits(true);

        final String unsupported = whatCannotBeMerged(source);
        if (unsupported != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_search", unsupported)
            );
        }

        // A frozen view answers for itself: it already knows which index and which shards, so none of the
        // name resolution below applies to it.
        final String pitId = request.param("pit");
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
            final var frozen = pit.get();
            return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
                try {
                    respondFrozen(channel, serving, metadata, frozen, source, counting);
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (IOException nested) {
                        logger.error("failed to report a frozen search failure", nested);
                    }
                }
            });
        }

        // Several indices, named one by one or matched by a prefix.
        //
        // A comma-separated list is resolved by looking each name up, which costs one register read per
        // name and no listing. A prefix pattern costs one bounded listing on top of that -- a single
        // ListObjectsV2 with a maximum key count, whose cost is set by the cap rather than by the
        // population, so a deployment with a hundred million indices pays what one with ten pays.
        //
        // That is why §6.3's refusal of enumeration does not refuse this. The rule was never "listings are
        // forbidden"; it was that a request must not cost the size of the deployment and must not answer
        // from a subset while looking complete. A capped listing costs a fixed amount, and a pattern that
        // matches more than the cap is refused rather than truncated.
        //
        // A pattern that is not a prefix stays refused, and the reason is the same one: `*-2026` cannot be
        // answered by a listing at all, only by reading every name in the deployment and matching each.
        final java.util.List<String> requested = java.util.List.of(index.split(",", -1));
        for (String name : requested) {
            if (name.isEmpty()) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "unsupported_search", "an empty index name was given")
                );
            }
            if (name.indexOf('?') >= 0 || isPrefixPattern(name) == false && name.indexOf('*') >= 0) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_search",
                        "only a trailing-wildcard pattern is supported, and ["
                            + name
                            + "] is not one: matching it would mean reading every index name in the "
                            + "deployment, which this system does not offer on a request path. Use a "
                            + "prefix like [logs-*], or name the indices you want."
                    )
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
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "too_many_indices", e.getMessage())
                );
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
        // saying something different — they know one of these may not exist yet and they mean it. That is
        // exactly what ignore_unavailable means everywhere else, and refusing to offer it forces them to
        // either guess or make two requests. It has to be asked for explicitly, which is what makes the
        // distinction real rather than a default nobody chose.
        final boolean ignoreUnavailable = request.paramAsBoolean("ignore_unavailable", false);
        for (String name : requested) {
            if (isPrefixPattern(name) && fromPattern.stream().noneMatch(match -> match.startsWith(name.substring(0, name.length() - 1)))) {
                if (ignoreUnavailable) {
                    continue;
                }
                // A pattern that matches nothing is a search over no indices, and answering it with zero
                // hits and a complete flag is the confident empty answer this surface exists to avoid. A
                // caller who means "whatever is there, possibly nothing" says so with ignore_unavailable,
                // the same way they do for a named index that may not exist yet.
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no index matches [" + name + "]")
                );
            }
        }
        final java.util.LinkedHashMap<String, Integer> indices = new java.util.LinkedHashMap<>();
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
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + name)
                );
            }
            if (resolved.index() != null) {
                indices.put(name, resolved.index().numberOfShards());
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
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_FOUND,
                            "index_not_found",
                            "alias [" + name + "] names [" + target + "], which does not exist"
                        )
                    );
                }
                indices.put(target, behind.get().numberOfShards());
            }
        }
        if (indices.isEmpty()) {
            // Every name was absent. Answering 200 with no hits here would be the very thing the flag is
            // not for: the caller allowed for *some* of their indices to be missing, not all of them, and
            // an empty answer over nothing at all is indistinguishable from an empty answer over
            // everything.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_FOUND,
                    "index_not_found",
                    "none of the indices named exist: " + String.join(", ", names)
                )
            );
        }

        // Membership is the address book and the placement input, so refresh once per search rather
        // than per shard.
        try {
            metadata.membership().refresh();
        } catch (Exception e) {
            logger.warn("could not refresh membership before searching", e);
        }

        // Off the HTTP thread. executeQueryPhase hands work to the search pool and this waits for it;
        // waiting on the transport thread that is meant to be reading the next request resets the
        // connection, which surfaces to the client as RST_STREAM rather than as anything diagnosable.
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                respond(channel, serving, metadata, indices, skipped, source, counting);
            } catch (Exception e) {
                try {
                    channel.sendResponse(new BytesRestResponse(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a search failure", nested);
                }
            }
        });
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
    private static String whatCannotBeMerged(SearchSourceBuilder source) {
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
    private static <T> T gated(
        ServerlessNode serving,
        java.util.Collection<String> indices,
        SearchSourceBuilder source,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws IOException {
        try {
            return serving.actionGate()
                .run(
                    org.opensearch.action.search.SearchAction.NAME,
                    // Every index the search covers, so a filter evaluating privileges sees all of them
                    // rather than the first one named.
                    new org.opensearch.action.search.SearchRequest(indices.toArray(new String[0]), source),
                    work,
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
                    final SearchHit[] hits = outcome.hits().toArray(new SearchHit[0]);
                    final org.opensearch.search.SearchHits searchHits = new org.opensearch.search.SearchHits(
                        hits,
                        new org.apache.lucene.search.TotalHits(outcome.total(), org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                        Float.NaN
                    );
                    final var internal = new org.opensearch.search.internal.InternalSearchResponse(
                        searchHits,
                        outcome.aggregations(),
                        null,
                        null,
                        false,
                        null,
                        1
                    );
                    return new org.opensearch.action.search.SearchResponse(
                        internal,
                        null,
                        outcome.shards(),
                        outcome.answered(),
                        0,
                        0L,
                        org.opensearch.action.search.ShardSearchFailure.EMPTY_ARRAY,
                        org.opensearch.action.search.SearchResponse.Clusters.EMPTY
                    );
                }

                @Override
                public org.opensearch.serverless.shard.ShardOperations.SearchOutcome read(
                    org.opensearch.core.action.ActionResponse response,
                    org.opensearch.serverless.shard.ShardOperations.SearchOutcome original
                ) {
                    if (response instanceof org.opensearch.action.search.SearchResponse answered) {
                        return new org.opensearch.serverless.shard.ShardOperations.SearchOutcome(
                            answered.getHits().getTotalHits() == null ? original.total() : answered.getHits().getTotalHits().value(),
                            java.util.List.of(answered.getHits().getHits()),
                            original.shards(),
                            original.answered(),
                            (org.opensearch.search.aggregations.InternalAggregations) answered.getAggregations()
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
        boolean counting
    ) throws IOException {
        final long startNanos = System.nanoTime();
        final var outcome = gated(
            serving,
            java.util.List.of(pit.index()),
            source,
            () -> SearchFanout.runFrozen(serving, metadata, pit, source)
        );
        render(channel, java.util.Map.of(pit.index(), pit.shards().size()), java.util.List.of(), outcome, tookMillis(startNanos), counting);
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        java.util.Map<String, Integer> indices,
        java.util.List<String> skipped,
        SearchSourceBuilder source,
        boolean counting
    ) throws IOException {
        final long startNanos = System.nanoTime();
        // Filtered here for the same reason DocumentHandler is: this handler formats over the shared
        // fan-out rather than going through ShardOperations, so the gate has to meet it where it works.
        final var outcome = gated(serving, indices.keySet(), source, () -> SearchFanout.run(serving, metadata, indices, source));
        render(channel, indices, skipped, outcome, tookMillis(startNanos), counting);
    }

    /**
     * Wall-clock elapsed since a search began, for the {@code took} field real OpenSearch always reports.
     *
     * <p>Measured around the fan-out only — parsing and validating the request happens before this is
     * called, the same boundary a classic node's own {@code took} is measured from.
     *
     * @param startNanos {@link System#nanoTime()} at the moment the fan-out began
     * @return elapsed milliseconds
     */
    private static long tookMillis(long startNanos) {
        return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    /**
     * Writes a count, which is a search's total without its hits.
     *
     * <p>{@code count} is exact rather than a lower bound, because this shell always tracks the total —
     * there is no {@code track_total_hits} ceiling to hit. The {@code _shards} block is the same one a
     * search reports, and for the same reason: a count assembled from some of the shards is a different
     * number from a count assembled from all of them, and a caller must be able to tell.
     *
     * @param channel the channel to answer on
     * @param outcome what the fan-out returned
     * @throws IOException if writing fails
     */
    private void renderCount(org.opensearch.rest.RestChannel channel, org.opensearch.serverless.shard.ShardOperations.SearchOutcome outcome)
        throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("count", outcome.total());
            builder.startObject("_shards");
            builder.field("total", outcome.shards());
            builder.field("successful", outcome.answered());
            builder.field("skipped", 0);
            builder.field("failed", outcome.shards() - outcome.answered());
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Writes the answer, wherever it came from.
     *
     * <p>Shared by the ordinary search and the frozen one, because a caller must not be able to tell which
     * path answered from the shape of the response — the difference between them is which commits were
     * read, not what a result looks like.
     */
    private void render(
        org.opensearch.rest.RestChannel channel,
        java.util.Map<String, Integer> indices,
        java.util.List<String> skipped,
        org.opensearch.serverless.shard.ShardOperations.SearchOutcome outcome,
        long tookMillis,
        boolean counting
    ) throws IOException {
        if (counting) {
            renderCount(channel, outcome);
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            renderInto(builder, indices, skipped, outcome, tookMillis);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Writes one search's answer into a builder the caller owns.
     *
     * <p>Extracted so {@code _msearch} produces the same body inside its array that {@code _search} produces
     * on its own. A second renderer would agree with this one until somebody changed one of them, and the
     * difference would surface as two response shapes for the same query.
     *
     * @param builder the builder to write into, positioned where an object may start
     * @param indices the indices searched, and how many shards each contributed
     * @param skipped indices a pattern named that do not exist
     * @param outcome what the fan-out returned
     * @param tookMillis how long it took
     * @throws IOException if writing fails
     */
    static void renderInto(
        XContentBuilder builder,
        java.util.Map<String, Integer> indices,
        java.util.List<String> skipped,
        org.opensearch.serverless.shard.ShardOperations.SearchOutcome outcome,
        long tookMillis
    ) throws IOException {
        {
            builder.startObject();
            builder.field("took", tookMillis);
            // Always false: nothing on this path enforces a search timeout, so there is nothing that could
            // make this true yet. Stated rather than omitted, because a caller checking this field before
            // trusting a result must see an honest answer, not an absent key that happens to read as falsy.
            builder.field("timed_out", false);
            builder.startObject("_shards");
            builder.field("total", outcome.shards());
            // successful/failed, real OpenSearch's own field names -- searched/unreachable were this
            // shell's own names for the identical count and are gone now, not aliased: a client reading
            // the real names must get real values, not a second, shell-specific spelling to maintain.
            builder.field("successful", outcome.answered());
            builder.field("skipped", 0);
            builder.field("failed", outcome.shards() - outcome.answered());
            builder.endObject();
            // Stated, not implied. A caller reading only "total" would otherwise have no way to tell a
            // complete answer from one computed over a fraction of the index. No real-OpenSearch field
            // says this; a coordinating node there always fans out to every shard it can resolve, so
            // "how much of the index answered" is not a question a classic search ever has to answer.
            builder.field("complete", outcome.complete());
            if (skipped.isEmpty() == false) {
                // Named, not merely counted -- _shards.skipped above is real OpenSearch's count (always 0
                // here, since nothing on this path skips a shard the way a throttled search can there);
                // this is a shell-specific list of index *names* dropped by ignore_unavailable, which has
                // no real-OpenSearch equivalent at all. A caller who asked to ignore what is missing still
                // has to be able to tell which of their indices this answer does not cover -- otherwise the
                // flag turns a visible absence into an invisible one, which is worse than the refusal it
                // replaced.
                builder.startArray("skipped_indices");
                for (String name : skipped) {
                    builder.value(name);
                }
                builder.endArray();
            }
            builder.startObject("hits");
            builder.startObject("total");
            builder.field("value", outcome.total());
            // Always "eq": trackTotalHits(true) is forced on every search this handler runs (see the
            // request-parsing side), so "value" is never a lower bound and "relation" never needs to say
            // otherwise -- unlike real OpenSearch, which reports "gte" once a search stops counting past a
            // configured ceiling. Nothing here has that ceiling yet, so "eq" is the honest constant.
            builder.field("relation", "eq");
            builder.endObject();
            float maxScore = Float.NaN;
            for (SearchHit hit : outcome.hits()) {
                if (Float.isNaN(hit.getScore()) == false && (Float.isNaN(maxScore) || hit.getScore() > maxScore)) {
                    maxScore = hit.getScore();
                }
            }
            if (Float.isNaN(maxScore) == false) {
                builder.field("max_score", maxScore);
            }
            builder.startArray("hits");
            for (SearchHit hit : outcome.hits()) {
                builder.startObject();
                // The hit's own index, not the request's: a search over several indices returns hits from
                // several, and telling a caller they all came from the first one named would be a lie that
                // reads like a formatting detail.
                builder.field("_index", hit.getIndex() == null ? String.join(",", indices.keySet()) : hit.getIndex());
                builder.field("_id", hit.getId());
                if (Float.isNaN(hit.getScore()) == false) {
                    builder.field("_score", hit.getScore());
                }
                if (hit.getFields().isEmpty() == false) {
                    // script_fields and docvalue_fields. They were computed and then dropped: a caller asking
                    // for a derived value got hits carrying nothing, which looks like the script produced
                    // nothing rather than like the renderer forgot it. Values are always an array, as
                    // OpenSearch returns them, because a field may be multi-valued and a client that
                    // sometimes gets a scalar has to branch on it.
                    builder.startObject("fields");
                    for (var field : hit.getFields().entrySet()) {
                        builder.startArray(field.getKey());
                        for (Object value : field.getValue().getValues()) {
                            builder.value(value);
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                if (hit.getSortValues() != null && hit.getSortValues().length > 0) {
                    // The cursor a caller sends back as search_after. It was already being relied on by
                    // paginating clients, which had to re-derive it from the sorted field values.
                    builder.startArray("sort");
                    for (Object value : hit.getSortValues()) {
                        builder.value(value);
                    }
                    builder.endArray();
                }
                final String hitSource = hit.getSourceAsString();
                if (hitSource != null && hitSource.isEmpty() == false) {
                    // Raw, so a document comes back as an object. It used to be written with
                    // builder.field(String, String), which returned the whole source as one escaped
                    // string that every client had to unescape before it could read a document.
                    builder.rawField(
                        "_source",
                        new java.io.ByteArrayInputStream(hitSource.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                        XContentType.JSON
                    );
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            if (outcome.aggregations() != null) {
                // Rendered by the aggregations themselves, which is the only way the shape matches what a
                // classic node returns: every aggregation type knows its own output, and a hand-written
                // renderer would agree with them until somebody used one it had not met.
                outcome.aggregations().toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            }
            builder.endObject();
        }
    }
}
