/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * Turns a search outcome into the response real OpenSearch would send, for a search or a count. Split out of
 * {@link SearchHandler}, which keeps the public entry points.
 */
final class SearchRendering {

    private SearchRendering() {}

    /**
     * Wall-clock elapsed since a search began, for the {@code took} field real OpenSearch always reports.
     *
     * <p>Measured around the fan-out only -- parsing and validating the request happens before this is
     * called, the same boundary a classic node's own {@code took} is measured from.
     *
     * @param startNanos {@link System#nanoTime()} at the moment the fan-out began
     * @return elapsed milliseconds
     */
    static long tookMillis(long startNanos) {
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
        org.opensearch.serverless.shard.SearchOutcome outcome,
        SearchSourceBuilder source,
        long tookMillis,
        String pitId
    ) {
        final boolean tracked = outcome.tracksTotalHits() == null
            ? source == null
                || source.trackTotalHitsUpTo() == null
                || source.trackTotalHitsUpTo() != org.opensearch.search.internal.SearchContext.TRACK_TOTAL_HITS_DISABLED
            : outcome.tracksTotalHits();
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
            outcome.skipped(),
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
    static void render(
        org.opensearch.rest.RestChannel channel,
        java.util.List<String> skipped,
        org.opensearch.serverless.shard.SearchOutcome outcome,
        SearchSourceBuilder source,
        long tookMillis,
        boolean counting,
        String pitId,
        org.opensearch.core.xcontent.ToXContent.Params params,
        java.util.function.UnaryOperator<org.opensearch.action.search.SearchResponse> postProcess
    ) throws IOException {
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
    static void renderCount(
        org.opensearch.rest.RestChannel channel,
        org.opensearch.action.search.SearchResponse response,
        SearchSourceBuilder source,
        org.opensearch.core.xcontent.ToXContent.Params params
    ) throws IOException {
        if (response.getHits().getTotalHits() == null) {
            throw SearchGate.unsupportedFilterRewrite("the total hits were removed from a count response");
        }
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
