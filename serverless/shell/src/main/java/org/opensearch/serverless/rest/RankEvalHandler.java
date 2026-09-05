/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.rankeval.EvalQueryQuality;
import org.opensearch.index.rankeval.EvaluationMetric;
import org.opensearch.index.rankeval.RankEvalResponse;
import org.opensearch.index.rankeval.RankEvalSpec;
import org.opensearch.index.rankeval.RatedRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.Script;
import org.opensearch.script.TemplateScript;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code /{index}/_rank_eval}: ranking evaluation, with the rank-eval module's own metrics.
 *
 * <p>The refusal this replaces said the arithmetic belonged in the harness doing the evaluating. It does;
 * the module already is that harness, and what it needed from a node was a way to run each rated
 * request's search. Every search runs through the same fan-out a {@code _search} does, and the loop that
 * turns hits and ratings into a score is the module's transport action's, mirrored line for line.
 */
public final class RankEvalHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public RankEvalHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_rank_eval_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/{index}/_rank_eval"), new Route(RestRequest.Method.POST, "/{index}/_rank_eval"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        // Hints; indices here resolve by one rule, and there is one search type.
        request.param("ignore_unavailable");
        request.param("allow_no_indices");
        request.param("expand_wildcards");
        request.param("search_type");
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final RankEvalSpec spec;
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(
                    serving.rankEvalXContentRegistry(),
                    LoggingDeprecationHandler.INSTANCE,
                    request.requiredContent().streamInput()
                )
        ) {
            spec = RankEvalSpec.parse(parser);
        } catch (Exception e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_request", "could not parse the evaluation: " + e.getMessage())
            );
        }
        if (spec.getRatedRequests().size() > MultiSearchHandler.MAX_SEARCHES) {
            // Each rated request is a full search; the same bound a batch has.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "bad_request",
                    "an evaluation may carry at most " + MultiSearchHandler.MAX_SEARCHES + " requests"
                )
            );
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                // Resolved here rather than on the HTTP thread: it reads registers.
                final SearchHandler.Resolution resolved = SearchHandler.resolveIndices(metadata, serving, index, false);
                if (resolved.refused()) {
                    channel.sendResponse(IndexAdminHandler.error(channel, resolved.status(), resolved.type(), resolved.reason()));
                    return;
                }
                evaluate(channel, serving, metadata, resolved.indices(), spec, request);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a rank-eval failure", nested);
                }
            }
        });
    }

    private void evaluate(
        org.opensearch.rest.RestChannel channel,
        ServerlessNode serving,
        MetadataPlane metadata,
        Map<String, org.opensearch.serverless.cluster.IndexDescriptor> indices,
        RankEvalSpec spec,
        ToXContent.Params params
    ) throws IOException {
        final EvaluationMetric metric = spec.getMetric();
        final Map<String, Exception> errors = new LinkedHashMap<>();
        final Map<String, TemplateScript.Factory> templates = new HashMap<>();
        for (Map.Entry<String, Script> entry : spec.getTemplates().entrySet()) {
            templates.put(entry.getKey(), serving.scriptService().compile(entry.getValue(), TemplateScript.CONTEXT));
        }
        final Map<String, EvalQueryQuality> details = new LinkedHashMap<>();
        for (RatedRequest rated : spec.getRatedRequests()) {
            SearchSourceBuilder evaluation = rated.getEvaluationRequest();
            try {
                if (evaluation == null) {
                    final TemplateScript.Factory template = templates.get(rated.getTemplateId());
                    if (template == null) {
                        throw new IllegalArgumentException("no template with id [" + rated.getTemplateId() + "]");
                    }
                    final String resolved = template.newInstance(rated.getParams()).execute();
                    try (
                        XContentParser sub = XContentType.JSON.xContent()
                            .createParser(serving.searchXContentRegistry(), LoggingDeprecationHandler.INSTANCE, resolved)
                    ) {
                        evaluation = SearchSourceBuilder.fromXContent(sub, false);
                    }
                }
                // The module's own rule: a rated request is a query, not an aggregation, a suggestion or a
                // profile, since none of those produce the ranked hits a metric scores.
                if (evaluation.suggest() != null) {
                    throw new IllegalArgumentException("Query in rated requests should not contain a suggest section.");
                }
                if (evaluation.aggregations() != null) {
                    throw new IllegalArgumentException("Query in rated requests should not contain aggregations.");
                }
                if (evaluation.highlighter() != null) {
                    throw new IllegalArgumentException("Query in rated requests should not contain a highlighter section.");
                }
                if (evaluation.explain() != null && evaluation.explain()) {
                    throw new IllegalArgumentException("Query in rated requests should not use explain.");
                }
                if (evaluation.profile()) {
                    throw new IllegalArgumentException("Query in rated requests should not use profile.");
                }
                if (metric.forcedSearchSize().isPresent()) {
                    evaluation.size(metric.forcedSearchSize().getAsInt());
                }
                final List<String> summaryFields = rated.getSummaryFields();
                if (summaryFields.isEmpty()) {
                    evaluation.fetchSource(false);
                } else {
                    evaluation.fetchSource(summaryFields.toArray(new String[0]), new String[0]);
                }
                SearchHandler.applyDefaults(evaluation, false);
                final String unsupported = SearchHandler.whatCannotBeMerged(evaluation);
                if (unsupported != null) {
                    throw new IllegalArgumentException(unsupported);
                }
                final SearchSourceBuilder ready = evaluation;
                final var outcome = SearchHandler.gated(
                    serving,
                    indices.keySet(),
                    ready,
                    admitted -> SearchFanout.run(serving, metadata, indices, admitted)
                );
                final SearchHit[] hits = outcome.hits().toArray(new SearchHit[0]);
                details.put(rated.getId(), metric.evaluate(rated.getId(), hits, rated.getRatedDocs()));
            } catch (Exception e) {
                // This request's problem, not the evaluation's: reported under its id, as the module does.
                errors.put(rated.getId(), e);
            }
        }
        final RankEvalResponse response = new RankEvalResponse(metric.combine(details.values()), details, errors);
        try (XContentBuilder builder = channel.newBuilder()) {
            response.toXContent(builder, params);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
