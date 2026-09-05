/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.script.TemplateScript;
import org.opensearch.script.mustache.MultiSearchTemplateRequest;
import org.opensearch.script.mustache.RestMultiSearchTemplateAction;
import org.opensearch.script.mustache.SearchTemplateRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Search templates: {@code _search/template}, {@code _render/template} and {@code _msearch/template}.
 *
 * <p>The mustache engine is core's, chosen by name the way painless was; the request and response shapes
 * are the module's own {@code SearchTemplateRequest} and rendering. What the module's transport action does
 * with a rendered template -- parse it into a source and search -- is done here by handing the source to
 * the same {@link SearchHandler#plan} a plain body reaches, so a templated search is the search its
 * rendering would have been and not a second implementation of one. A stored template resolves through
 * this deployment's own stored scripts.
 */
public final class SearchTemplateHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;
    private final SearchHandler search;
    private final MultiSearchHandler multiSearch;

    /**
     * Creates the handler.
     *
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public SearchTemplateHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
        this.search = new SearchHandler(node, plane);
        this.multiSearch = new MultiSearchHandler(plane, node);
    }

    @Override
    public String getName() {
        return "serverless_search_template_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_search/template"),
            new Route(RestRequest.Method.POST, "/_search/template"),
            new Route(RestRequest.Method.GET, "/{index}/_search/template"),
            new Route(RestRequest.Method.POST, "/{index}/_search/template"),
            new Route(RestRequest.Method.GET, "/_render/template"),
            new Route(RestRequest.Method.POST, "/_render/template"),
            new Route(RestRequest.Method.GET, "/_render/template/{id}"),
            new Route(RestRequest.Method.POST, "/_render/template/{id}"),
            new Route(RestRequest.Method.GET, "/_msearch/template"),
            new Route(RestRequest.Method.POST, "/_msearch/template"),
            new Route(RestRequest.Method.GET, "/{index}/_msearch/template"),
            new Route(RestRequest.Method.POST, "/{index}/_msearch/template")
        );
    }

    private static final java.util.Set<String> RESPONSE_PARAMS = java.util.Set.of(
        RestSearchAction.TYPED_KEYS_PARAM,
        RestSearchAction.TOTAL_HITS_AS_INT_PARAM
    );

    @Override
    protected java.util.Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    public boolean supportsContentStream() {
        return true;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            IndexAdminHandler.consumeAllParams(request);
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final String path = request.path();
        if (path.contains("/_render/template")) {
            return render(request, serving);
        }
        if (path.endsWith("/_msearch/template")) {
            return multi(request, serving, metadata);
        }
        return single(request, serving);
    }

    private RestChannelConsumer render(RestRequest request, ServerlessNode serving) throws IOException {
        final String id = request.param("id");
        final SearchTemplateRequest template;
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            template = SearchTemplateRequest.fromXContent(parser);
        }
        if (id != null) {
            template.setScriptType(ScriptType.STORED);
            template.setScript(id);
        }
        // Off the HTTP thread before rendering: a stored template is a script-register read on a cache
        // miss, which is object-store IO on the thread that should be reading the next request.
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final String rendered = render(serving, template);
                // The module's own render shape: the rendered template, raw.
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.rawField("template_output", new BytesArray(rendered).streamInput(), XContentType.JSON);
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            } catch (Exception e) {
                report(channel, e, "render");
            }
        });
    }

    private void report(org.opensearch.rest.RestChannel channel, Exception e, String what) {
        try {
            channel.sendResponse(IndexAdminHandler.failure(channel, e));
        } catch (IOException nested) {
            logger.error("failed to report a search template " + what + " failure", nested);
        }
    }

    private RestChannelConsumer single(RestRequest request, ServerlessNode serving) throws IOException {
        // The same URL parameters a plain search takes, parsed by core into the same request.
        final SearchRequest searchRequest = new SearchRequest();
        final SearchSourceBuilder source = new SearchSourceBuilder();
        searchRequest.source(source);
        final SearchTemplateRequest template;
        try {
            RestSearchAction.parseSearchRequest(searchRequest, request, null, null, size -> source.size(size));
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                template = SearchTemplateRequest.fromXContent(parser);
            }
        } catch (Exception e) {
            IndexAdminHandler.consumeAllParams(request);
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "bad_query",
                    "could not parse the search template: " + e.getMessage()
                )
            );
        }
        // Everything after parsing runs off the HTTP thread, as SearchHandler.prepareRequest and multi()
        // in this file already do: rendering a stored template reads the script register on a miss, and
        // plan() reads pipelines, points in time and index descriptors before it returns its consumer.
        // All of that used to run on the Netty worker that should have been reading the next request.
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final SearchSourceBuilder rendered = parse(serving, render(serving, template), template);
                // The rendered source takes the body's place; what the URL said about size and from stays
                // as the request's own, since the URL wins over the body on a plain search too.
                if (source.size() >= 0) {
                    rendered.size(source.size());
                }
                if (source.from() >= 0) {
                    rendered.from(source.from());
                }
                if (template.getSearchPipeline() != null) {
                    searchRequest.pipeline(template.getSearchPipeline());
                }
                searchRequest.source(rendered);
                final org.opensearch.search.builder.PointInTimeBuilder pit = rendered.pointInTimeBuilder();
                rendered.pointInTimeBuilder(null);
                search.plan(request, searchRequest, rendered, pit, false).accept(channel);
            } catch (Exception e) {
                report(channel, e, "search");
            }
        });
    }

    private RestChannelConsumer multi(RestRequest request, ServerlessNode serving, MetadataPlane metadata) throws IOException {
        request.param("max_concurrent_searches");
        final MultiSearchTemplateRequest batch;
        try {
            batch = RestMultiSearchTemplateAction.parseRequest(request, true);
        } catch (Exception e) {
            IndexAdminHandler.consumeAllParams(request);
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_request", e.getMessage())
            );
        }
        if (batch.requests().size() > MultiSearchHandler.MAX_SEARCHES) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "malformed_request",
                    "a batch may carry at most " + MultiSearchHandler.MAX_SEARCHES + " searches"
                )
            );
        }
        final List<MultiSearchHandler.Sub> subs = new ArrayList<>();
        for (SearchTemplateRequest template : batch.requests()) {
            final SearchRequest line = template.getRequest();
            final String index = line == null || line.indices().length == 0 ? request.param("index") : String.join(",", line.indices());
            if (index == null) {
                subs.add(
                    MultiSearchHandler.Sub.refuse(
                        null,
                        RestStatus.BAD_REQUEST,
                        "malformed_request",
                        "no index given for this search and none in the path"
                    )
                );
                continue;
            }
            try {
                subs.add(
                    MultiSearchHandler.Sub.of(
                        index,
                        parse(serving, render(serving, template), template),
                        line != null && line.indicesOptions().ignoreUnavailable()
                    )
                );
            } catch (Exception e) {
                subs.add(
                    MultiSearchHandler.Sub.refuse(
                        index,
                        RestStatus.BAD_REQUEST,
                        "malformed_request",
                        e.getMessage() == null ? e.toString() : e.getMessage()
                    )
                );
            }
        }
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                multiSearch.answer(channel, metadata, serving, subs, request);
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a multi-search-template failure", nested);
                }
            }
        });
    }

    /** Renders the template through the script service, as the module's transport action does. */
    private static String render(ServerlessNode serving, SearchTemplateRequest template) {
        final Map<String, Object> params = template.getScriptParams() == null ? Map.of() : template.getScriptParams();
        final Script script = new Script(
            template.getScriptType(),
            template.getScriptType() == ScriptType.STORED ? null : "mustache",
            template.getScript(),
            params
        );
        return serving.scriptService().compile(script, TemplateScript.CONTEXT).newInstance(params).execute();
    }

    private static SearchSourceBuilder parse(ServerlessNode serving, String rendered, SearchTemplateRequest template) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(serving.searchXContentRegistry(), LoggingDeprecationHandler.INSTANCE, rendered)
        ) {
            final SearchSourceBuilder source = SearchSourceBuilder.searchSource();
            source.parseXContent(parser, false);
            if (template.isExplain()) {
                source.explain(true);
            }
            if (template.isProfile()) {
                source.profile(true);
            }
            return source;
        }
    }
}
