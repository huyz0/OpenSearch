/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.apache.lucene.search.Explanation;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * {@code _explain} — why one document does or does not match one query.
 *
 * <p><b>This is the first classic scoring endpoint served here, and the reason it can be is that it is not a
 * fan-out.</b> Every refusal on this surface has come down to one of two things: needing cluster state, or
 * needing to enumerate. An explain needs neither. It names a document, a named document lives on one shard,
 * and that shard is found from the shard-head the same way a get finds it. The work left over is one term
 * lookup and Lucene's own {@code explain} against a searcher already open — cheaper than the search whose
 * score it accounts for, which has to ask every shard.
 *
 * <p><b>It says which copy answered, which the classic endpoint has no reason to.</b> A score is not a
 * property of a document; it is a function of the whole shard's term and document frequencies. Explaining
 * against a published commit while a writer holds newer segments gives a real explanation of a real score
 * that will not be the score a search returns. In a cluster every copy of a shard holds the same segments so
 * the question never arises. Here a shard may have no owner and be read from its last published commit, so
 * the answer carries {@code realtime} for the same reason a get does, and means the same thing.
 */
public final class ExplainHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public ExplainHandler(Supplier<MetadataPlane> plane, Supplier<ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_explain_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/{index}/_explain/{id}"),
            new Route(RestRequest.Method.POST, "/{index}/_explain/{id}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final String id = request.param("id");
        final boolean hasBody = request.hasContent();
        // q= with its companions, through core's own query-string parser -- df, analyzer, default_operator,
        // lenient and analyze_wildcard are consumed by it and mean what they mean on _search.
        final QueryBuilder fromUrl = org.opensearch.rest.action.RestActions.urlParamsToQueryBuilder(request);
        final String routing = request.param("routing");
        final String storedFields = request.param("stored_fields");
        final org.opensearch.search.fetch.subphase.FetchSourceContext wantSource = org.opensearch.search.fetch.subphase.FetchSourceContext
            .parseFromRestRequest(request);
        // A hint: there is one copy of each shard for preference to choose between.
        request.param("preference");
        if (routing != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_explain",
                    "routing is not supported: a document is placed by its id alone"
                )
            );
        }
        if (storedFields != null || (wantSource != null && wantSource.fetchSource())) {
            // Refused rather than silently omitted: core answers these with a "get" block carrying the
            // document, and this explains a score without fetching the document back.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_explain",
                    "_source and stored_fields on an explain are not supported: the document is not returned with its "
                        + "explanation. GET /{index}/_doc/{id} reads it"
                )
            );
        }

        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        final QueryBuilder query;
        if (hasBody) {
            final SearchSourceBuilder source;
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
                        "could not parse the explain body: " + e.getMessage()
                    )
                );
            }
            if (source.query() == null) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.BAD_REQUEST,
                        "bad_query",
                        "the body must contain a query to explain against"
                    )
                );
            }
            query = source.query();
        } else if (fromUrl != null) {
            // The same parser search's q= uses, so the two shorthands cannot mean different things -- which
            // matters more here than anywhere: explaining a query the caller did not write is worse than
            // refusing, because the explanation would be correct about the wrong query.
            query = fromUrl;
        } else {
            // No match-all default, unlike _count. "How many documents are there" is a question; "explain
            // this document against nothing in particular" is not one, and answering it with match_all
            // would hand back a constant score that looks like a real result.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_query", "send a query body, or a q= query string")
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final ShardOperations operations = new ShardOperations(serving, metadata, true);
                respond(channel, index, id, operations.explain(index, id, query));
            } catch (ShardOperations.NoSuchIndexException e) {
                sendQuietly(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index);
            } catch (ShardOperations.SystemIndexException e) {
                sendQuietly(channel, RestStatus.FORBIDDEN, "system_index", e.getMessage());
            } catch (ShardOperations.NotHereException e) {
                // The same status and type the get and update endpoints give the same state.
                sendQuietly(channel, e.restStatus(), e.restType(serving.localNode().getId()), e.getMessage());
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report an explain failure", nested);
                }
            }
        });
    }

    private void respond(RestChannel channel, String index, String id, ShardOperations.Explained explained) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("_index", index);
            builder.field("_id", id);
            builder.field("matched", explained.matched());
            if (explained.explanation() != null) {
                builder.startObject("explanation");
                render(builder, explained.explanation());
                builder.endObject();
            }
            // Which copy scored it, said plainly. A non-realtime explanation was computed over a published
            // commit's segment statistics; it is a true account of that commit and may differ from what a
            // search returns while a writer holds newer segments.
            builder.field("realtime", explained.realtime());
            builder.field("_node", explained.servedBy());
            builder.endObject();
            // A document that is not there is a 404, as it is in OpenSearch -- and distinct from a document
            // that is there and did not match, which is a 200 with matched:false and the explanation saying
            // which clause failed. Collapsing the two would answer the easy question and drop the hard one.
            channel.sendResponse(new BytesRestResponse(explained.exists() ? RestStatus.OK : RestStatus.NOT_FOUND, builder));
        }
    }

    private void render(XContentBuilder builder, Explanation explanation) throws IOException {
        builder.field("value", explanation.getValue());
        builder.field("description", explanation.getDescription());
        final Explanation[] details = explanation.getDetails();
        if (details.length > 0) {
            builder.startArray("details");
            for (Explanation detail : details) {
                builder.startObject();
                render(builder, detail);
                builder.endObject();
            }
            builder.endArray();
        }
    }

    private void sendQuietly(RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report an explain refusal", e);
        }
    }
}
