/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.serverless.shell.ActionGate;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/** Multi-get with native parsing, admitted request execution and native response filtering. */
public final class MultiGetHandler extends BaseRestHandler {
    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     * @param node the serving node
     * @param plane the metadata plane
     */
    public MultiGetHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_mget_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_mget"),
            new Route(RestRequest.Method.GET, "/_mget"),
            new Route(RestRequest.Method.POST, "/{index}/_mget"),
            new Route(RestRequest.Method.GET, "/{index}/_mget")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String defaultIndex = request.param("index");
        final FetchSourceContext requestSource = FetchSourceContext.parseFromRestRequest(request);
        final String storedFields = request.param("stored_fields");
        final String routing = request.param("routing");
        final String preference = request.param("preference");
        final boolean realtime = request.paramAsBoolean("realtime", true);
        final boolean refresh = request.paramAsBoolean("refresh", false);
        if (storedFields != null || routing != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_read",
                    storedFields != null
                        ? "stored_fields is not supported: a get returns source"
                        : "routing is not supported: documents are placed by id"
                )
            );
        }
        if (request.hasContentOrSourceParam() == false) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a multi-get needs a body naming the documents")
            );
        }
        final MultiGetRequest ask = new MultiGetRequest().preference(preference).realtime(realtime).refresh(refresh);
        try (var parser = request.contentOrSourceParamParser()) {
            ask.add(defaultIndex, null, requestSource, null, parser, true);
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException("trailing content after the multi-get request");
            }
            if (ask.getItems().stream().anyMatch(item -> item.index() == null)) {
                throw new IllegalArgumentException("every document needs an _index or an index in the request path");
            }
            final var validation = ask.validate();
            if (validation != null) {
                throw validation;
            }
        } catch (Exception e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "bad_request", "the body could not be read: " + e.getMessage())
            );
        }
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        return channel -> serving.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final MultiGetResponse response = serving.actionGate().run(MultiGetAction.NAME, ask, admitted -> {
                    if (admitted instanceof MultiGetRequest selected) {
                        return fetch(serving, metadata, selected);
                    }
                    throw new OpenSearchStatusException("multi-get requires a native MultiGetRequest", RestStatus.NOT_IMPLEMENTED);
                }, ActionGate.identity(MultiGetResponse.class));
                try (var builder = channel.newBuilder()) {
                    response.toXContent(builder, request);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a multi-get failure", nested);
                }
            }
        });
    }

    private static MultiGetResponse fetch(ServerlessNode serving, MetadataPlane metadata, MultiGetRequest request) throws Exception {
        final var validation = request.validate();
        if (validation != null) {
            throw validation;
        }
        final ShardOperations operations = new ShardOperations(serving, metadata, true);
        final MultiGetItemResponse[] responses = new MultiGetItemResponse[request.getItems().size()];
        final List<Callable<Boolean>> tasks = new ArrayList<>(responses.length);
        for (int i = 0; i < responses.length; i++) {
            final int position = i;
            final MultiGetRequest.Item item = request.getItems().get(i);
            tasks.add(() -> {
                try {
                    if (serving.isSystemIndex(item.index())) {
                        throw new OpenSearchSecurityException(
                            "system_index: [" + item.index() + "] belongs to a plugin and cannot be read here",
                            RestStatus.FORBIDDEN
                        );
                    }
                    if (item.routing() != null || item.storedFields() != null) {
                        throw new OpenSearchStatusException(
                            "routing and stored_fields are not supported for multi-get items",
                            RestStatus.NOT_IMPLEMENTED
                        );
                    }
                    final GetRequest get = new GetRequest(item.index(), item.id()).routing(item.routing())
                        .storedFields(item.storedFields())
                        .fetchSourceContext(item.fetchSourceContext())
                        .version(item.version())
                        .versionType(item.versionType())
                        .preference(request.preference())
                        .realtime(request.realtime())
                        .refresh(request.refresh());
                    responses[position] = new MultiGetItemResponse(operations.get(get), null);
                } catch (ShardOperations.NoSuchIndexException e) {
                    responses[position] = failure(item, new IndexNotFoundException(item.index()));
                } catch (ShardOperations.SystemIndexException e) {
                    responses[position] = failure(
                        item,
                        new OpenSearchSecurityException("system_index: " + e.getMessage(), RestStatus.FORBIDDEN)
                    );
                } catch (ShardOperations.NotHereException e) {
                    responses[position] = failure(
                        item,
                        new OpenSearchStatusException(e.restType(serving.localNode().getId()) + ": " + e.getMessage(), e.restStatus())
                    );
                } catch (Exception e) {
                    responses[position] = failure(item, e);
                }
                return true;
            });
        }
        try {
            Fanout.run(serving.threadPool().executor(ThreadPool.Names.GET), Fanout.DEFAULT_CONCURRENCY, tasks);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while reading a multi-get", e);
        }
        return new MultiGetResponse(responses);
    }

    private static MultiGetItemResponse failure(MultiGetRequest.Item item, Exception cause) {
        return new MultiGetItemResponse(null, new MultiGetResponse.Failure(item.index(), item.id(), cause));
    }
}
