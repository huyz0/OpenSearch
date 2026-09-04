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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.metadata.RepositoryInUseException;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code PUT}, {@code GET} and {@code DELETE} on {@code /_snapshot[/{repo}]} — a repository, registered
 * rather than configured, with the same request and response shapes real OpenSearch's own repository
 * endpoints use.
 *
 * <p><b>A repository here is a namespace within this deployment's own object store, not a distinct
 * storage backend.</b> Classic OpenSearch's repository abstraction exists to let a snapshot land on
 * storage distinct from the cluster's own data; this shell has exactly one configured object store (D3),
 * and a snapshot's data already lives durably in it. {@code type} and {@code settings} are accepted and
 * stored the way real OpenSearch's request body shape has them — see {@link RepositoryDescriptor} — but
 * only {@link RepositoryDescriptor#TYPE_NATIVE} is functional; any other {@code type} is refused rather
 * than silently treated as this deployment's own store, because a caller naming a specific backend is
 * asking for something this does not do, not asking for a synonym of what it already does.
 *
 * <p><b>{@code PUT} is an upsert, matching real OpenSearch.</b> Registering the same name twice updates
 * its settings rather than refusing the second call — see {@link MetadataPlane#putRepository}.
 */
public final class RepositoryHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, whose action gate the plugins' filters live behind
     */
    public RepositoryHandler(Supplier<MetadataPlane> plane, Supplier<ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_repository_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_snapshot"),
            new Route(RestRequest.Method.PUT, "/_snapshot/{repo}"),
            new Route(RestRequest.Method.POST, "/_snapshot/{repo}"),
            new Route(RestRequest.Method.GET, "/_snapshot/{repo}"),
            new Route(RestRequest.Method.DELETE, "/_snapshot/{repo}"),
            // Verification, which used to fall through to the snapshot handler as a snapshot named
            // "_verify" and fail on a missing indices list.
            new Route(RestRequest.Method.POST, "/_snapshot/{repo}/_verify")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final String repoParam = request.param("repo");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        // Hints: there is no cluster manager to time out against, and a repository here is a namespace
        // in this deployment's own object store, which is verified by the deployment running at all.
        request.param("master_timeout");
        request.param("cluster_manager_timeout");
        request.param("timeout");
        request.param("verify");
        if (request.path().endsWith("/_verify")) {
            return channel -> dispatch(channel, () -> {
                if (metadata.describeRepository(repoParam).isEmpty()) {
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_FOUND,
                            "repository_missing_exception",
                            "[" + repoParam + "] missing"
                        )
                    );
                    return;
                }
                // Core's VerifyRepositoryResponse: the nodes that could reach the repository. Every node
                // here reaches the one object store, and this one is answering.
                final var serving = node.get();
                try (XContentBuilder builder = channel.newBuilder()) {
                    builder.startObject();
                    builder.startObject("nodes");
                    builder.startObject(serving.localNode().getId());
                    builder.field("name", serving.localNode().getName());
                    builder.endObject();
                    builder.endObject();
                    builder.endObject();
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            });
        }
        if (repoParam == null || "_all".equals(repoParam) || "*".equals(repoParam)) {
            // GET /_snapshot, GET /_snapshot/_all and GET /_snapshot/* are the same request on real
            // OpenSearch: every repository, unfiltered. request.method() is always GET here -- no route
            // above registers PUT/POST/DELETE without {repo}.
            return channel -> dispatch(channel, () -> handleList(channel, metadata, null));
        }
        if (repoParam.startsWith("_")) {
            // Not a repository name: an underscore-prefixed segment here is an API this shell does not
            // implement, and answering "no such repository: _status" would send a caller looking for a
            // repository that was never a name. The same class as an index API read as an index name.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "'"
                        + repoParam
                        + "' is not a repository: names beginning with an underscore are reserved for "
                        + "APIs, and this shell does not implement this one"
                )
            );
        }
        final List<String> names = List.of(repoParam.split(","));

        switch (request.method()) {
            case PUT:
            case POST: {
                final Map<String, Object> body = request.hasContent() ? parseBody(request) : Map.of();
                if (names.size() != 1) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_request", "one repository per PUT")
                    );
                }
                final String repo = names.get(0);
                final Object typeField = body.get("type");
                final String type = typeField == null ? RepositoryDescriptor.TYPE_NATIVE : String.valueOf(typeField);
                if (type.equals(RepositoryDescriptor.TYPE_NATIVE) == false) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_repository_type",
                            "type '"
                                + type
                                + "' is not supported; this deployment has exactly one object store, reached with type '"
                                + RepositoryDescriptor.TYPE_NATIVE
                                + "' (D3) -- a genuinely distinct backend is not offered here"
                        )
                    );
                }
                @SuppressWarnings("unchecked")
                final Map<String, Object> settings = body.get("settings") instanceof Map
                    ? new LinkedHashMap<>((Map<String, Object>) body.get("settings"))
                    : Map.of();
                return channel -> dispatch(channel, () -> {
                    gated(
                        org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction.NAME,
                        new org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest(repo),
                        () -> {
                            metadata.putRepository(new RepositoryDescriptor(repo, type, settings, metadata.clock().getAsLong()));
                            return null;
                        }
                    );
                    respondAcknowledged(channel);
                });
            }
            case GET:
                if (names.size() == 1) {
                    return channel -> dispatch(channel, () -> handleDescribe(channel, metadata, names.get(0)));
                }
                return channel -> dispatch(channel, () -> handleList(channel, metadata, names));
            case DELETE:
                return channel -> dispatch(channel, () -> handleDelete(channel, metadata, names));
            default:
                return channel -> channel.sendResponse(
                    error(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", request.method() + " is not supported here")
                );
        }
    }

    private void handleDescribe(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo) throws Exception {
        final Optional<RepositoryDescriptor> descriptor = gated(
            org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction.NAME,
            new org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest(new String[] { repo }),
            () -> metadata.describeRepository(repo)
        );
        if (descriptor.isEmpty()) {
            channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "repository_missing", "no such repository: " + repo));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            writeRepository(builder, descriptor.get());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * @param only the repositories to include, or null for every registered one
     */
    private void handleList(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, List<String> only) throws Exception {
        final List<RepositoryDescriptor> all = gated(
            org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction.NAME,
            new org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest(
                only == null ? new String[0] : only.toArray(new String[0])
            ),
            metadata::listRepositories
        );
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            for (RepositoryDescriptor descriptor : all) {
                if (only != null && only.contains(descriptor.name()) == false) {
                    continue;
                }
                writeRepository(builder, descriptor);
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void handleDelete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, List<String> repos) throws Exception {
        for (String repo : repos) {
            final boolean existed;
            try {
                existed = gated(
                    org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction.NAME,
                    new org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest(repo),
                    () -> metadata.deleteRepository(repo)
                );
            } catch (RepositoryInUseException e) {
                channel.sendResponse(error(channel, RestStatus.CONFLICT, "repository_in_use", e.getMessage()));
                return;
            }
            if (existed == false) {
                channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "repository_missing", "no such repository: " + repo));
                return;
            }
        }
        respondAcknowledged(channel);
    }

    private void writeRepository(XContentBuilder builder, RepositoryDescriptor descriptor) throws IOException {
        builder.startObject(descriptor.name());
        builder.field("type", descriptor.type());
        builder.startObject("settings");
        for (Map.Entry<String, Object> setting : descriptor.settings().entrySet()) {
            builder.field(setting.getKey(), setting.getValue());
        }
        builder.endObject();
        builder.endObject();
    }

    private void respondAcknowledged(org.opensearch.rest.RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private Map<String, Object> parseBody(RestRequest request) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            return parser.map();
        }
    }

    /**
     * Runs work off the transport thread, or inline when there is no node to borrow a pool from.
     *
     * <p>Every operation here touches the object store, and the transport thread is not the thread to
     * wait on remote IO from — the same lesson the cluster-settings write path learned.
     */
    private void dispatch(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        final var serving = node.get();
        if (serving == null) {
            runQuietly(channel, work);
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> runQuietly(channel, work));
    }

    private void runQuietly(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        try {
            work.run();
        } catch (Exception e) {
            try {
                channel.sendResponse(IndexAdminHandler.failure(channel, e));
            } catch (IOException nested) {
                logger.error("failed to report a repository administration failure", nested);
            }
        }
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        final var serving = node.get();
        return serving == null ? work.get() : serving.actionGate().run(action, request, work);
    }

    private static BytesRestResponse error(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason)
        throws IOException {
        return IndexAdminHandler.error(channel, status, type, reason);
    }
}
