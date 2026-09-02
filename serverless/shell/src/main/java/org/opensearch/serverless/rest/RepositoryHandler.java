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
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RepositoryAlreadyExistsException;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.metadata.RepositoryInUseException;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code PUT}, {@code GET} and {@code DELETE} on {@code /_snapshot/{repo}} — a repository, registered
 * rather than configured.
 *
 * <p><b>A repository here is a namespace within this deployment's own object store, not a distinct
 * storage backend.</b> Classic OpenSearch's repository abstraction exists to let a snapshot land on
 * storage distinct from the cluster's own data; this shell has exactly one configured object store (D3),
 * and a snapshot's data already lives durably in it. Registering a repository records a name to take
 * snapshots under and nothing to configure — there is no {@code type}, no {@code settings}, no bucket to
 * name, because the object store this deployment already points at is the only one there is.
 *
 * <p><b>Listing every repository is refused, not silently missing.</b> {@code GET /_snapshot} with no name
 * would be an inventory operation over however many repositories exist, the same shape
 * {@code /_serverless/indices} is refused for. Describe one by name.
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
            new Route(RestRequest.Method.PUT, "/_snapshot/{repo}"),
            new Route(RestRequest.Method.GET, "/_snapshot/{repo}"),
            new Route(RestRequest.Method.DELETE, "/_snapshot/{repo}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final String repo = request.param("repo");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        if ("_all".equals(repo)) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "unsupported_read",
                    "listing every repository is an inventory operation, not a serving one; describe one by name"
                )
            );
        }

        switch (request.method()) {
            case PUT:
                return channel -> dispatch(channel, () -> {
                    try {
                        gated(
                            org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction.NAME,
                            new org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest(repo),
                            () -> {
                                metadata.createRepository(new RepositoryDescriptor(repo, metadata.clock().getAsLong()));
                                return null;
                            }
                        );
                        respondAcknowledged(channel, repo);
                    } catch (RepositoryAlreadyExistsException e) {
                        channel.sendResponse(error(channel, RestStatus.BAD_REQUEST, "repository_already_exists", e.getMessage()));
                    } catch (Exception e) {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    }
                });
            case GET:
                return channel -> dispatch(channel, () -> {
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
                        builder.field("repository", descriptor.get().name());
                        builder.field("created_at", descriptor.get().createdAtMillis());
                        builder.endObject();
                        channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                    }
                });
            case DELETE:
                return channel -> dispatch(channel, () -> {
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
                    } catch (Exception e) {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                        return;
                    }
                    if (existed == false) {
                        channel.sendResponse(error(channel, RestStatus.NOT_FOUND, "repository_missing", "no such repository: " + repo));
                        return;
                    }
                    respondAcknowledged(channel, repo);
                });
            default:
                return channel -> channel.sendResponse(
                    error(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", request.method() + " is not supported here")
                );
        }
    }

    private void respondAcknowledged(org.opensearch.rest.RestChannel channel, String repo) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("repository", repo);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
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
                channel.sendResponse(new BytesRestResponse(channel, e));
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
