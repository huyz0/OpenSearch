/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesAction;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryAction;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.Names;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.metadata.RepositoryInUseException;
import org.opensearch.serverless.shell.ActionGate;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        final String repo = request.param("repo");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        // There is no cluster manager to time out against. Verification checks this deployment's own store.
        request.param("master_timeout");
        request.param("cluster_manager_timeout");
        request.param("timeout");
        final boolean verify = request.paramAsBoolean("verify", true);
        if (request.path().endsWith("/_verify")) {
            return channel -> dispatch(
                channel,
                () -> respond(
                    channel,
                    gated(VerifyRepositoryAction.NAME, new VerifyRepositoryRequest(repo), VerifyRepositoryResponse.class, admitted -> {
                        final String selected = ((VerifyRepositoryRequest) admitted).name();
                        Names.validateId(selected, "repository");
                        metadata.describeRepository(selected).orElseThrow(() -> new RepositoryMissingException(selected));
                        return new VerifyRepositoryResponse(new org.opensearch.cluster.node.DiscoveryNode[] { node.get().localNode() });
                    })
                )
            );
        }
        if (repo != null && repo.startsWith("_") && "_all".equals(repo) == false) {
            return channel -> channel.sendResponse(
                error(
                    channel,
                    RestStatus.NOT_IMPLEMENTED,
                    "not_implemented",
                    "repository names beginning with an underscore are reserved for APIs"
                )
            );
        }
        if (request.method() == RestRequest.Method.GET) {
            final String[] names = repo == null ? new String[0] : repo.split(",");
            return channel -> dispatch(
                channel,
                () -> respond(
                    channel,
                    gated(
                        GetRepositoriesAction.NAME,
                        new GetRepositoriesRequest(names),
                        GetRepositoriesResponse.class,
                        admitted -> describe(metadata, ((GetRepositoriesRequest) admitted).repositories())
                    )
                )
            );
        }
        switch (request.method()) {
            case PUT:
            case POST: {
                // Decode on the worker, where the configuration reads and writes run as well.
                final BytesReference content = request.hasContent() ? request.content() : null;
                final var mediaType = request.getMediaType();
                return channel -> dispatch(channel, () -> {
                    final var put = new PutRepositoryRequest(repo).type(RepositoryDescriptor.TYPE_NATIVE).verify(verify);
                    if (content != null) {
                        try (
                            var parser = mediaType.xContent()
                                .createParser(
                                    NamedXContentRegistry.EMPTY,
                                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                    content.streamInput()
                                )
                        ) {
                            put.source(parser.map());
                        }
                    }
                    respond(channel, gated(PutRepositoryAction.NAME, put, AcknowledgedResponse.class, admitted -> {
                        final var selected = (PutRepositoryRequest) admitted;
                        Names.validateId(selected.name(), "repository");
                        final var invalid = selected.validate();
                        if (invalid != null) {
                            throw invalid;
                        }
                        if (RepositoryDescriptor.TYPE_NATIVE.equals(selected.type()) == false) {
                            throw new IndexAdminHandler.RefusedException(
                                RestStatus.NOT_IMPLEMENTED,
                                "unsupported_repository_type",
                                "type '"
                                    + selected.type()
                                    + "' is not supported; this deployment has exactly one object store, reached with type '"
                                    + RepositoryDescriptor.TYPE_NATIVE
                                    + "' (D3)"
                            );
                        }
                        if (selected.cryptoSettings() != null) {
                            throw new IndexAdminHandler.RefusedException(
                                RestStatus.NOT_IMPLEMENTED,
                                "unsupported_repository_setting",
                                "repository crypto_settings are not supported"
                            );
                        }
                        final Map<String, Object> settings;
                        try (var builder = XContentFactory.jsonBuilder()) {
                            builder.startObject();
                            selected.settings()
                                .toXContent(
                                    builder,
                                    new org.opensearch.core.xcontent.ToXContent.MapParams(Map.of("flat_settings", "true"))
                                );
                            builder.endObject();
                            settings = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                                BytesReference.bytes(builder),
                                true,
                                XContentType.JSON
                            ).v2();
                        }
                        metadata.putRepository(
                            new RepositoryDescriptor(selected.name(), selected.type(), settings, metadata.clock().getAsLong())
                        );
                        return new AcknowledgedResponse(true);
                    }));
                });
            }
            case DELETE: {
                final String[] names = repo == null ? new String[0] : repo.split(",");
                return channel -> dispatch(channel, () -> {
                    for (String name : names) {
                        // Validate before the gate too: wildcard mutations must never acknowledge a listing.
                        Names.validateId(name, "repository");
                        if ("_all".equals(name) || Regex.isSimpleMatchPattern(name)) {
                            throw new IllegalArgumentException("repository mutations require concrete repository names");
                        }
                    }
                    boolean acknowledged = true;
                    for (String name : names) {
                        final var result = gated(
                            DeleteRepositoryAction.NAME,
                            new DeleteRepositoryRequest(name),
                            AcknowledgedResponse.class,
                            admitted -> {
                                final String selected = ((DeleteRepositoryRequest) admitted).name();
                                Names.validateId(selected, "repository");
                                if ("_all".equals(selected) || Regex.isSimpleMatchPattern(selected)) {
                                    throw new IllegalArgumentException("repository mutations require concrete repository names");
                                }
                                if (metadata.deleteRepository(selected) == false) {
                                    throw new RepositoryMissingException(selected);
                                }
                                return new AcknowledgedResponse(true);
                            }
                        );
                        acknowledged &= result.isAcknowledged();
                    }
                    respond(channel, new AcknowledgedResponse(acknowledged));
                });
            }
            default:
                return channel -> channel.sendResponse(
                    error(channel, RestStatus.METHOD_NOT_ALLOWED, "method_not_allowed", request.method() + " is not supported here")
                );
        }
    }

    /** Resolves the selection admitted by the filters, and builds core's native response. */
    private GetRepositoriesResponse describe(MetadataPlane metadata, String[] names) throws IOException {
        final Map<String, RepositoryDescriptor> selected = new LinkedHashMap<>();
        for (String name : names) {
            if ("_all".equals(name) == false && Regex.isSimpleMatchPattern(name) == false) {
                selected.put(name, metadata.describeRepository(name).orElseThrow(() -> new RepositoryMissingException(name)));
            }
        }
        if (names.length == 0 || java.util.Arrays.stream(names).anyMatch(n -> "_all".equals(n) || Regex.isSimpleMatchPattern(n))) {
            for (var descriptor : metadata.listRepositories()) {
                if (names.length == 0
                    || java.util.Arrays.stream(names).anyMatch(n -> "_all".equals(n) || Regex.simpleMatch(n, descriptor.name()))) {
                    selected.put(descriptor.name(), descriptor);
                }
            }
        }
        // Core's constructor is package private; its public parser preserves the native plugin contract.
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            for (var descriptor : selected.values()) {
                builder.startObject(descriptor.name());
                builder.field("type", descriptor.type());
                builder.startObject("settings");
                Settings.builder()
                    .loadFromMap(descriptor.settings())
                    .build()
                    .toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
            try (
                var parser = XContentType.JSON.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        BytesReference.bytes(builder).streamInput()
                    )
            ) {
                return GetRepositoriesResponse.fromXContent(parser);
            }
        }
    }

    private void respond(org.opensearch.rest.RestChannel channel, ToXContentObject response) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            response.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /** Runs object-store work and decoding off the HTTP transport thread. */
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
                if (e instanceof IndexAdminHandler.RefusedException refused) {
                    channel.sendResponse(error(channel, refused.status, refused.type, refused.getMessage()));
                } else if (e instanceof RepositoryInUseException) {
                    channel.sendResponse(error(channel, RestStatus.CONFLICT, "repository_in_use", e.getMessage()));
                } else {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                }
            } catch (IOException nested) {
                logger.error("failed to report a repository administration failure", nested);
            }
        }
    }

    private <T extends ActionResponse> T gated(
        String action,
        ActionRequest request,
        Class<T> responseType,
        CheckedFunction<ActionRequest, T, Exception> work
    ) throws Exception {
        final var serving = node.get();
        return serving == null ? work.apply(request) : serving.actionGate().run(action, request, work, ActionGate.identity(responseType));
    }

    private static BytesRestResponse error(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason)
        throws IOException {
        return IndexAdminHandler.error(channel, status, type, reason);
    }
}
