/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A plugin's own transport action, running on the shell.
 *
 * <p><b>The gap.</b> An action is how a plugin does anything that is not a document operation — the
 * OpenSearch security plugin's configuration-update API is one. The shell's client implemented an
 * allowlist of core's actions and refused everything else by name, so a plugin calling
 * {@code client.execute(itsOwnAction, request)} was told the shell does not implement it. Core builds
 * these with Guice, which R4 says plugins do not get here, so the shell builds them by resolving each
 * constructor against what this node can supply.
 *
 * <p><b>The action does real work through the client</b> rather than returning a constant. An action that
 * could be constructed and could not reach the shell's data plane would be a mechanism that proves
 * nothing.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessPluginActionTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    /** The plugin's own action: count what an index holds, by asking the shell. */
    public static final class CountAction extends ActionType<CountResponse> {
        static final CountAction INSTANCE = new CountAction();
        static final String NAME = "cluster:admin/serverless_testkit/count";

        private CountAction() {
            super(NAME, CountResponse::new);
        }
    }

    /** The request: which index to count. */
    public static final class CountRequest extends ActionRequest {

        private final String index;

        CountRequest(String index) {
            this.index = index;
        }

        CountRequest(StreamInput in) throws IOException {
            super(in);
            this.index = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** The response: how many were found. */
    public static final class CountResponse extends ActionResponse {

        private final long count;

        CountResponse(long count) {
            this.count = count;
        }

        CountResponse(StreamInput in) throws IOException {
            this.count = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(count);
        }
    }

    /**
     * The action itself.
     *
     * <p>Its constructor is the ordinary shape — {@code TransportService}, {@code ActionFilters}, and the
     * client it works through — which is what the shell has to be able to satisfy without Guice.
     */
    public static final class TransportCountAction extends HandledTransportAction<CountRequest, CountResponse> {

        private final org.opensearch.transport.client.Client client;

        public TransportCountAction(
            TransportService transportService,
            ActionFilters actionFilters,
            org.opensearch.transport.client.Client client
        ) {
            super(CountAction.NAME, transportService, actionFilters, CountRequest::new);
            this.client = client;
        }

        @Override
        protected void doExecute(Task task, CountRequest request, ActionListener<CountResponse> listener) {
            try {
                final var response = client.search(
                    new org.opensearch.action.search.SearchRequest(new String[] { request.index }).source(
                        new org.opensearch.search.builder.SearchSourceBuilder().query(
                            org.opensearch.index.query.QueryBuilders.matchAllQuery()
                        )
                    )
                ).actionGet();
                listener.onResponse(new CountResponse(response.getHits().getTotalHits().value()));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    /** A plugin that ships the action and a REST handler that calls it. */
    public static final class CountingPlugin extends Plugin implements ActionPlugin {

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(CountAction.INSTANCE, TransportCountAction.class));
        }

        @Override
        public List<RestHandler> getRestHandlers(
            Settings settings,
            org.opensearch.rest.RestController restController,
            org.opensearch.common.settings.ClusterSettings clusterSettings,
            org.opensearch.common.settings.IndexScopedSettings indexScopedSettings,
            org.opensearch.common.settings.SettingsFilter settingsFilter,
            org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
            java.util.function.Supplier<org.opensearch.cluster.node.DiscoveryNodes> nodesInCluster
        ) {
            return List.of(new BaseRestHandler() {
                @Override
                public String getName() {
                    return "serverless_testkit_count";
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, "/_testkit/count/{index}"));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
                    final String index = request.param("index");
                    return channel -> client.execute(CountAction.INSTANCE, new CountRequest(index), new ActionListener<>() {
                        @Override
                        public void onResponse(CountResponse response) {
                            try (XContentBuilder builder = channel.newBuilder()) {
                                builder.startObject();
                                builder.field("count", response.count);
                                builder.endObject();
                                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                            } catch (IOException e) {
                                onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            try {
                                channel.sendResponse(new BytesRestResponse(channel, e));
                            } catch (IOException io) {
                                throw new AssertionError(io);
                            }
                        }
                    });
                }
            });
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-action")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * A plugin's action is built, registered, and does real work.
     *
     * <p>Reached both ways that matter: through the plugin's own REST handler over HTTP, and through the
     * client directly. The second is the one a plugin's internals use, and it is the one that was refused
     * by name before this existed.
     */
    public void testAPluginsOwnActionRuns() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-action"), List.of(new CountingPlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            assertEquals("the action must be registered", java.util.Set.of(CountAction.NAME), node.pluginActions().names());

            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"one\"}").status());
            assertEquals(201, send(node, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"two\"}").status());

            // Through the plugin's own endpoint, which reaches the action through the NodeClient.
            final var counted = send(node, "GET", "/_testkit/count/alpha", null);
            assertEquals(counted.body(), 200, counted.status());
            assertTrue("the plugin's action must have counted what was written: " + counted.body(), counted.body().contains("\"count\":2"));

            // And directly, which is how a plugin's own code calls it.
            final CountResponse direct = node.client().execute(CountAction.INSTANCE, new CountRequest("alpha")).actionGet();
            assertEquals("the same answer through the client", 2L, direct.count);
        }
    }

    /**
     * A plugin cannot take over one of the shell's own actions by declaring it.
     *
     * <p>The allowlist is consulted first and the plugins' actions after, so a plugin naming
     * {@code indices:data/write/index} gets its action built and never reached. That ordering is the only
     * thing standing between a plugin and every write on the node, so it is asserted rather than assumed.
     */
    public void testAPluginCannotTakeOverACoreAction() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-action-shadow"), List.of(new ShadowingPlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            // Through the client, which is the path the ordering actually governs. The REST write path does
            // not go through the client's dispatch at all, so asserting on an HTTP write proved nothing
            // about this -- the first version of this test did exactly that and could not fail.
            node.client()
                .index(
                    new org.opensearch.action.index.IndexRequest("alpha").id("1")
                        .source("{\"msg\":\"real\"}", org.opensearch.common.xcontent.XContentType.JSON)
                )
                .actionGet();

            final var got = send(node, "GET", "/alpha/_doc/1", null);
            assertTrue("the shell's own write must have happened: " + got.body(), got.body().contains("real"));
            assertFalse("the plugin's action must not have run: " + got.body(), got.body().contains("hijacked"));
        }
    }

    /** A plugin that declares core's index action as its own. */
    public static final class ShadowingPlugin extends Plugin implements ActionPlugin {

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(ShadowAction.INSTANCE, TransportShadowAction.class));
        }
    }

    /** Core's index action name, claimed by a plugin. */
    public static final class ShadowAction extends ActionType<CountResponse> {
        static final ShadowAction INSTANCE = new ShadowAction();

        private ShadowAction() {
            super(org.opensearch.action.index.IndexAction.NAME, CountResponse::new);
        }
    }

    /** Answers everything with a document that says "hijacked", if it is ever reached. */
    public static final class TransportShadowAction extends HandledTransportAction<CountRequest, CountResponse> {

        public TransportShadowAction(TransportService transportService, ActionFilters actionFilters) {
            super(org.opensearch.action.index.IndexAction.NAME + "/shadow", transportService, actionFilters, CountRequest::new);
        }

        @Override
        protected void doExecute(Task task, CountRequest request, ActionListener<CountResponse> listener) {
            listener.onFailure(new IllegalStateException("hijacked"));
        }
    }

    /**
     * An action nothing can build fails the node's start, saying what was missing.
     *
     * <p><b>Refusing is the point.</b> The alternative — skipping an action whose constructor cannot be
     * satisfied — leaves a plugin apparently installed and quietly broken, discovered much later by a
     * caller getting an unexplained error from a code path nobody was looking at. This fails at the moment
     * of the mistake, with the class named.
     */
    public void testAnActionThatCannotBeBuiltFailsTheStart() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-action-unbuildable"), List.of(new UnbuildablePlugin()))) {
            final IllegalStateException thrown = expectThrows(IllegalStateException.class, node::start);
            final String reported = thrown.getMessage() + (thrown.getCause() == null ? "" : " / " + thrown.getCause().getMessage());
            assertTrue("the failure must name the action: " + reported, reported.contains("TransportUnbuildableAction"));
            assertTrue(
                "and must say what this node could have supplied: " + reported,
                reported.contains("no public constructor could be satisfied")
            );
        }
    }

    /** A plugin whose action wants something no node has. */
    public static final class UnbuildablePlugin extends Plugin implements ActionPlugin {

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(UnbuildableAction.INSTANCE, TransportUnbuildableAction.class));
        }
    }

    /** The action type for the unbuildable action. */
    public static final class UnbuildableAction extends ActionType<CountResponse> {
        static final UnbuildableAction INSTANCE = new UnbuildableAction();

        private UnbuildableAction() {
            super("cluster:admin/serverless_testkit/unbuildable", CountResponse::new);
        }
    }

    /** Whatever this is, no node has one. */
    public static final class SomethingNobodyProvides {}

    /** Its only constructor asks for it. */
    public static final class TransportUnbuildableAction extends HandledTransportAction<CountRequest, CountResponse> {

        public TransportUnbuildableAction(TransportService transportService, ActionFilters actionFilters, SomethingNobodyProvides missing) {
            super("cluster:admin/serverless_testkit/unbuildable", transportService, actionFilters, CountRequest::new);
        }

        @Override
        protected void doExecute(Task task, CountRequest request, ActionListener<CountResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("never built"));
        }
    }

    /**
     * Two constructors the shell could choose between is a choice nobody made, so it is refused.
     *
     * <p>Picking one would mean the node's behaviour depended on the order {@code getConstructors} happens
     * to return, which is not specified and can differ between JVMs. A plugin author who wrote two
     * satisfiable constructors of the same shape did not intend either one to win.
     */
    public void testTwoSatisfiableConstructorsAreRefused() throws Exception {
        try (ServerlessNode node = new ServerlessNode(nodeSettings("plugin-action-ambiguous"), List.of(new AmbiguousPlugin()))) {
            final IllegalStateException thrown = expectThrows(IllegalStateException.class, node::start);
            final String reported = thrown.getMessage() + (thrown.getCause() == null ? "" : " / " + thrown.getCause().getMessage());
            assertTrue("the failure must say why it will not choose: " + reported, reported.contains("reflection order"));
        }
    }

    /** A plugin whose action offers two equally good constructors. */
    public static final class AmbiguousPlugin extends Plugin implements ActionPlugin {

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(AmbiguousAction.INSTANCE, TransportAmbiguousAction.class));
        }
    }

    /** The action type for the ambiguous action. */
    public static final class AmbiguousAction extends ActionType<CountResponse> {
        static final AmbiguousAction INSTANCE = new AmbiguousAction();

        private AmbiguousAction() {
            super("cluster:admin/serverless_testkit/ambiguous", CountResponse::new);
        }
    }

    /** Same two collaborators, two orders. */
    public static final class TransportAmbiguousAction extends HandledTransportAction<CountRequest, CountResponse> {

        public TransportAmbiguousAction(TransportService transportService, ActionFilters actionFilters) {
            super("cluster:admin/serverless_testkit/ambiguous", transportService, actionFilters, CountRequest::new);
        }

        public TransportAmbiguousAction(ActionFilters actionFilters, TransportService transportService) {
            this(transportService, actionFilters);
        }

        @Override
        protected void doExecute(Task task, CountRequest request, ActionListener<CountResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("never built"));
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json");
            builder.method(
                method,
                body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
            );
            final HttpResponse<String> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
