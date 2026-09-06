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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A plugin's own action, run on a node the plugin chose, over an authenticated hop.
 *
 * <p><b>The backlog said a plugin could not forward its own action. It could.</b> Every piece was already
 * present: the projected cluster state carries every live member, so a plugin finds a peer exactly as it
 * would on a classic node; a {@code HandledTransportAction} registers its own handler on every node; and
 * the thread context travels with a transport request, so the caller a plugin set arrives on the far side.
 * The first test here passes with the shell's interceptor removed, which is how that was established
 * rather than assumed.
 *
 * <p><b>What was actually missing was authentication of that hop.</b> The sender signed nothing and the
 * receiver checked nothing, so any process that could reach the transport port could invoke any plugin's
 * action — the credential API an auth plugin ships included. That is the second test, and it is the one
 * that fails without the interceptor.
 *
 * <p><b>The plugin here uses no shell API.</b> {@code ClusterService} and {@code TransportService} are
 * core types, reached the way core hands them to an action; nothing below names anything under
 * {@code org.opensearch.serverless}. A plugin written for OpenSearch forwards its own action here without
 * being rewritten, and the shell authenticates underneath it.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessPluginHopTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    /** The header the plugin sets for its caller, which must survive the hop. */
    private static final String CALLER = "x-testkit-caller";

    /** Captures a node's transport service, so a test can send from it without the shell's signing. */
    private static final AtomicReference<TransportService> STRANGER_TRANSPORT = new AtomicReference<>();

    /** Which node ran this, and what caller it saw. */
    public static final class WhereAction extends ActionType<WhereResponse> {
        static final WhereAction INSTANCE = new WhereAction();
        static final String NAME = "cluster:admin/serverless_testkit/where";

        private WhereAction() {
            super(NAME, WhereResponse::new);
        }
    }

    /** The request: the node to answer on, by name, or null for "wherever this arrives". */
    public static final class WhereRequest extends ActionRequest {

        private final String target;

        WhereRequest(String target) {
            this.target = target;
        }

        WhereRequest(StreamInput in) throws IOException {
            super(in);
            this.target = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(target);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** The answer: the node that ran it, and the caller it could see. */
    public static final class WhereResponse extends ActionResponse {

        private final String ranOn;
        private final String caller;

        WhereResponse(String ranOn, String caller) {
            this.ranOn = ranOn;
            this.caller = caller;
        }

        WhereResponse(StreamInput in) throws IOException {
            this.ranOn = in.readString();
            this.caller = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(ranOn);
            out.writeOptionalString(caller);
        }
    }

    /**
     * The action, in the shape a classic node's would be: it answers locally, or hands itself to the node
     * the caller named. The forwarding is the plugin's own decision and uses only core types.
     */
    public static final class TransportWhereAction extends HandledTransportAction<WhereRequest, WhereResponse> {

        private final TransportService transportService;
        private final ClusterService clusterService;

        public TransportWhereAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
            super(WhereAction.NAME, transportService, actionFilters, WhereRequest::new);
            this.transportService = transportService;
            this.clusterService = clusterService;
        }

        @Override
        protected void doExecute(Task task, WhereRequest request, ActionListener<WhereResponse> listener) {
            final ThreadContext context = transportService.getThreadPool().getThreadContext();
            final DiscoveryNode local = transportService.getLocalNode();
            if (request.target == null || request.target.equals(local.getName())) {
                listener.onResponse(new WhereResponse(local.getName(), context.getHeader(CALLER)));
                return;
            }
            DiscoveryNode peer = null;
            for (DiscoveryNode candidate : clusterService.state().nodes()) {
                if (candidate.getName().equals(request.target)) {
                    peer = candidate;
                    break;
                }
            }
            if (peer == null) {
                listener.onFailure(new IllegalArgumentException("no node named [" + request.target + "] in the cluster state"));
                return;
            }
            try {
                transportService.connectToNode(peer);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            transportService.sendRequest(peer, WhereAction.NAME, new WhereRequest((String) null), new TransportResponseHandler<WhereResponse>() {
                @Override
                public WhereResponse read(StreamInput in) throws IOException {
                    return new WhereResponse(in);
                }

                @Override
                public void handleResponse(WhereResponse response) {
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    /** Ships the action and a REST handler that names a node and a caller. */
    public static final class WherePlugin extends Plugin implements ActionPlugin {

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(WhereAction.INSTANCE, TransportWhereAction.class));
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
                    return "serverless_testkit_where";
                }

                @Override
                public List<Route> routes() {
                    return List.of(new Route(RestRequest.Method.GET, "/_testkit/where/{node}"));
                }

                @Override
                protected RestChannelConsumer prepareRequest(RestRequest request, org.opensearch.transport.client.node.NodeClient client) {
                    final String node = request.param("node");
                    return channel -> {
                        final ThreadContext context = client.threadPool().getThreadContext();
                        // The caller this plugin authenticated, set the way a plugin sets one. It must
                        // arrive on the far side, or an action that runs elsewhere runs anonymously.
                        try (ThreadContext.StoredContext ignored = context.newStoredContext(false)) {
                            context.putHeader(CALLER, "alice");
                            client.execute(WhereAction.INSTANCE, new WhereRequest(node), new ActionListener<>() {
                                @Override
                                public void onResponse(WhereResponse response) {
                                    try (XContentBuilder builder = channel.newBuilder()) {
                                        builder.startObject();
                                        builder.field("ran_on", response.ranOn);
                                        builder.field("caller", response.caller);
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
                    };
                }
            });
        }
    }

    /** Grabs a node's transport service, so the stranger can send without the shell signing for it. */
    public static final class TransportCapturingPlugin extends Plugin implements ActionPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(CaptureAction.INSTANCE, TransportCaptureAction.class));
        }
    }

    /** Exists only so its transport action's constructor is handed the transport service. */
    public static final class CaptureAction extends ActionType<WhereResponse> {
        static final CaptureAction INSTANCE = new CaptureAction();

        private CaptureAction() {
            super("cluster:admin/serverless_testkit/capture", WhereResponse::new);
        }
    }

    /** Captures and answers nothing. */
    public static final class TransportCaptureAction extends HandledTransportAction<WhereRequest, WhereResponse> {
        public TransportCaptureAction(TransportService transportService, ActionFilters actionFilters) {
            super(CaptureAction.INSTANCE.name(), transportService, actionFilters, WhereRequest::new);
            STRANGER_TRANSPORT.set(transportService);
        }

        @Override
        protected void doExecute(Task task, WhereRequest request, ActionListener<WhereResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("this action exists only to capture the transport service"));
        }
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-plugin-hop")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * A plugin sends its own action to a node it named, and the caller travels with it.
     *
     * <p><b>This is a regression guard, not proof of a new capability.</b> It passes with the shell's
     * interceptor removed — which is the evidence that forwarding already worked and the backlog entry
     * claiming otherwise was wrong. What it protects is that authenticating the hop did not break it:
     * signing happens on the caller's context around a send that is asynchronous, and getting that wrong
     * would strand every plugin forward.
     *
     * <p>Both directions are asserted, and the local one matters as much: a node answering for itself
     * takes no hop, so nothing signs, and an interceptor that demanded a signature anyway would refuse a
     * plugin's action on the node it was already running on.
     */
    public void testAPluginRunsItsActionOnTheNodeItNamed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("hop-a"), List.of(new WherePlugin()));
            ServerlessNode second = new ServerlessNode(nodeSettings("hop-b"), List.of(new WherePlugin()))
        ) {
            first.start();
            first.setMetadataPlane(plane);
            second.start();
            second.setMetadataPlane(plane);
            // Each publishes a lease, which is what one node reads to learn where another is.
            new BackgroundReconciler(first, plane).tick(clock.get());
            new BackgroundReconciler(second, plane).tick(clock.get());
            plane.membership().refreshIfOlderThan(0);
            // And then the resync, which is what puts those members into the projected cluster state --
            // the reconcile pass runs it on a cadence of its own, every RESYNC_EVERY_PASSES backstops.
            // Called directly rather than waited for, because ten minutes is not a test.
            first.syncFrom(plane);
            second.syncFrom(plane);

            final Response elsewhere = get(first, "/_testkit/where/hop-b");
            assertEquals(elsewhere.body(), 200, elsewhere.status());
            assertTrue("the action must have run on the node the plugin named: " + elsewhere.body(), elsewhere.has("\"ran_on\":\"hop-b\""));
            assertTrue("and the caller must have crossed the hop: " + elsewhere.body(), elsewhere.has("\"caller\":\"alice\""));

            final Response here = get(first, "/_testkit/where/hop-a");
            assertEquals(here.body(), 200, here.status());
            assertTrue("the local path still answers locally: " + here.body(), here.has("\"ran_on\":\"hop-a\""));
            assertTrue("with its caller: " + here.body(), here.has("\"caller\":\"alice\""));
        }
    }

    /**
     * A stranger cannot invoke a plugin's action, which is the half of this that is a security fix.
     *
     * <p>Before the hop was authenticated, a {@code HandledTransportAction} registered its handler and
     * nothing checked who called it — so any process that could reach the transport port could invoke any
     * plugin's action, the auth plugin's credential API included. The stranger here is what an outsider
     * actually has: a transport service and the target's address, no metadata plane, and no membership.
     */
    public void testAStrangerCannotInvokeAPluginsAction() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        STRANGER_TRANSPORT.set(null);
        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("hop-owner"), List.of(new WherePlugin()));
            ServerlessNode stranger = new ServerlessNode(nodeSettings("hop-stranger"), List.of(new TransportCapturingPlugin()))
        ) {
            owner.start();
            owner.setMetadataPlane(plane);
            stranger.start();

            final TransportService transport = STRANGER_TRANSPORT.get();
            assertNotNull("the stranger's transport service must have been captured", transport);
            transport.connectToNode(owner.localNode());

            final PlainActionFuture<WhereResponse> future = PlainActionFuture.newFuture();
            transport.sendRequest(
                owner.localNode(),
                WhereAction.NAME,
                new WhereRequest((String) null),
                new TransportResponseHandler<WhereResponse>() {
                    @Override
                    public WhereResponse read(StreamInput in) throws IOException {
                        return new WhereResponse(in);
                    }

                    @Override
                    public void handleResponse(WhereResponse response) {
                        future.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        future.onFailure(e);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                }
            );

            final Exception refused = expectThrows(
                Exception.class,
                () -> future.actionGet(org.opensearch.common.unit.TimeValue.timeValueSeconds(30))
            );
            final StringBuilder chain = new StringBuilder();
            for (Throwable t = refused; t != null; t = t.getCause()) {
                chain.append(t).append(" | ");
            }
            assertTrue("a stranger's plugin action must be refused: " + chain, chain.toString().contains("transport token"));
        }
    }

    private record Response(int status, String body) {
        boolean has(String fragment) {
            return body.contains(fragment);
        }
    }

    private static Response get(ServerlessNode node, String path) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                    .timeout(Duration.ofSeconds(60))
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Response(response.statusCode(), response.body());
        }
    }
}
