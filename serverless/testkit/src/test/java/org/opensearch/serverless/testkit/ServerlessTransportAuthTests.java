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
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.ForwardedIndexRequest;
import org.opensearch.serverless.transport.ForwardedIndexResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Forwarded requests are authenticated per request, and this is the side of that which matters: the
 * requests that must be refused.
 *
 * <p>Every two-node test in this module shares one metadata plane, so every node reads the same secret
 * and the check on the receiving side was invisible to all of them: a node presenting nothing, the wrong
 * thing, or the same thing twice had never been tried. These are those cases, through the shell's public
 * API and the transport only. The header names and the header's shape are the wire protocol, which a
 * test is entitled to know; nothing here reaches into the router.
 *
 * <p><b>Two headers, for the length of the move.</b> A sender presents both the per-request MAC
 * ({@code generation:sender:timestamp:nonce:mac}) and the static token it replaces, and a receiver
 * verifies the MAC whenever one is presented and falls back to the token only in its absence. So a
 * wrong MAC is refused however good the token beside it is, and a wrong token is only reachable as a
 * refusal from a sender that signs nothing -- which is what a raw send from a stranger is.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessTransportAuthTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    /** The header a forwarded request carries its per-request authentication in: the wire protocol, verbatim. */
    private static final String MAC_HEADER = "x-serverless-transport-mac";

    /** The header the static deployment secret travelled in, still presented beside the MAC. */
    private static final String TOKEN_HEADER = "x-serverless-transport-token";

    /** The transport service of the node that installed {@link TransportCapturingPlugin}. */
    private static final AtomicReference<TransportService> TRANSPORT = new AtomicReference<>();

    /** The last forwarded request the interceptor saw leave, and the MAC header it left under. */
    private static final AtomicReference<TransportRequest> LAST_REQUEST = new AtomicReference<>();
    private static final AtomicReference<String> LAST_MAC = new AtomicReference<>();

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-transport-auth")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    /** A request that is never sent: the action exists so its constructor is handed the transport service. */
    public static final class ProbeRequest extends ActionRequest {
        ProbeRequest() {}

        ProbeRequest(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Its response, equally unused. */
    public static final class ProbeResponse extends ActionResponse {
        ProbeResponse(StreamInput in) {}

        @Override
        public void writeTo(StreamOutput out) {}
    }

    /** The action type. */
    public static final class ProbeAction extends ActionType<ProbeResponse> {
        static final ProbeAction INSTANCE = new ProbeAction();

        private ProbeAction() {
            super("cluster:admin/serverless_testkit/transport_probe", ProbeResponse::new);
        }
    }

    /**
     * The action, whose only job is to be built: the shell resolves its constructor against the node's
     * services, and the transport service is one of them. There is no other public route to a node's
     * transport service, which is as it should be; a plugin is what an outsider on the port looks like
     * from inside the process.
     */
    public static final class TransportProbeAction extends HandledTransportAction<ProbeRequest, ProbeResponse> {
        public TransportProbeAction(TransportService transportService, ActionFilters actionFilters) {
            super(ProbeAction.INSTANCE.name(), transportService, actionFilters, ProbeRequest::new);
            TRANSPORT.set(transportService);
        }

        @Override
        protected void doExecute(Task task, ProbeRequest request, ActionListener<ProbeResponse> listener) {
            listener.onFailure(new UnsupportedOperationException("the probe action is never executed"));
        }
    }

    /**
     * A plugin that ships the action above, and watches every outgoing forwarded write leave.
     *
     * <p>The interceptor is core's own mechanism, the one a security plugin uses to see what crosses the
     * wire. It records the request object and the MAC header that was on the context as it went, which is
     * exactly what a bystander on a plaintext transport captures -- and exactly what a replay re-sends.
     */
    public static final class TransportCapturingPlugin extends Plugin implements ActionPlugin, NetworkPlugin {
        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(ProbeAction.INSTANCE, TransportProbeAction.class));
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return new AsyncSender() {
                        @Override
                        public <T extends TransportResponse> void sendRequest(
                            Transport.Connection connection,
                            String action,
                            TransportRequest request,
                            TransportRequestOptions options,
                            TransportResponseHandler<T> handler
                        ) {
                            if (ForwardedIndexRequest.ACTION.equals(action)) {
                                LAST_REQUEST.set(request);
                                LAST_MAC.set(threadContext.getHeader(MAC_HEADER));
                            }
                            sender.sendRequest(connection, action, request, options, handler);
                        }
                    };
                }
            });
        }
    }

    /**
     * A member of the deployment whose MAC does not verify is refused, and nothing is written.
     *
     * <p>The bad header gets there the way a bad one would in life: it is already in the sender's thread
     * context, and the router does not overwrite a header that is present. It is well formed -- the right
     * generation, this sender, a timestamp inside the window, a fresh nonce -- and only the MAC field is
     * wrong, so what is refused is the signature and not the envelope. The static token beside it is the
     * deployment's real one, which is the point: a receiver that knows the MAC does not fall back to the
     * token when the MAC is bad. The write goes out through the node's ordinary client, so what is
     * refused is the shell's own forward and not a request this test assembled by hand, and the control at
     * the end -- the same write with nothing planted -- is what says the refusal was about the MAC.
     */
    public void testAForwardedWriteWithTheWrongMacIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("mac-owner"), List.of());
            ServerlessNode other = new ServerlessNode(nodeSettings("mac-other"), List.of())
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);
            assertTrue(
                "the other node must not own the shard, so the write has to be forwarded",
                other.reconciler().openShards().isEmpty()
            );

            final String tampered = plane.transportSecrets().generation()
                + ":"
                + other.localNode().getId()
                + ":"
                + clock.get()
                + ":"
                + "00112233445566778899aabbccddeeff"
                + ":"
                + "0".repeat(64);
            final ThreadContext context = other.threadPool().getThreadContext();
            final Exception refused;
            try (ThreadContext.StoredContext ignored = context.stashContext()) {
                context.putHeader(MAC_HEADER, tampered);
                refused = expectThrows(
                    Exception.class,
                    () -> other.client()
                        .index(new IndexRequest("alpha").id("k").source("{\"msg\":\"wrong mac\",\"n\":1}", XContentType.JSON))
                        .actionGet()
                );
            }
            assertTrue("refused for the signature, not for something else: " + describe(refused), mentions(refused, "transport token"));
            assertFalse(
                "a refused forward must not have written anything",
                owner.client().get(new GetRequest("alpha", "k")).actionGet().isExists()
            );

            // The control: with nothing planted the router signs the request itself.
            other.client()
                .index(new IndexRequest("alpha").id("k").source("{\"msg\":\"right mac\",\"n\":1}", XContentType.JSON))
                .actionGet();
            assertTrue(owner.client().get(new GetRequest("alpha", "k")).actionGet().isExists());
        }
    }

    /**
     * A caller on the transport port that signs nothing is refused: with no credential at all, and with
     * the wrong static token and no MAC, which is the only way a wrong token can still be presented.
     *
     * <p>This is the attack the authentication exists to stop: a process that can reach the port sends a
     * forwarded write, addressed to a shard it knows the owner holds, with no membership in the
     * deployment. The stranger here has no metadata plane and has never read the object store; it has a
     * transport service and the owner's address, which is everything an outsider has. The raw send is
     * what keeps the MAC off the request -- the shell's own client would sign it -- so this is also where
     * the token fallback is shown to refuse a wrong token rather than to let one through.
     */
    public void testAForwardedWriteWithNoCredentialOrTheWrongTokenIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        TRANSPORT.set(null);
        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("raw-owner"), List.of());
            ServerlessNode stranger = new ServerlessNode(nodeSettings("raw-stranger"), List.of(new TransportCapturingPlugin()))
        ) {
            owner.start();
            stranger.start();
            owner.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);

            final TransportService transport = TRANSPORT.get();
            assertNotNull("the stranger's transport service must have been captured", transport);
            final DiscoveryNode target = owner.localNode();
            transport.connectToNode(target);
            final ForwardedIndexRequest write = new ForwardedIndexRequest("alpha", 0, "k", "{\"msg\":\"stranger\",\"n\":1}", true);

            // Nothing at all.
            final Exception bare = expectThrows(
                Exception.class,
                () -> sendRaw(transport, target, write).actionGet(TimeValue.timeValueSeconds(30))
            );
            assertTrue("refused with no credential: " + describe(bare), mentions(bare, "transport token"));

            // The wrong static token, and no MAC beside it to be preferred.
            final ThreadContext context = stranger.threadPool().getThreadContext();
            final Exception wrongToken;
            try (ThreadContext.StoredContext ignored = context.stashContext()) {
                context.putHeader(TOKEN_HEADER, "not-this-deployment-secret");
                wrongToken = expectThrows(
                    Exception.class,
                    () -> sendRaw(transport, target, write).actionGet(TimeValue.timeValueSeconds(30))
                );
            }
            assertTrue("refused for the wrong token: " + describe(wrongToken), mentions(wrongToken, "transport token"));

            assertFalse(
                "a refused forward must not have written anything",
                owner.client().get(new GetRequest("alpha", "k")).actionGet().isExists()
            );
        }
    }

    /**
     * A captured frame re-sent verbatim is refused as a replay, even from a member with every right to
     * send a fresh one.
     *
     * <p>This is the property the static token could not have: a bystander who copied a frame off a
     * plaintext transport could re-apply it at will -- an index write, or worse a delete. The interceptor
     * records the request and its MAC exactly as they left, the same way a bystander would, and the replay
     * goes to the same owner within the window: same sender, same nonce, so the receiver has seen it.
     */
    public void testAReplayedForwardedFrameIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        TRANSPORT.set(null);
        LAST_REQUEST.set(null);
        LAST_MAC.set(null);
        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("replay-owner"), List.of());
            ServerlessNode other = new ServerlessNode(nodeSettings("replay-other"), List.of(new TransportCapturingPlugin()))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);

            other.client().index(new IndexRequest("alpha").id("k").source("{\"msg\":\"first\",\"n\":1}", XContentType.JSON)).actionGet();
            assertTrue(owner.client().get(new GetRequest("alpha", "k")).actionGet().isExists());
            final TransportRequest captured = LAST_REQUEST.get();
            final String mac = LAST_MAC.get();
            assertNotNull("the interceptor must have seen the forwarded write", captured);
            assertNotNull("and the MAC it left under", mac);

            // The same bytes under the same header, from a transport that is otherwise a member in good
            // standing: the receiver refuses on the nonce, not on who is asking.
            final TransportService transport = TRANSPORT.get();
            final DiscoveryNode target = owner.localNode();
            transport.connectToNode(target);
            final ThreadContext context = other.threadPool().getThreadContext();
            final Exception replayed;
            try (ThreadContext.StoredContext ignored = context.stashContext()) {
                context.putHeader(MAC_HEADER, mac);
                replayed = expectThrows(
                    Exception.class,
                    () -> sendRaw(transport, target, captured).actionGet(TimeValue.timeValueSeconds(30))
                );
            }
            assertTrue("refused as a replay: " + describe(replayed), mentions(replayed, "replayed"));

            // A fresh forward of the same document from the same node is still fine: it is the frame that
            // is dead, not the sender.
            other.client().index(new IndexRequest("alpha").id("k").source("{\"msg\":\"second\",\"n\":2}", XContentType.JSON)).actionGet();
            assertTrue(owner.client().get(new GetRequest("alpha", "k")).actionGet().getSourceAsString().contains("second"));
        }
    }

    /**
     * Rotating the secret keeps the fleet writing: a sender ahead of a receiver makes the receiver
     * re-read, a sender one generation behind is accepted under the previous secret, and only a sender two
     * behind is refused -- until it re-reads, after which it is served again.
     *
     * <p>Each node's copy of the secrets is driven by hand here through the public re-read, because the
     * clock is frozen and the lease-bounded refresh would otherwise never fire; what is being shown is
     * every path a generation mismatch can take, not the timer. With a moving clock the timer is what
     * makes the two-behind case rare: a node re-reads once a lease whether or not anything refused it.
     */
    public void testRotationKeepsBothGenerationsForOneWindow() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("rotate-owner"), List.of());
            ServerlessNode other = new ServerlessNode(nodeSettings("rotate-other"), List.of())
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(plane);
            other.setMetadataPlane(plane);
            owner.activateWriter(plane, "alpha", 0);
            other.syncFrom(plane);

            // Both nodes read generation 1 by forwarding once.
            write(other, "g1");
            assertTrue(owner.client().get(new GetRequest("alpha", "g1")).actionGet().isExists());
            final long first = plane.transportSecrets().generation();

            // Rotate; the sender re-reads and signs under generation 2. The receiver still holds
            // generation 1, meets a newer one, and re-reads the register rather than refusing.
            final MetadataPlane.TransportSecrets second = plane.rotateTransportSecret();
            assertEquals(first + 1, second.generation());
            assertEquals("the old secret becomes the previous one", other.transportSecrets(false).current(), second.previous());
            other.transportSecrets(true);
            write(other, "g2-ahead");
            assertTrue(
                "a sender ahead of the receiver is served",
                owner.client().get(new GetRequest("alpha", "g2-ahead")).actionGet().isExists()
            );
            assertEquals("and the receiver has caught up", second.generation(), owner.transportSecrets(false).generation());

            // Rotate again; only the receiver re-reads. The sender signs under generation 2, which is
            // now the previous secret, and is accepted for the window.
            final MetadataPlane.TransportSecrets third = plane.rotateTransportSecret();
            owner.transportSecrets(true);
            assertEquals(third.generation(), owner.transportSecrets(false).generation());
            assertEquals(second.generation(), other.transportSecrets(false).generation());
            write(other, "g2-behind");
            assertTrue(
                "a sender one generation behind is served under the previous secret",
                owner.client().get(new GetRequest("alpha", "g2-behind")).actionGet().isExists()
            );

            // Rotate once more, receiver re-reads: the sender's generation 2 is now neither current nor
            // previous, and is refused.
            plane.rotateTransportSecret();
            owner.transportSecrets(true);
            final Exception stale = expectThrows(Exception.class, () -> write(other, "g2-stale"));
            assertTrue("two generations behind is refused: " + describe(stale), mentions(stale, "secret generation"));
            assertFalse(owner.client().get(new GetRequest("alpha", "g2-stale")).actionGet().isExists());

            // And the fix is the re-read, not a restart.
            other.transportSecrets(true);
            write(other, "g4");
            assertTrue("a sender that re-read is served again", owner.client().get(new GetRequest("alpha", "g4")).actionGet().isExists());
        }
    }

    private static void write(ServerlessNode from, String id) {
        from.client().index(new IndexRequest("alpha").id(id).source("{\"msg\":\"" + id + "\",\"n\":1}", XContentType.JSON)).actionGet();
    }

    private static PlainActionFuture<ForwardedIndexResponse> sendRaw(
        TransportService transport,
        DiscoveryNode target,
        TransportRequest request
    ) {
        final PlainActionFuture<ForwardedIndexResponse> future = PlainActionFuture.newFuture();
        transport.sendRequest(
            target,
            ForwardedIndexRequest.ACTION,
            request,
            TransportRequestOptions.EMPTY,
            new TransportResponseHandler<ForwardedIndexResponse>() {
                @Override
                public ForwardedIndexResponse read(StreamInput in) throws IOException {
                    return new ForwardedIndexResponse(in);
                }

                @Override
                public void handleResponse(ForwardedIndexResponse response) {
                    future.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    future.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            }
        );
        return future;
    }

    private static boolean mentions(Throwable failure, String text) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains(text)) {
                return true;
            }
        }
        return false;
    }

    private static String describe(Throwable failure) {
        final StringBuilder out = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
            out.append(cause).append(" <- ");
        }
        return out.toString();
    }
}
