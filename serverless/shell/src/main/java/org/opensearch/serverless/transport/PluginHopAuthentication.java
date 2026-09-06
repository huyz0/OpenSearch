/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.transport.TransportResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Authenticates a plugin's own action when it crosses a node boundary.
 *
 * <p><b>What was missing, precisely — and it was not the forwarding.</b> A plugin could already find a
 * peer, because the projected cluster state carries every live member and
 * {@code clusterService.state().nodes()} answers exactly as it does on a classic node. A
 * {@code HandledTransportAction} already registered its own handler on the far side. The thread context
 * already travelled, so the caller a plugin set already arrived. {@code ServerlessPluginHopTests} passes
 * its forwarding test with this class removed, which is how that was established.
 *
 * <p>What did not happen was <b>authentication</b>. A plugin calling {@code transportService.sendRequest}
 * sent nothing the receiver could check, and the receiver checked nothing — so any process that could
 * reach the transport port could invoke any plugin's action, including the ones a plugin ships to manage
 * its own credentials. That is a hole, not a missing feature, and it is what this closes.
 *
 * <p><b>Done as an interceptor, which is core's own extension point, so that a plugin needs no shell API
 * at all.</b> The alternative was a method on the shell's client saying "run this over there", which
 * every plugin would have had to be rewritten against and which {@code serverless/auth} could not have
 * called without naming the shell. A plugin does what it does on a classic node and the hop is signed
 * underneath it.
 *
 * <p><b>Only plugin actions, and decided per message rather than per registration.</b> Signing everything
 * would put a MAC on the transport handshake, which runs while the connection that would carry it is still
 * being established. Deciding at registration is not possible either: a plugin's handler registers from
 * its own constructor, which the shell runs <em>while</em> it is still building the registry this consults.
 * So every handler is wrapped and the wrapper asks, once a message has actually arrived, whether this
 * action is one a plugin brought. Everything else is delegated untouched, which is every path that existed
 * before this class.
 *
 * <p><b>What the far side gets.</b> {@link ShardRouter#withMac} puts the deployment's MAC on the outgoing
 * request and marks it as plugin-originated when the caller is acting as a plugin; the thread context
 * travels with the request as it always has, so the authenticated principal a plugin set arrives with it.
 * That is the "identity crosses the hop" half, and it comes from reusing the shell's own signing rather
 * than inventing a second scheme.
 *
 * <p><b>One caveat worth naming.</b> The MAC covers a digest of the serialised request, so a plugin whose
 * request does not serialise identically on both sides fails verification rather than running with a
 * request nobody checked. That is the safe direction, and it is the same assumption the shell's own
 * forwarded requests already make of themselves.
 */
public final class PluginHopAuthentication implements TransportInterceptor {

    private final Supplier<ShardRouter> router;
    private final Function<String, Boolean> isPluginAction;

    /**
     * Creates the interceptor.
     *
     * <p>Both are suppliers because this is built with the transport service, which is built before the
     * router exists and long before the plugins' actions have been constructed.
     *
     * @param router supplies the router whose signing and verification this reuses, or null before there is one
     * @param isPluginAction answers whether an action name is one a plugin brought
     */
    public PluginHopAuthentication(Supplier<ShardRouter> router, Function<String, Boolean> isPluginAction) {
        this.router = router;
        this.isPluginAction = isPluginAction;
    }

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
                final ShardRouter signer = signerFor(action);
                if (signer == null) {
                    sender.sendRequest(connection, action, request, options, handler);
                    return;
                }
                // The same try-with-resources the shell's own forwards use: the headers go on the caller's
                // context so they are written into the request, and come off again when this returns.
                // sendRequest serialises the context synchronously, so the asynchronous completion below
                // does not need them.
                try (var ignored = signer.withMac(action, request)) {
                    sender.sendRequest(connection, action, request, options, handler);
                } catch (IOException e) {
                    // Reported to the caller's own handler rather than thrown: a sender that throws where
                    // an asynchronous send was expected leaves the caller waiting for a listener that will
                    // never fire.
                    handler.handleException(new org.opensearch.transport.SendRequestTransportException(connection.getNode(), action, e));
                }
            }
        };
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
        String action,
        String executor,
        boolean forceExecution,
        TransportRequestHandler<T> actualHandler
    ) {
        return new TransportRequestHandler<T>() {
            @Override
            public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
                final ShardRouter verifier = signerFor(action);
                if (verifier != null) {
                    // Throwing here is the refusal: the transport turns it into a failure response, and the
                    // plugin's action never sees a request nobody could vouch for.
                    verifier.requireMac(action, request);
                }
                actualHandler.messageReceived(request, channel, task);
            }
        };
    }

    /** The router to sign or verify with, or null when this action is not a plugin's or there is no router yet. */
    private ShardRouter signerFor(String action) {
        if (Boolean.TRUE.equals(isPluginAction.apply(action)) == false) {
            return null;
        }
        // Null until the node has built its router. Nothing can have sent a plugin action before then --
        // the actions themselves are built later still -- so this is a guard rather than a path.
        return router.get();
    }
}
