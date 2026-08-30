/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.auth;

import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code /_serverless/security/users/{user}} — creating, checking and removing accounts.
 *
 * <p>This exists because a system whose only credential is the one in its configuration file is not a
 * system anybody operates. It is also the plugin's second hook doing real work: a plugin serving its own
 * endpoint, which writes to its own index, through the client the host handed it.
 *
 * <p><b>Only the configured account may call it, and that is one rule rather than a role model.</b>
 * Without an {@code ActionFilter} layer there is no privilege evaluation in this shell, so the choice is
 * between account management being open to every authenticated caller — which makes authentication
 * pointless, since anyone who can log in can mint themselves another account — and a single hard-coded
 * check. The single check is chosen, and named for what it is. Anything more elaborate would look like
 * authorization while being enforced in exactly one place.
 *
 * <p><b>The caller's identity is read on the REST thread, before any work is handed off.</b> It lives in a
 * {@link ThreadContext} transient, and transients do not follow work onto another pool unless they are
 * explicitly carried; reading it here and capturing the value is what makes the check happen against the
 * caller rather than against whoever the executor thread last served.
 */
public final class UsersHandler extends BaseRestHandler {

    private final CredentialStore store;
    private final Supplier<ThreadContext> context;
    private final Supplier<java.util.concurrent.ExecutorService> executor;

    /**
     * Creates the handler.
     *
     * @param store the accounts
     * @param context supplies the thread context, which does not exist when this is constructed
     * @param executor supplies the plugin's own pool, for the same reason the request path uses it
     */
    UsersHandler(CredentialStore store, Supplier<ThreadContext> context, Supplier<java.util.concurrent.ExecutorService> executor) {
        this.store = store;
        this.context = context;
        this.executor = executor;
    }

    @Override
    public String getName() {
        return "serverless_auth_users_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_serverless/security/users/{user}"),
            new Route(RestRequest.Method.GET, "/_serverless/security/users/{user}"),
            new Route(RestRequest.Method.DELETE, "/_serverless/security/users/{user}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Every parameter is read before any early return: BaseRestHandler turns a deliberate refusal into
        // a 400 about an unconsumed parameter otherwise.
        final String user = request.param("user");
        final RestRequest.Method method = request.method();

        final ThreadContext threadContext = context.get();
        final String caller = threadContext == null ? null : threadContext.<String>getTransient(ServerlessAuthPlugin.PRINCIPAL);

        final char[] password;
        if (method == RestRequest.Method.PUT) {
            if (request.hasContentOrSourceParam() == false) {
                return channel -> error(channel, RestStatus.BAD_REQUEST, "missing_body", "a body with a password is required");
            }
            final Map<String, Object> body;
            try (org.opensearch.core.xcontent.XContentParser parser = request.contentOrSourceParamParser()) {
                body = parser.map();
            }
            final Object offered = body.get("password");
            if (offered == null || offered.toString().isEmpty()) {
                return channel -> error(channel, RestStatus.BAD_REQUEST, "missing_password", "the body must contain a password");
            }
            password = offered.toString().toCharArray();
        } else {
            password = null;
        }

        if (store.bootstrapUser().equals(caller) == false) {
            if (password != null) {
                Arrays.fill(password, '\0');
            }
            // 403, not 404: the caller is authenticated and this endpoint plainly exists, so hiding it
            // would only make a legitimate operator think the plugin was misconfigured.
            return channel -> error(
                channel,
                RestStatus.FORBIDDEN,
                "not_permitted",
                "only the configured account ["
                    + store.bootstrapUser()
                    + "] may manage accounts; this deployment authenticates without authorizing, so there "
                    + "is no role that could grant it"
            );
        }

        // Off the transport thread: everything below is a get, an index or a delete against the object
        // store, and the pool that reads HTTP is not the pool to wait on those from. The plugin's own pool
        // rather than the node's GENERIC, because the shell's client runs its work on GENERIC and waiting
        // there for a GENERIC task is how a pool deadlocks under load.
        final java.util.concurrent.ExecutorService pool = executor.get();
        if (pool == null) {
            return channel -> error(channel, RestStatus.SERVICE_UNAVAILABLE, "not_started", "this node has not finished starting");
        }
        return channel -> pool.execute(() -> {
            try {
                run(channel, method, user, password);
            } catch (Exception e) {
                try {
                    respond(channel, e);
                } catch (IOException nested) {
                    logger.error("failed to report an account management failure", nested);
                }
            } finally {
                if (password != null) {
                    Arrays.fill(password, '\0');
                }
            }
        });
    }

    private void run(RestChannel channel, RestRequest.Method method, String user, char[] password) throws Exception {
        if (method == RestRequest.Method.PUT) {
            store.put(user, password);
            final XContentBuilder body = channel.newBuilder().startObject().field("user", user).field("acknowledged", true).endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, body));
            return;
        }
        if (method == RestRequest.Method.DELETE) {
            final boolean found = store.remove(user);
            final XContentBuilder body = channel.newBuilder().startObject().field("user", user).field("found", found).endObject();
            channel.sendResponse(new BytesRestResponse(found ? RestStatus.OK : RestStatus.NOT_FOUND, body));
            return;
        }
        final boolean exists = store.exists(user);
        // Never the stored record, not even to the account that may manage accounts. There is no operation
        // that needs it, and an endpoint that hands out password hashes turns one leaked credential into
        // an offline attack on every other one.
        final XContentBuilder body = channel.newBuilder().startObject().field("user", user).field("exists", exists).endObject();
        channel.sendResponse(new BytesRestResponse(exists ? RestStatus.OK : RestStatus.NOT_FOUND, body));
    }

    private void respond(RestChannel channel, Exception e) throws IOException {
        if (e instanceof IllegalArgumentException) {
            error(channel, RestStatus.BAD_REQUEST, "invalid_username", e.getMessage());
            return;
        }
        if (e instanceof org.opensearch.action.NoShardAvailableActionException) {
            // The account index is one shard like any other: it may be unowned or mid-activation, and the
            // node that will own it does not exist until something asks for it. Asking again shortly is
            // genuinely the fix, so the response says so instead of reporting a server error.
            error(
                channel,
                RestStatus.SERVICE_UNAVAILABLE,
                "accounts_unavailable",
                "the account index is not being served yet, which resolves as a node picks it up; retry: " + e.getMessage()
            );
            return;
        }
        channel.sendResponse(new BytesRestResponse(channel, e));
    }

    private static void error(RestChannel channel, RestStatus status, String type, String reason) {
        try {
            final XContentBuilder body = channel.newErrorBuilder()
                .startObject()
                .startObject("error")
                .field("type", type)
                .field("reason", reason)
                .endObject()
                .field("status", status.getStatus())
                .endObject();
            channel.sendResponse(new BytesRestResponse(status, body));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }
}
