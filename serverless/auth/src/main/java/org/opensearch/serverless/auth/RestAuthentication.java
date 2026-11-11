/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.auth;

import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.http.HttpChannel;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.ThreadPool;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Authenticates REST requests for {@link ServerlessAuthPlugin}: reads the caller's credentials, checks them off the
 * transport thread under the throttle, and either admits the request with its principal or refuses it.
 */
final class RestAuthentication {

    private final ServerlessAuthPlugin plugin;

    RestAuthentication(ServerlessAuthPlugin plugin) {
        this.plugin = plugin;
    }

    /**
     * The seam. Every request passes through here before its handler sees it.
     *
     * <p><b>Two paths, because checking a credential is two very different amounts of work.</b> A
     * credential this node checked moments ago costs a keyed hash and a map lookup, and runs on the thread
     * that read the request. One it has not seen costs a deliberately slow derivation and possibly a read
     * from the object store, and blocking the HTTP event loop on either would trade the safety of every
     * request for the convenience of writing this in one branch. So a miss is handed to
     * {@link ServerlessAuthPlugin#newCheckerPool a pool of this plugin's own} and the handler runs from there.
     *
     * <p><b>The cache before the throttle, on purpose.</b> The throttle prices guesses, and a cache hit is
     * not a guess: it is a caller this node verified within the TTL. Consulting the throttle first was
     * what let one bad account behind a shared address refuse every good one behind it. The cost of this
     * order is small and stated: a guesser whose pair is waiting learns at once, rather than after the
     * wait, if a guess happens to be the password some other caller verified here in the last minute --
     * a guesser who by then holds the password either way.
     *
     * <p><b>A wait is a refusal or a delay, and the throttle says which.</b> Refusals -- no header, the
     * wrong scheme, an unreadable one, a pair or an address that has earned a wait -- are answered inline
     * with no work, because dispatching in order to say "not yet" would let an unauthenticated caller queue
     * work on this node. A delay is scheduled on the node's scheduler and submitted to the checker pool
     * when it ends, so the wait holds a connection and nothing else: no checker thread sleeps through it,
     * which would have let four requests a minute idle the whole pool. Delays are bounded by
     * {@link ServerlessAuthPlugin#CHECK_QUEUE_SIZE}, since each holds a channel and a credential, and past the bound the
     * answer is the 503 a full queue gets. The configured account is only ever delayed, which is what
     * makes it a recovery path rather than a name anyone can lock.
     *
     * @param threadContext the node's thread context, where an authenticated principal is left
     * @param headersToCopy the headers the controller preserves, none of which this uses
     * @return the wrapper every handler is registered behind
     */
    public UnaryOperator<RestHandler> getRestHandlerWrapper(ThreadContext threadContext, Set<RestHeaderDefinition> headersToCopy) {
        plugin.context.set(threadContext);
        return original -> (RestHandler) (request, channel, client) -> {
            final String header = request.header("Authorization");
            if (header == null || header.isBlank()) {
                refuse(channel, RestStatus.UNAUTHORIZED, "no_credentials", "no credentials were offered", true);
                return;
            }
            if (header.regionMatches(true, 0, "Basic ", 0, "Basic ".length()) == false) {
                // Bearer is in core's token vocabulary and nothing here issues one, so accepting the header
                // and ignoring it would be worse than saying which scheme this node understands.
                refuse(
                    channel,
                    RestStatus.UNAUTHORIZED,
                    "unsupported_scheme",
                    "this node authenticates with HTTP Basic; no other scheme is configured",
                    true
                );
                return;
            }
            final Credential offered = decodeBasic(header);
            if (offered == null) {
                refuse(channel, RestStatus.UNAUTHORIZED, "malformed_credentials", "the Basic credentials could not be read", true);
                return;
            }
            final String user = offered.user();
            final char[] password = offered.password();

            if (plugin.store.isCached(user, password)) {
                Arrays.fill(password, '\0');
                admit(threadContext, user);
                original.handleRequest(request, channel, client);
                return;
            }

            // isCached declines once a second so the marker gets read on the slow path; that decline is
            // not a miss, and a caller with a live entry is neither refused nor delayed for it.
            final String address = remoteAddress(request);
            final LoginThrottle.Decision decision = plugin.store.isKnown(user, password)
                ? LoginThrottle.Decision.NOW
                : plugin.throttle.decide(user, address, plugin.store.bootstrapUser().equals(user));
            if (decision.refuse()) {
                Arrays.fill(password, '\0');
                refuse(
                    channel,
                    RestStatus.TOO_MANY_REQUESTS,
                    "too_many_attempts",
                    "too many failed attempts " + decision.because() + "; try again in " + decision.retryAfterSeconds() + "s",
                    false,
                    decision.retryAfterSeconds()
                );
                return;
            }

            final java.util.concurrent.ExecutorService pool = plugin.checkers.get();
            final ThreadPool scheduler = plugin.threadPool.get();
            if (pool == null || scheduler == null) {
                // The HTTP transport binds before plugins build their components, so there is a window at
                // startup where requests arrive and there is nowhere to check them. Brief, and a 503.
                Arrays.fill(password, '\0');
                refuse(channel, RestStatus.SERVICE_UNAVAILABLE, "authentication_unavailable", "this node has not finished starting", false);
                return;
            }
            final Runnable slowPath = () -> submit(pool, threadContext, user, password, address, request, channel, client, original);
            if (decision.immediate()) {
                slowPath.run();
                return;
            }

            if (plugin.pendingDelayed.incrementAndGet() > plugin.delayedCap) {
                plugin.pendingDelayed.decrementAndGet();
                Arrays.fill(password, '\0');
                refuse(
                    channel,
                    RestStatus.SERVICE_UNAVAILABLE,
                    "authentication_overloaded",
                    "too many checks are waiting on this node; retry",
                    false,
                    decision.retryAfterSeconds()
                );
                return;
            }
            try {
                // SAME: the scheduler thread does nothing but hand the check to the pool, which is a
                // non-blocking submit; the derivation never runs there.
                scheduler.schedule(() -> {
                    plugin.pendingDelayed.decrementAndGet();
                    slowPath.run();
                }, TimeValue.timeValueMillis(decision.waitMillis()), ThreadPool.Names.SAME);
            } catch (org.opensearch.core.concurrency.OpenSearchRejectedExecutionException e) {
                plugin.pendingDelayed.decrementAndGet();
                Arrays.fill(password, '\0');
                refuse(channel, RestStatus.SERVICE_UNAVAILABLE, "authentication_unavailable", "this node is shutting down", false);
            }
        };
    }

    /** Hands an uncached check to the checker pool, or says the queue is full. */
    void submit(
        java.util.concurrent.ExecutorService pool,
        ThreadContext threadContext,
        String user,
        char[] password,
        String address,
        RestRequest request,
        RestChannel channel,
        org.opensearch.transport.client.node.NodeClient client,
        RestHandler original
    ) {
        try {
            pool.execute(() -> {
                try {
                    if (check(threadContext, user, password, address, channel)) {
                        original.handleRequest(request, channel, client);
                    }
                } catch (Exception e) {
                    try {
                        channel.sendResponse(new BytesRestResponse(channel, e));
                    } catch (Exception nested) {
                        ServerlessAuthPlugin.LOGGER.error("failed to report an authentication failure", nested);
                    }
                } finally {
                    Arrays.fill(password, '\0');
                }
            });
        } catch (org.opensearch.core.concurrency.OpenSearchRejectedExecutionException e) {
            Arrays.fill(password, '\0');
            // The queue is full. Not a 429: this caller did nothing wrong, the node is simply busy
            // checking, and a second from now it may well not be.
            refuse(
                channel,
                RestStatus.SERVICE_UNAVAILABLE,
                "authentication_overloaded",
                "this node's authentication queue is full; retry",
                false,
                1
            );
        }
    }

    /**
     * Names the caller's address for the throttle, or null when the channel has none to give.
     *
     * <p>The address and not the port: every connection has a fresh port, and a key that changed per
     * connection would count nothing.
     *
     * <p><b>Through a proxy only when told which proxies to trust.</b> With {@link ServerlessAuthPlugin#TRUSTED_PROXIES}
     * set, a connection from one of them is attributed to the rightmost {@code X-Forwarded-For} entry
     * that is not itself a trusted proxy: the address the nearest proxy of ours actually saw, rather than
     * whatever the client chose to write at the left of the header. A header this cannot read is a header
     * this does not believe, and the proxy's own address is the one fact left.
     */
    String remoteAddress(RestRequest request) {
        final HttpChannel http = request.getHttpChannel();
        final InetSocketAddress remote = http == null ? null : http.getRemoteAddress();
        if (remote == null || remote.getAddress() == null) {
            return null;
        }
        final InetAddress peer = remote.getAddress();
        if (plugin.trustedProxies.isEmpty() || isTrustedProxy(peer) == false) {
            return NetworkAddress.format(peer);
        }
        final String forwarded = request.header("X-Forwarded-For");
        if (forwarded == null) {
            return NetworkAddress.format(peer);
        }
        final String[] hops = forwarded.split(",");
        for (int i = hops.length - 1; i >= 0; i--) {
            final String hop = hops[i].trim();
            if (InetAddresses.isInetAddress(hop) == false) {
                break;
            }
            final InetAddress candidate = InetAddresses.forString(hop);
            if (isTrustedProxy(candidate) == false) {
                return NetworkAddress.format(candidate);
            }
        }
        return NetworkAddress.format(peer);
    }

    boolean isTrustedProxy(InetAddress address) {
        for (ServerlessAuthPlugin.TrustedNetwork network : plugin.trustedProxies) {
            if (network.contains(address)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks a credential that was not already known, and refuses if it does not hold up.
     *
     * @param threadContext where the principal is left on success
     * @param user the username offered
     * @param password the password offered, zeroed before this returns
     * @param address the caller's address, for the throttle
     * @param channel the channel to refuse on
     * @return true if the handler should run
     * @throws Exception if the refusal cannot be written
     */
    boolean check(ThreadContext threadContext, String user, char[] password, String address, RestChannel channel) throws Exception {
        final CredentialStore.Verdict verdict;
        try {
            verdict = plugin.store.verify(user, password);
        } finally {
            Arrays.fill(password, '\0');
        }

        if (verdict.unavailable() != null) {
            // Not a 401, and not a failure the throttle counts: the caller may well have offered a
            // perfectly good credential, and telling them it was rejected would send them to change a
            // password that was never the problem.
            return refuse(channel, RestStatus.SERVICE_UNAVAILABLE, "authentication_unavailable", verdict.unavailable(), false);
        }
        if (verdict.authenticated() == false) {
            plugin.throttle.failed(user, address);
            return refuse(channel, RestStatus.UNAUTHORIZED, "authentication_failed", "the credentials offered were not accepted", true);
        }
        plugin.throttle.succeeded(user, address);
        admit(threadContext, verdict.principal());
        return true;
    }

    /** Records who the caller is, for the handler and for any plugin that asks the identity service. */
    static void admit(ThreadContext threadContext, String principal) {
        // Each request runs in a stashed context, so this is set once per request and never overwrites a
        // value from another one -- but a wrapped handler that retries would, and putTransient throws on a
        // second write rather than quietly replacing an identity.
        if (threadContext.getTransient(ServerlessAuthPlugin.PRINCIPAL) == null) {
            threadContext.putTransient(ServerlessAuthPlugin.PRINCIPAL, principal);
        }
    }

    /**
     * Reads a Basic header into a username and a password.
     *
     * <p><b>Standard Base64, not the URL-safe alphabet.</b> RFC 7617 says {@code base64(user:pass)} in the
     * alphabet with {@code +} and {@code /}; core's {@link BasicAuthToken} decodes with
     * {@code Base64.getUrlDecoder()}, which rejects exactly those two characters, so roughly one header in
     * two with a random password would fail to parse. Using core's class here would have inherited that,
     * so this decodes the header itself and leaves the token type to {@link ServerlessSubject}, where it
     * is the caller who constructed it.
     *
     * <p><b>Never a {@code String} for the password.</b> A string is immutable and lives until the
     * collector gets to it, so zeroing a {@code char[]} copied out of one was cosmetic. The decoded bytes
     * are split at the colon, the password goes straight into a {@code char[]} the caller owns and zeroes,
     * and every intermediate buffer is wiped on the way out.
     *
     * @param header the Authorization header
     * @return the credential, or null if the header is not readable
     */
    static Credential decodeBasic(String header) {
        final byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(header.substring("Basic ".length()).trim());
        } catch (IllegalArgumentException e) {
            return null;
        }
        try {
            int colon = -1;
            for (int i = 0; i < decoded.length; i++) {
                if (decoded[i] == ':') {
                    colon = i;
                    break;
                }
            }
            if (colon <= 0) {
                return null;
            }
            final String user = new String(decoded, 0, colon, StandardCharsets.UTF_8);
            final CharBuffer chars = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(decoded, colon + 1, decoded.length - colon - 1));
            final char[] password = new char[chars.remaining()];
            chars.get(password);
            if (chars.hasArray()) {
                Arrays.fill(chars.array(), '\0');
            }
            return new Credential(user, password);
        } finally {
            Arrays.fill(decoded, (byte) 0);
        }
    }

    /** A decoded credential; the password is the caller's to zero once it has been checked. */
    record Credential(String user, char[] password) {
    }

    static boolean refuse(RestChannel channel, RestStatus status, String type, String reason, boolean challenge) {
        return refuse(channel, status, type, reason, challenge, 0);
    }

    static boolean refuse(RestChannel channel, RestStatus status, String type, String reason, boolean challenge, long retryAfterSeconds) {
        final org.opensearch.core.xcontent.XContentBuilder body;
        final BytesRestResponse response;
        try {
            body = channel.newErrorBuilder()
                .startObject()
                .startObject("error")
                .field("type", type)
                .field("reason", reason)
                .endObject()
                .field("status", status.getStatus())
                .endObject();
            response = new BytesRestResponse(status, body);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("could not write an authentication refusal", e);
        }
        if (challenge) {
            // Without this a browser or a client library has no way to know it should offer a credential,
            // and a 401 with no challenge reads as a broken server rather than a locked door.
            response.addHeader("WWW-Authenticate", "Basic realm=\"" + ServerlessAuthPlugin.REALM + "\", charset=\"UTF-8\"");
        }
        if (retryAfterSeconds > 0) {
            // A 429 or a 503 without this is a refusal with no advice; with it, a well-behaved client
            // waits exactly as long as it is told to and a badly-behaved one is refused again for free.
            response.addHeader("Retry-After", Long.toString(retryAfterSeconds));
        }
        channel.sendResponse(response);
        return false;
    }
}
