/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.restcore;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;

import java.io.IOException;

/**
 * What every REST handler answers with: a deliberate refusal in OpenSearch's own error shape, an uncaught failure
 * without the node's internals, and the request parameters and action filters every handler reads the same way.
 */
public final class RestReplies {

    private RestReplies() {}

    /** A refusal raised while parsing, carrying the status and type the caller should see. */
    public static final class RefusedException extends Exception {
        public final RestStatus status;
        public final String type;

        public RefusedException(RestStatus status, String type, String reason) {
            super(reason);
            this.status = status;
            this.type = type;
        }
    }

    /**
     * Renders a deliberate refusal in real OpenSearch's own error shape.
     *
     * <p><b>{@code error} is an object here, not a bare string, and that is not cosmetic.</b> Every
     * uncaught exception on this surface already renders correctly — {@code new BytesRestResponse(channel,
     * e)} is core's own {@code OpenSearchException} renderer, and it has always produced
     * {@code {"error": {"root_cause": [...], "type": ..., "reason": ...}, "status": N}}. This helper is
     * what every <em>deliberate</em> refusal on this surface goes through instead — the 501s, the 400s,
     * the conditional-write and wildcard-pattern refusals — and until now it rendered {@code "error"} as a
     * plain string. Any client that parses an error structurally rather than only checking the HTTP status
     * broke on precisely the responses that were trying hardest to explain themselves. This makes the two
     * paths render the same shape, because a caller should not be able to tell "the shell refused this on
     * purpose" from "something threw" by the error envelope's own shape.
     *
     * @param channel the channel to build a response for
     * @param status the HTTP status
     * @param type the error type, e.g. {@code "index_not_found"}
     * @param reason a human-readable explanation
     * @return the rendered response
     * @throws IOException if building the response fails
     */
    public static BytesRestResponse error(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason)
        throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject("error");
            builder.startArray("root_cause");
            builder.startObject();
            builder.field("type", type);
            builder.field("reason", reason);
            builder.endObject();
            builder.endArray();
            builder.field("type", type);
            builder.field("reason", reason);
            builder.endObject();
            builder.field("status", status.getStatus());
            builder.endObject();
            return new BytesRestResponse(status, builder);
        }
    }

    /**
     * Renders an uncaught failure without the node's internals.
     *
     * <p>Core's own renderer, except that an object-store failure carrying a filesystem path is answered
     * as what it is -- an object-store failure naming the blob -- rather than with the absolute path of
     * a file on this node's disk.
     *
     * @param channel the channel to answer on
     * @param e the failure
     * @return the response
     * @throws IOException if rendering fails
     */
    public static BytesRestResponse failure(org.opensearch.rest.RestChannel channel, Exception e) throws IOException {
        for (Throwable cause = e; cause != null; cause = cause.getCause()) {
            if (cause instanceof org.opensearch.serverless.shard.UnsupportedRequestOptionException) {
                return new BytesRestResponse(channel, RestStatus.NOT_IMPLEMENTED, e);
            }
            if (cause instanceof org.opensearch.serverless.store.WriteBackpressure.ThrottledException throttled) {
                // Refused before anything was applied: an honest "not now", with when to come back.
                final BytesRestResponse response = new BytesRestResponse(channel, e);
                response.addHeader("Retry-After", Integer.toString(throttled.retryAfterSeconds()));
                return response;
            }
            if (cause instanceof org.opensearch.serverless.write.StaleIncarnationException) {
                // Routed by a resolution that has since gone stale; resolving again is the whole remedy.
                return error(channel, RestStatus.SERVICE_UNAVAILABLE, "index_recreated", cause.getMessage());
            }
            if (cause instanceof java.nio.file.FileSystemException fs) {
                final String blob = fs.getFile() == null ? "?" : java.nio.file.Path.of(fs.getFile()).getFileName().toString();
                return error(
                    channel,
                    RestStatus.INTERNAL_SERVER_ERROR,
                    "object_store_failure",
                    cause.getClass().getSimpleName() + " on blob [" + blob + "]"
                );
            }
            if (cause == cause.getCause()) {
                break;
            }
        }
        return new BytesRestResponse(channel, e);
    }

    /**
     * Reads {@code refresh} the way core reads it: {@code true}, {@code false}, bare (meaning true) or
     * {@code wait_for}.
     *
     * <p>{@code wait_for} means "answer once this write is visible to search". A write here is made
     * visible by refreshing the shard before answering, which is what {@code true} does -- so the two
     * spellings ask for the same thing at the same cost, and {@code wait_for} is honoured by doing it.
     * This used to be parsed as a boolean, which made a spelling every client library offers a 400.
     *
     * @param request the request
     * @return whether to refresh before answering
     */
    public static boolean refresh(RestRequest request) {
        final String value = request.param("refresh");
        if (value == null) {
            return false;
        }
        if (value.isEmpty() || "wait_for".equals(value)) {
            return true;
        }
        return org.opensearch.common.Booleans.parseBoolean(value);
    }

    /**
     * Reads every parameter a request carries, so a refusal made before a handler got to them all renders
     * as itself rather than as BaseRestHandler's "unrecognized parameter" about whichever one it had not
     * reached yet.
     *
     * @param request the request
     */
    public static void consumeAllParams(RestRequest request) {
        for (String name : java.util.List.copyOf(request.params().keySet())) {
            request.param(name);
        }
    }

    /**
     * Runs work under the plugins' action filters, named as core names the action.
     *
     * <p>The same call every handler makes, so the table of what is gated is the table of handlers that
     * call this -- not a list kept elsewhere that drifts. Two-thirds of this surface used to reach the
     * metadata plane directly, and a security plugin's privilege evaluation never ran for any of it.
     *
     * @param <T> what the work returns
     * @param serving the node, or null before one is wired
     * @param action core's action name
     * @param request the request a filter evaluates, carrying the indices it touches
     * @param work what to do if admitted
     * @return what the work returned
     * @throws Exception a filter's refusal, or the work's failure
     */
    public static <T> T gate(
        org.opensearch.serverless.node.NodeContext serving,
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        return serving == null ? work.get() : serving.services().actionGate().run(action, request, work);
    }

    /**
     * Words a failed forward for the caller, telling a deadline apart from a refusal.
     *
     * <p>A forward that timed out is not "stale routing, retry": the owner may have applied the write
     * and been slow to answer, and a client that retries an auto-id write on that message writes the
     * document twice. The deadline case says so, so the client can read before it writes again.
     *
     * @param owner the node the request was forwarded to
     * @param e what went wrong
     * @return the message
     */
    public static String forwardFailureMessage(String owner, Exception e) {
        if (org.opensearch.ExceptionsHelper.unwrap(e, org.opensearch.transport.ReceiveTimeoutTransportException.class) != null
            || org.opensearch.ExceptionsHelper.unwrap(e, org.opensearch.OpenSearchTimeoutException.class) != null
            || org.opensearch.ExceptionsHelper.unwrap(
                e,
                org.opensearch.common.util.concurrent.UncategorizedExecutionException.class
            ) != null && e.getMessage() != null && e.getMessage().contains("imeout")) {
            return "the owner "
                + owner
                + " did not answer within the deadline; the operation may or may not have been applied there, so read before retrying a write";
        }
        return "could not forward to " + owner + ", which the shard-head named as owner: " + e.getMessage();
    }
}
