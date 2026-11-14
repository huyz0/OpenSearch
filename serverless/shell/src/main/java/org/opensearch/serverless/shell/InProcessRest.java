/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.http.HttpChannel;
import org.opensearch.http.HttpRequest;
import org.opensearch.http.HttpResponse;
import org.opensearch.rest.AbstractRestChannel;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Runs one of the shell's own REST handlers for a plugin's request, in process.
 *
 * <p><b>Why through the handler.</b> Changing a mapping, an index's settings, an alias or rolling an alias over is
 * what the shell's REST handlers already do against the metadata plane, with their validation, their refusals and the
 * action filters they run. A plugin asking for the same change through its client gets the same behaviour by being
 * run through the same handler, rather than through a second account of each that could drift from the first.
 *
 * <p><b>What it skips.</b> The handler is called directly, not through the REST controller, so the request does not
 * pass the controller's wrapper (an authentication plugin's REST filter, which would turn away a request that carries
 * no credentials) or the shell's guard on system indices. Both exist for requests from outside the node; a plugin's
 * client request comes from inside it, as on a classic node, where the client calls the transport action directly.
 */
final class InProcessRest {

    private InProcessRest() {}

    /** What the handler answered: its status and its body. */
    record Answer(RestStatus status, String body) {
        boolean ok() {
            return status.getStatus() < 300;
        }

        XContentParser parser() throws java.io.IOException {
            return JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                body
            );
        }

        /** The handler's error, as the exception a plugin would have caught from core. */
        Exception failure(String index) {
            OpenSearchStatusException parsed;
            try (XContentParser parser = parser()) {
                parsed = BytesRestResponse.errorFromXContent(parser);
            } catch (Exception e) {
                parsed = new OpenSearchStatusException(body, status);
            }
            final String reason = parsed.getMessage() == null ? "" : parsed.getMessage();
            if (status == RestStatus.NOT_FOUND && index != null && reason.contains("index_not_found")) {
                return new org.opensearch.index.IndexNotFoundException(index);
            }
            if (reason.contains("already_exists") || reason.contains("already exists")) {
                return new org.opensearch.ResourceAlreadyExistsException(reason);
            }
            return parsed;
        }
    }

    /**
     * Runs the handler and waits for its answer.
     *
     * @param handler the shell's handler, unwrapped
     * @param method the method
     * @param path the path, for the handler's own path checks
     * @param params the path and query parameters the route would have bound
     * @param body the JSON body, or null
     * @param client the node client
     * @return what it answered
     */
    static Answer call(
        RestHandler handler,
        RestRequest.Method method,
        String path,
        Map<String, String> params,
        String body,
        org.opensearch.transport.client.node.NodeClient client
    ) throws Exception {
        final BytesReference content = body == null ? BytesArray.EMPTY : new BytesArray(body.getBytes(StandardCharsets.UTF_8));
        final RestRequest request = RestRequest.request(NamedXContentRegistry.EMPTY, new Request(method, path, content), new Channel());
        request.params().putAll(params);
        final CompletableFuture<RestResponse> answered = new CompletableFuture<>();
        final AbstractRestChannel channel = new AbstractRestChannel(request, true) {
            @Override
            public void sendResponse(RestResponse response) {
                answered.complete(response);
            }
        };
        try {
            handler.handleRequest(request, channel, client);
        } catch (Exception e) {
            channel.sendResponse(new BytesRestResponse(channel, e));
        }
        final RestResponse response;
        try {
            response = answered.get();
        } catch (ExecutionException e) {
            throw e.getCause() instanceof Exception cause ? cause : e;
        }
        return new Answer(response.status(), response.content().utf8ToString());
    }

    private record Request(RestRequest.Method method, String uri, BytesReference content) implements HttpRequest {
        @Override
        public Map<String, List<String>> getHeaders() {
            return content.length() == 0 ? Map.of() : Map.of("Content-Type", List.of("application/json"));
        }

        @Override
        public List<String> strictCookies() {
            return List.of();
        }

        @Override
        public HttpVersion protocolVersion() {
            return HttpVersion.HTTP_1_1;
        }

        @Override
        public HttpRequest removeHeader(String header) {
            return this;
        }

        @Override
        public HttpResponse createResponse(RestStatus status, BytesReference content) {
            throw new UnsupportedOperationException("an in-process request is answered through its channel");
        }

        @Override
        public Exception getInboundException() {
            return null;
        }

        @Override
        public void release() {}

        @Override
        public HttpRequest releaseAndCopy() {
            return this;
        }
    }

    private static final class Channel implements HttpChannel {
        private static final InetSocketAddress LOOPBACK = new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), 0);

        @Override
        public void sendResponse(HttpResponse response, ActionListener<Void> listener) {
            listener.onFailure(new UnsupportedOperationException("an in-process request is answered through its channel"));
        }

        @Override
        public InetSocketAddress getLocalAddress() {
            return LOOPBACK;
        }

        @Override
        public InetSocketAddress getRemoteAddress() {
            return LOOPBACK;
        }

        @Override
        public void close() {}

        @Override
        public void addCloseListener(ActionListener<Void> listener) {}

        @Override
        public boolean isOpen() {
            return true;
        }
    }
}
