/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.function.UnaryOperator;

/**
 * Refuses REST handlers that have not declared themselves available under serverless mode.
 *
 * <h2>Why this lives in the plugin</h2>
 *
 * Disaggregated storage makes several existing APIs meaningless or dangerous: shard-store APIs report on
 * local disk state that may not exist, {@code _forcemerge} means something else entirely under a compaction
 * service, and snapshot/restore is partly redundant with a manifest-native format. So a serverless
 * deployment needs to gate REST surface per handler.
 *
 * <p>An earlier version of this put the switch in core: a {@code rest.serverless_mode.enabled} setting on
 * {@link org.opensearch.rest.RestController}, a field, an extra constructor, and a 410 branch in
 * {@code dispatchRequest}. It was default-off and so broke nothing, but it put a decision in core rather
 * than a hook, which is the thing core changes here are supposed to avoid. Core keeps only the vocabulary,
 * {@link RestHandler#apiAvailabilityScope()}, which nothing in core reads.
 *
 * <p>{@code ActionPlugin.getRestHandlerWrapper} already exists upstream and already sees every handler, so
 * no new seam was needed for this at all.
 *
 * <h2>One difference from the core version, and it is an improvement</h2>
 *
 * {@code RestController} registers {@code /favicon.ico} through {@code registerHandlerNoWrap}, which
 * bypasses the wrapper. The core version had to give the favicon an explicit {@code AVAILABLE} override to
 * stop its own new setting from returning 410 for it. Here the favicon is simply never wrapped, so that
 * special case disappears rather than being reproduced.
 */
public final class ServerlessRestGate implements UnaryOperator<RestHandler> {

    static final String REFUSAL_MESSAGE = "this API is not available on a node running in serverless mode";

    @Override
    public RestHandler apply(RestHandler handler) {
        if (handler.apiAvailabilityScope() == RestHandler.ApiAvailabilityScope.AVAILABLE) {
            // Returned unwrapped rather than wrapped-and-passed-through, so that an available handler
            // keeps its own identity for anything that inspects it (usage stats, deprecation wrapping).
            return handler;
        }
        return new Refused(handler);
    }

    /**
     * Stands in for a handler the deployment does not offer. Delegates everything that describes the
     * handler and replaces only what it does, so routing, naming and stats continue to see the real thing.
     */
    private static final class Refused extends RestHandler.Wrapper {

        Refused(RestHandler delegate) {
            super(delegate);
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws IOException {
            // INTERNAL_ONLY is refused here as well as UNAVAILABLE. This wrapper is only ever reached
            // through external HTTP dispatch, so an "internal caller" cannot arrive by this route by
            // construction; a handler declaring INTERNAL_ONLY is saying ordinary HTTP callers must not
            // reach it, which is exactly the case being handled.
            //
            // Body shaped by hand rather than through BytesRestResponse.createSimpleErrorResponse, which
            // core keeps package-private. Same two fields, so a client cannot tell the difference.
            channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.GONE,
                    channel.newErrorBuilder()
                        .startObject()
                        .field("error", REFUSAL_MESSAGE)
                        .field("status", RestStatus.GONE.getStatus())
                        .endObject()
                )
            );
        }
    }
}
