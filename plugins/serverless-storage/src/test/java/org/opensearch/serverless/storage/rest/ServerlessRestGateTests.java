/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

/**
 * The serverless REST gate, which is the plugin-side replacement for what used to be a setting and a branch
 * inside {@code RestController}.
 *
 * <p>What these assert is that the gate refuses what the handler's own declaration says to refuse, that it
 * refuses by not running the handler rather than by running it and discarding the result, and that a node
 * which has not turned gating on installs no wrapper at all.
 */
public class ServerlessRestGateTests extends OpenSearchTestCase {

    /** A handler that records whether it ran, so "refused" can mean "did not execute" rather than "returned 410". */
    private static final class RecordingHandler implements RestHandler {

        private final ApiAvailabilityScope scope;
        private final AtomicBoolean ran = new AtomicBoolean();

        RecordingHandler(ApiAvailabilityScope scope) {
            this.scope = scope;
        }

        @Override
        public ApiAvailabilityScope apiAvailabilityScope() {
            return scope;
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(RestRequest.Method.GET, "/_recording"));
        }

        @Override
        public boolean supportsContentStream() {
            return true;
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
            ran.set(true);
        }
    }

    private FakeRestChannel dispatch(RestHandler handler) throws Exception {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_recording").build();
        FakeRestChannel channel = new FakeRestChannel(request, true, 1);
        handler.handleRequest(request, channel, null);
        return channel;
    }

    public void testAnAvailableHandlerIsLeftAlone() throws Exception {
        RecordingHandler handler = new RecordingHandler(RestHandler.ApiAvailabilityScope.AVAILABLE);

        RestHandler gated = new ServerlessRestGate().apply(handler);

        assertSame("an available handler must not be wrapped at all", handler, gated);
        dispatch(gated);
        assertTrue("and it must still run", handler.ran.get());
    }

    public void testAnUnavailableHandlerIsRefusedWithoutRunning() throws Exception {
        RecordingHandler handler = new RecordingHandler(RestHandler.ApiAvailabilityScope.UNAVAILABLE);

        FakeRestChannel channel = dispatch(new ServerlessRestGate().apply(handler));

        assertFalse("the handler must never execute, or gating is cosmetic", handler.ran.get());
        assertEquals(RestStatus.GONE, channel.capturedResponse().status());
    }

    /**
     * INTERNAL_ONLY is refused too. This wrapper is only reachable through external HTTP dispatch, so a
     * handler declaring itself internal is saying exactly that callers arriving this way must not reach it.
     */
    public void testAnInternalOnlyHandlerIsAlsoRefused() throws Exception {
        RecordingHandler handler = new RecordingHandler(RestHandler.ApiAvailabilityScope.INTERNAL_ONLY);

        FakeRestChannel channel = dispatch(new ServerlessRestGate().apply(handler));

        assertFalse(handler.ran.get());
        assertEquals(RestStatus.GONE, channel.capturedResponse().status());
    }

    /**
     * A handler that says nothing is refused, because {@code apiAvailabilityScope()} defaults to UNAVAILABLE.
     * That default is why core cannot be the one enforcing this: were the check in {@code RestController},
     * every handler in every plugin that had never heard of serverless mode would need an override.
     */
    public void testAnUndeclaredHandlerIsRefused() throws Exception {
        RestHandler undeclared = new RestHandler() {
            @Override
            public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) {
                throw new AssertionError("must not run");
            }
        };

        FakeRestChannel channel = dispatch(new ServerlessRestGate().apply(undeclared));

        assertEquals(RestStatus.GONE, channel.capturedResponse().status());
    }

    /**
     * Everything describing the handler must survive wrapping, and only what it does may change. Routes in
     * particular: core registers a wrapped handler under the wrapper's routes, so a gate that dropped them
     * would unregister the path rather than refuse it, and the request would 404 instead of 410.
     */
    public void testARefusedHandlerStillDescribesItself() {
        RecordingHandler handler = new RecordingHandler(RestHandler.ApiAvailabilityScope.UNAVAILABLE);

        RestHandler gated = new ServerlessRestGate().apply(handler);

        assertEquals(handler.routes(), gated.routes());
        assertEquals(handler.supportsContentStream(), gated.supportsContentStream());
        assertEquals(handler.apiAvailabilityScope(), gated.apiAvailabilityScope());
    }

    /**
     * Off by default, and "off" means no wrapper rather than an identity wrapper. Core allows exactly one
     * plugin to install a REST wrapper, so returning a passthrough would silently claim that slot.
     */
    public void testTheWrapperIsAbsentUnlessGatingIsTurnedOn() {
        UnaryOperator<RestHandler> byDefault = new ServerlessStoragePlugin(Settings.EMPTY).getRestHandlerWrapper(null, Set.of());
        assertNull("a node that has not opted in must install no wrapper", byDefault);

        Settings on = Settings.builder().put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REST_GATING_ENABLED_SETTING.getKey(), true).build();
        assertNotNull("and one that has opted in must", new ServerlessStoragePlugin(on).getRestHandlerWrapper(null, Set.of()));
    }
}
