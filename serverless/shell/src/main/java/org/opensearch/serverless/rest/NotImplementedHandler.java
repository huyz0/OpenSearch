/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

/**
 * Answers a classic OpenSearch endpoint that this shell does not implement with an explicit 501.
 *
 * <p>Decision D2 of {@code rfc-serverless-shell.md} makes the REST surface an allowlist. The reason it
 * is worth registering handlers whose only job is to refuse: an unrouted path already fails, but it
 * fails as "no handler for uri", which reads like a typo. These endpoints are not missing — they are
 * absent by design, because there is no elected node to compute a cluster-wide answer and no global
 * state to compute it from.
 *
 * <p>The alternative — implementing them against a node-local view — is precisely the
 * "confident empty answer" failure {@code HANDOFF.md} records eight times, where a green cluster health
 * or an empty cat listing is returned by a node that simply cannot see anything. A 501 is a worse user
 * experience and a far better answer.
 */
public final class NotImplementedHandler extends BaseRestHandler {

    private final String path;
    private final String reason;

    /**
     * Creates a refusal for one path.
     *
     * @param path the exact path to refuse
     * @param reason why it does not exist here, reported to the caller
     */
    public NotImplementedHandler(String path, String reason) {
        this.path = path;
        this.reason = reason;
    }

    @Override
    public String getName() {
        return "serverless_not_implemented" + path.replace('/', '_');
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, path),
            new Route(RestRequest.Method.POST, path),
            new Route(RestRequest.Method.PUT, path),
            new Route(RestRequest.Method.DELETE, path)
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        // Consume every parameter before returning. BaseRestHandler rejects a request whose parameters
        // were not all read, so a refusal on a path with a placeholder in it -- /{index}/_count, say --
        // came back as "unrecognized parameter: [index]" instead of as the refusal it is.
        for (String name : java.util.List.copyOf(request.params().keySet())) {
            request.param(name);
        }
        // The shared error helper, so a refusal renders the same nested "error" object every other
        // deliberate refusal on this surface does. This handler built its own body with "error" as a bare
        // string -- the shape M47 removed everywhere else and missed here, which meant the responses
        // trying hardest to explain themselves were the ones a structurally-parsing client broke on.
        // The path is carried in the reason rather than as a field of its own, because the error object's
        // shape is core's and is not this handler's to extend.
        return channel -> channel.sendResponse(
            IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "not_implemented", path + " is not available here: " + reason)
        );
    }
}
