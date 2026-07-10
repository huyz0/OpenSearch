/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/index/{index}/_snapshot_release} with a body of the shape
 * {@code {"snapshot_id": "..."}} -- the REST surface for {@link IndexSnapshotReleaseAction}
 * (rfc-serverless-opensearch.md &sect;14).
 */
public class RestIndexSnapshotReleaseAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestIndexSnapshotReleaseAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_index_snapshot_release";
    }

    /** The single route this handler serves. */
    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/index/{index}/_snapshot_release"));
    }

    /**
     * @param request the incoming REST request, whose path names the index and body names the snapshot.
     * @param client used to dispatch the parsed {@link IndexSnapshotReleaseRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexName = request.param("index");
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        Object snapshotId = body.get("snapshot_id");
        if (!(snapshotId instanceof String)) {
            throw new IllegalArgumentException("request body must contain a string \"snapshot_id\"");
        }
        IndexSnapshotReleaseRequest releaseRequest = new IndexSnapshotReleaseRequest(indexName, (String) snapshotId);
        return channel -> client.executeLocally(IndexSnapshotReleaseAction.INSTANCE, releaseRequest, new RestToXContentListener<>(channel));
    }
}
