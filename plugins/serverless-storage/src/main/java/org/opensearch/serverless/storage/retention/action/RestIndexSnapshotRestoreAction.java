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
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/index/{index}/_snapshot_restore} with a body of the shape
 * {@code {"snapshot_id": "..."}} -- the REST surface for {@link IndexSnapshotRestoreAction}
 * (rfc-serverless-opensearch.md &sect;14).
 */
public class RestIndexSnapshotRestoreAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestIndexSnapshotRestoreAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_index_snapshot_restore";
    }

    /** The single route this handler serves. */
    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into
     * for the target index, so every one of its REST handlers declares itself available under
     * serverless mode (rfc-serverless-opensearch.md &sect;11) -- unlike, say, {@code _forcemerge}
     * or shard-store APIs, nothing here assumes local-disk shard state that disaggregated storage
     * invalidates.
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/index/{index}/_snapshot_restore"));
    }

    /**
     * @param request the incoming REST request, whose path names the index and body names the snapshot.
     * @param client used to dispatch the parsed {@link IndexSnapshotRestoreRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexName = request.param("index");
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        Object snapshotId = body.get("snapshot_id");
        Object restoreTo = body.get("restore_to");
        if (snapshotId != null && restoreTo != null) {
            throw new IllegalArgumentException(
                "request body must contain \"snapshot_id\" or \"restore_to\", not both: a pin and an instant "
                    + "can resolve to different generations"
            );
        }
        final IndexSnapshotRestoreRequest restoreRequest;
        if (restoreTo != null) {
            if (!(restoreTo instanceof Number)) {
                throw new IllegalArgumentException("\"restore_to\" must be a number of epoch milliseconds");
            }
            restoreRequest = IndexSnapshotRestoreRequest.toInstant(indexName, ((Number) restoreTo).longValue());
        } else {
            if (!(snapshotId instanceof String)) {
                throw new IllegalArgumentException("request body must contain a string \"snapshot_id\" or a numeric \"restore_to\"");
            }
            restoreRequest = new IndexSnapshotRestoreRequest(indexName, (String) snapshotId);
        }
        return channel -> client.executeLocally(IndexSnapshotRestoreAction.INSTANCE, restoreRequest, new RestToXContentListener<>(channel));
    }
}
