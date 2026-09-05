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
import org.opensearch.rest.RestHandler.ApiAvailabilityScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/_snapshot_pin} with a body of the shape
 * {@code {"index_uuid": "...", "shard_id": 0, "snapshot_id": "..."}} -- the REST surface for
 * {@link SnapshotPinAction} (rfc-serverless-opensearch.md &sect;14). Durably pins the shard's
 * current manifest generation under {@code snapshot_id}, metadata-only, no data movement.
 */
public class RestSnapshotPinAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestSnapshotPinAction() {}

    /** This handler's registered name, for logging/metrics. */
    @Override
    public String getName() {
        return "serverless_storage_snapshot_pin";
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
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_snapshot_pin"));
    }

    /**
     * @param request the incoming REST request, whose body names the shard and snapshot.
     * @param client used to dispatch the parsed {@link SnapshotPinRequest} locally.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        Object indexUuid = body.get("index_uuid");
        Object shardId = body.get("shard_id");
        Object snapshotId = body.get("snapshot_id");
        if (!(indexUuid instanceof String) || !(shardId instanceof Number) || !(snapshotId instanceof String)) {
            throw new IllegalArgumentException(
                "request body must contain a string \"index_uuid\", a numeric \"shard_id\", and a string \"snapshot_id\""
            );
        }
        // The deep-snapshot export's own per-shard pins live under a reserved prefix and are taken and
        // released by that action itself; the shard-level request type deliberately accepts it, because that
        // is the path the export uses. An operator reaching this endpoint by hand is not that path, and a
        // hand-written pin or release inside that namespace could strip the pin holding a copy in progress.
        if (((String) snapshotId).startsWith(SnapshotPinRequest.DEEP_SNAPSHOT_PIN_ID_PREFIX)) {
            throw new IllegalArgumentException(SnapshotPinRequest.reservedSnapshotIdError(SnapshotPinRequest.DEEP_SNAPSHOT_PIN_ID_PREFIX));
        }
        SnapshotPinRequest pinRequest = new SnapshotPinRequest((String) indexUuid, ((Number) shardId).intValue(), (String) snapshotId);
        return channel -> client.executeLocally(SnapshotPinAction.INSTANCE, pinRequest, new RestToXContentListener<>(channel));
    }
}
