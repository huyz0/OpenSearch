/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

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
 * {@code POST /_plugins/_serverless/storage/_snapshot_deep} with a body naming the shard, the
 * repository and the snapshot identity -- the shard-level REST surface for {@link
 * ShardDeepSnapshotAction}, the same shape {@link
 * org.opensearch.serverless.storage.retention.action.RestSnapshotPinAction} uses for its own
 * shard-level action. {@link IndexDeepSnapshotAction}'s own {@link RestIndexDeepSnapshotAction} is
 * the ordinary way to reach this; this route exists so the shard-level action, like every other one
 * in this plugin, has a REST surface of its own rather than being reachable only via another action's
 * internal fan-out.
 */
public class RestShardDeepSnapshotAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestShardDeepSnapshotAction() {}

    @Override
    public String getName() {
        return "serverless_storage_shard_deep_snapshot";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into for
     * the target index, so this handler declares itself available under serverless mode
     * (rfc-serverless-opensearch.md &sect;11), the same as every other handler in this plugin.
     */
    @Override
    public ApiAvailabilityScope apiAvailabilityScope() {
        return ApiAvailabilityScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/_snapshot_deep"));
    }

    /**
     * @param request the incoming REST request, whose body names the shard, the repository, and the
     *                snapshot identity to copy it under.
     * @param client used to dispatch the parsed {@link ShardDeepSnapshotRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        Map<String, Object> body;
        try (XContentParser parser = request.contentParser()) {
            body = parser.map();
        }
        Object indexName = body.get("index_name");
        Object indexUuid = body.get("index_uuid");
        Object shardId = body.get("shard_id");
        Object repositoryName = body.get("repository");
        Object snapshotName = body.get("snapshot_name");
        Object snapshotUuid = body.get("snapshot_uuid");
        if (indexName instanceof String == false
            || indexUuid instanceof String == false
            || shardId instanceof Number == false
            || repositoryName instanceof String == false
            || snapshotName instanceof String == false
            || snapshotUuid instanceof String == false) {
            throw new IllegalArgumentException(
                "request body must contain string \"index_name\", \"index_uuid\", \"repository\", \"snapshot_name\", "
                    + "\"snapshot_uuid\", and a numeric \"shard_id\""
            );
        }
        ShardDeepSnapshotRequest deepSnapshotRequest = new ShardDeepSnapshotRequest(
            (String) indexName,
            (String) indexUuid,
            ((Number) shardId).intValue(),
            (String) repositoryName,
            (String) snapshotName,
            (String) snapshotUuid
        );
        return channel -> client.executeLocally(
            ShardDeepSnapshotAction.INSTANCE,
            deepSnapshotRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
