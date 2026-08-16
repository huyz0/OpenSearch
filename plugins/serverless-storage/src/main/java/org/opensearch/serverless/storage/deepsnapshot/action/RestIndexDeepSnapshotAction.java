/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler.ServerlessScope;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * {@code POST /_plugins/_serverless/storage/index/{index}/_snapshot_deep/{repository}/{snapshot}} --
 * the REST surface for {@link IndexDeepSnapshotAction} (rfc-serverless-opensearch.md &sect;14,
 * docs/rounds/006-durability/plan.md item 5). Copies every shard's currently-pinned commit into
 * {@code repository} in the repository's own standard format, under {@code snapshot}, and finalizes
 * it -- an independent copy, not a pointer, restorable by a cluster that has never heard of this
 * plugin.
 *
 * <p>Both names are path parameters rather than a request body, the same shape {@link
 * org.opensearch.serverless.storage.clone.action.RestShardCloneAction} uses for its own two names:
 * this action needs nothing else from the caller.
 */
public class RestIndexDeepSnapshotAction extends BaseRestHandler {

    /** Creates the handler; stateless, so no configuration is needed. */
    public RestIndexDeepSnapshotAction() {}

    @Override
    public String getName() {
        return "serverless_storage_index_deep_snapshot";
    }

    /**
     * Every action this plugin exposes is meaningful only when serverless storage is opted into for
     * the target index, so this handler declares itself available under serverless mode
     * (rfc-serverless-opensearch.md &sect;11), the same as every other handler in this plugin.
     */
    @Override
    public ServerlessScope serverlessScope() {
        return ServerlessScope.AVAILABLE;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_plugins/_serverless/storage/index/{index}/_snapshot_deep/{repository}/{snapshot}"));
    }

    /**
     * @param request the incoming REST request, whose path names the index, the repository, and the snapshot.
     * @param client used to dispatch the parsed {@link IndexDeepSnapshotRequest}.
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String indexName = request.param("index");
        String repositoryName = request.param("repository");
        String snapshotName = request.param("snapshot");
        IndexDeepSnapshotRequest deepSnapshotRequest = new IndexDeepSnapshotRequest(indexName, repositoryName, snapshotName);
        return channel -> client.execute(IndexDeepSnapshotAction.INSTANCE, deepSnapshotRequest, new RestToXContentListener<>(channel));
    }
}
