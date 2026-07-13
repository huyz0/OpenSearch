/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

import org.opensearch.action.ActionType;

/**
 * Reports the real object-store request counts (get/put/delete/list) tallied on whichever node
 * receives the request -- rfc-serverless-opensearch.md &sect;18 risk #1's own mitigation, "publish
 * request-count metrics from day one, and treat them as SLOs," which until now had no runtime
 * signal at all, only a build-time regression-test guard.
 *
 * <p>Deliberately single-node scope, same shape as {@code NodeCacheStatsAction}: this only ever
 * answers from the receiving node's own {@link org.opensearch.serverless.storage.security.ObjectStoreRequestCounter},
 * never fans out across the cluster.
 */
public class NodeObjectStoreRequestStatsAction extends ActionType<NodeObjectStoreRequestStatsResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeObjectStoreRequestStatsAction INSTANCE = new NodeObjectStoreRequestStatsAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/object_store_request_stats";

    private NodeObjectStoreRequestStatsAction() {
        super(NAME, NodeObjectStoreRequestStatsResponse::new);
    }
}
