/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal.action;

import org.opensearch.action.ActionType;

/**
 * Reports the node-shared WAL chunk service's currently-buffered (not yet flushed) record count
 * and total payload bytes on whichever node receives the request -- rfc-serverless-opensearch.md
 * &sect;10's still-open "ingest tier: WAL upload backlog" autoscaling hook, the same shape {@code
 * NodeCacheStatsAction} already established for the cache-layer metrics.
 *
 * <p>Deliberately single-node scope, same shape as {@code NodeCacheStatsAction}: this only ever
 * answers from the receiving node's own {@link org.opensearch.serverless.storage.wal.WalChunkService},
 * never fans out across the cluster. Reports zero for both fields if WAL mirroring is off on this
 * node (no {@link org.opensearch.serverless.storage.wal.WalChunkService} instance at all), rather
 * than failing the request.
 */
public class NodeWalBacklogAction extends ActionType<NodeWalBacklogResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeWalBacklogAction INSTANCE = new NodeWalBacklogAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/wal_backlog";

    private NodeWalBacklogAction() {
        super(NAME, NodeWalBacklogResponse::new);
    }
}
