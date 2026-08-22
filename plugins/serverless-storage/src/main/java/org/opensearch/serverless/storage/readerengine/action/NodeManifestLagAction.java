/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.action.ActionType;

/**
 * Lists every reader shard's manifest-generation lag tracked on whichever node receives the
 * request -- the reader-tier counterpart to {@code
 * org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction}, closing
 * &sect;10's still-open "search tier: manifest-generation lag" autoscaling hook
 * (rfc-serverless-opensearch.md) the same way that action closed the ingest-tier idle-activity
 * hook.
 *
 * <p>Deliberately single-node scope, same shape as {@code NodeIdleShardsAction}: this only ever
 * answers from the receiving node's own {@link org.opensearch.serverless.storage.readerengine.ReaderShardActivityRegistry},
 * never fans out across the cluster.
 */
public class NodeManifestLagAction extends ActionType<NodeManifestLagResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeManifestLagAction INSTANCE = new NodeManifestLagAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/manifest_lag";

    private NodeManifestLagAction() {
        super(NAME, NodeManifestLagResponse::new);
    }
}
