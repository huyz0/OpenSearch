/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.action.ActionType;

/**
 * Reports the node-shared in-memory bundle cache's hit/miss counts plus every reader shard's
 * local disk cache hit-rate and cold-read latency, tracked on whichever node receives the request
 * -- rfc-serverless-opensearch.md &sect;9's "cache hit-rate and cold-read latency are first-class
 * metrics" goal and &sect;10's still-open "search tier: ... cache hit rate" autoscaling hook, the
 * same shape {@code NodeManifestLagAction} already established for manifest-generation lag.
 *
 * <p>Deliberately single-node scope, same shape as {@code NodeManifestLagAction}: this only ever
 * answers from the receiving node's own {@link org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache}
 * and {@link org.opensearch.serverless.storage.format.CacheStatsRegistry}, never fans out across
 * the cluster.
 */
public class NodeCacheStatsAction extends ActionType<NodeCacheStatsResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeCacheStatsAction INSTANCE = new NodeCacheStatsAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/cache_stats";

    private NodeCacheStatsAction() {
        super(NAME, NodeCacheStatsResponse::new);
    }
}
