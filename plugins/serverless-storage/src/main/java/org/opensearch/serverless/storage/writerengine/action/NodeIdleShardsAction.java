/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.action.ActionType;

/**
 * Lists every writer shard's idle time tracked on whichever node receives the request -- the
 * "signal collection" half of &sect;7.3/&sect;10's still-open autoscaling story
 * (rfc-serverless-opensearch.md), letting an external suspension/scale-to-zero controller discover
 * which shards on a node are idle, and by how much, without already knowing every (indexUuid,
 * shardId) to ask {@link ShardIdleTimeAction} about one at a time.
 *
 * <p>Deliberately single-node scope, same shape as {@link ShardIdleTimeAction}: this only ever
 * answers from the receiving node's own {@link org.opensearch.serverless.storage.writerengine.ShardActivityRegistry},
 * never fans out across the cluster -- a caller that wants every idle shard cluster-wide is
 * expected to call this once per data node, the same "caller already knows the topology" contract
 * {@link ShardIdleTimeAction} already established, not a new pattern invented here.
 */
public class NodeIdleShardsAction extends ActionType<NodeIdleShardsResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final NodeIdleShardsAction INSTANCE = new NodeIdleShardsAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/node/idle_shards";

    private NodeIdleShardsAction() {
        super(NAME, NodeIdleShardsResponse::new);
    }
}
