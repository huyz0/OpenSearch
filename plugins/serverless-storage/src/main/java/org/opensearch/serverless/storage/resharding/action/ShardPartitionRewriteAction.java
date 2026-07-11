/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionType;

/**
 * The user-facing entry point for triggering one split target's physical bundle rewrite on demand
 * (rfc-serverless-opensearch.md &sect;16 Phase 5's "logical-first, physical-later" follow-up):
 * materializes the target's full pre-split document set, filters it down to just its own
 * partition, and republishes the result as a fresh, self-sufficient bundle -- see {@link
 * org.opensearch.serverless.storage.resharding.PartitionRewritePublisher}'s own javadoc for the
 * full mechanism. {@link ShardPartitionRewriteRequest} names a shard by (indexUuid, shardId);
 * {@link TransportShardPartitionRewriteAction} does the actual work.
 */
public class ShardPartitionRewriteAction extends ActionType<ShardPartitionRewriteResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardPartitionRewriteAction INSTANCE = new ShardPartitionRewriteAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/partition_rewrite";

    private ShardPartitionRewriteAction() {
        super(NAME, ShardPartitionRewriteResponse::new);
    }
}
