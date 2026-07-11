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
 * The user-facing entry point for resharding-by-copy (rfc-serverless-opensearch.md &sect;16 Phase
 * 5): creates one split target shard as one of {@code numPartitions} logical partitions of a
 * source shard's current published manifest, without copying any bundle bytes. {@link
 * ShardSplitRequest} names a source and target shard plus the target's partition assignment;
 * {@link TransportShardSplitAction} does the actual work via {@link
 * org.opensearch.serverless.storage.resharding.ShardSplitter#split}.
 */
public class ShardSplitAction extends ActionType<ShardSplitResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardSplitAction INSTANCE = new ShardSplitAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/split";

    private ShardSplitAction() {
        super(NAME, ShardSplitResponse::new);
    }
}
