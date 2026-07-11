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
 * The user-facing entry point for resharding-by-copy's "shrink" half
 * (rfc-serverless-opensearch.md &sect;16 Phase 5): merges several existing shards' current document
 * sets into one brand-new target shard identity via a real Lucene merge -- see {@link
 * org.opensearch.serverless.storage.resharding.ShardShrinker}'s own javadoc for why this, unlike
 * split, cannot be zero-copy. {@link ShardShrinkRequest} names every source shard plus the target;
 * {@link TransportShardShrinkAction} does the actual work.
 */
public class ShardShrinkAction extends ActionType<ShardShrinkResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardShrinkAction INSTANCE = new ShardShrinkAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/shrink";

    private ShardShrinkAction() {
        super(NAME, ShardShrinkResponse::new);
    }
}
