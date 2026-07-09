/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.action.ActionType;

/**
 * The first user-facing entry point into {@link org.opensearch.serverless.storage.clone.ShardCloner}
 * (rfc-serverless-opensearch.md &sect;14) -- everything before this was reachable only from Java
 * code within the plugin itself (tests, or a future caller with direct access to the internal
 * classes). {@link ShardCloneRequest} names a source and target shard by (indexUuid, shardId);
 * {@link TransportShardCloneAction} does the actual work.
 */
public class ShardCloneAction extends ActionType<ShardCloneResponse> {

    public static final ShardCloneAction INSTANCE = new ShardCloneAction();
    public static final String NAME = "cluster:admin/serverless/storage/shard/clone";

    private ShardCloneAction() {
        super(NAME, ShardCloneResponse::new);
    }
}
