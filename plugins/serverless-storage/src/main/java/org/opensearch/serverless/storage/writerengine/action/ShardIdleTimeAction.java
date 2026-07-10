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
 * The user-facing entry point for querying how long a writer shard has been idle
 * (rfc-serverless-opensearch.md &sect;16 Phase 4's "suspended writers, scale-to-zero/cold-start"
 * milestone, &sect;15's "autoscaling signal emitters" bullet) -- the first real consumer of {@link
 * org.opensearch.serverless.storage.writerengine.ObjectStoreWriterEngine#millisSinceLastActivity()}
 * outside the engine itself. {@link ShardIdleTimeRequest} names a shard by (indexUuid, shardId);
 * {@link TransportShardIdleTimeAction} does the actual (node-local, no I/O) lookup.
 *
 * <p>Deliberately just a read: this action reports the raw signal, the same "data first, decision
 * later" shape the signal itself was built in. Routing this into an actual suspension decision or
 * a scale-to-zero controller is separate, still-open Phase 4 work.
 */
public class ShardIdleTimeAction extends ActionType<ShardIdleTimeResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardIdleTimeAction INSTANCE = new ShardIdleTimeAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/idle_time";

    private ShardIdleTimeAction() {
        super(NAME, ShardIdleTimeResponse::new);
    }
}
