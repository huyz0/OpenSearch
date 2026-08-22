/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.action.ActionType;

/**
 * The user-facing entry point for triggering one shard's compaction on demand
 * (rfc-serverless-opensearch.md &sect;7.4, &sect;16 Phase 4.5's "narrower than originally scoped"
 * gap), without waiting out {@code CompactionSchedulerTask}'s own fixed background interval and
 * without the core transport-layer work a real {@code _forcemerge} redirect would need (see that
 * section's own status note for why routing core's {@code _forcemerge} itself through this
 * service remains out of scope for this plugin alone). {@link CompactionTriggerRequest} names a
 * shard by (indexUuid, shardId); {@link TransportCompactionTriggerAction} does the actual work.
 */
public class CompactionTriggerAction extends ActionType<CompactionTriggerResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final CompactionTriggerAction INSTANCE = new CompactionTriggerAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/compact";

    private CompactionTriggerAction() {
        super(NAME, CompactionTriggerResponse::new);
    }
}
