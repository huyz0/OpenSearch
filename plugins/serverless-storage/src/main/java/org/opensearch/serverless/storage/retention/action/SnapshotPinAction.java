/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionType;

/**
 * The user-facing entry point for the first real "snapshot = pinned manifest set" feature
 * (rfc-serverless-opensearch.md &sect;14) -- until now only the underlying generic mechanism
 * ({@link org.opensearch.serverless.storage.retention.DurablePinRegistry}) existed, used by PITR
 * retention and zero-copy clone, but nothing let an operator actually pin a shard's current
 * manifest generation under a snapshot name. Durably pins the shard's <em>current</em> head
 * generation (read via {@code ShardStateStore#get}) under {@code snapshotId} -- metadata-only, no
 * data movement, exactly as &sect;14 describes: "a retention-pinned manifest per shard ... no data
 * movement." {@link SnapshotPinRequest} names a shard by (indexUuid, shardId) plus the snapshot
 * name; {@link TransportSnapshotPinAction} does the actual (object-store-only, no specific-node
 * routing needed) work.
 *
 * <p>Deliberately scoped to one shard per call, same shape as {@link
 * org.opensearch.serverless.storage.compaction.action.CompactionTriggerAction} and {@link
 * org.opensearch.serverless.storage.clone.action.ShardCloneAction} -- an index-wide "snapshot every
 * shard under one name, all-or-nothing" orchestration layer is separate future work, not attempted
 * here.
 *
 * <p>Create-or-replace, not additive: pinning under a {@code snapshotId} that's already pinning an
 * older generation first releases that older pin before adding the new one (see {@link
 * TransportSnapshotPinAction}), matching ordinary snapshot semantics where re-running a snapshot
 * under the same name updates it to the shard's current state rather than accumulating every
 * generation ever pinned under that name.
 */
public class SnapshotPinAction extends ActionType<SnapshotPinResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final SnapshotPinAction INSTANCE = new SnapshotPinAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/snapshot_pin";

    private SnapshotPinAction() {
        super(NAME, SnapshotPinResponse::new);
    }
}
