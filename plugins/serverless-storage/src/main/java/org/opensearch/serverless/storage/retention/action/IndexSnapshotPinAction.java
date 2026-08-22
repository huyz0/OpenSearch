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
 * The index-wide "snapshot every shard under one name, all-or-nothing" orchestration layer
 * flagged as the one still-open piece of &sect;14's design after {@link SnapshotPinAction} landed
 * per-shard pinning. Pins every shard of an index under the same {@code snapshotId} by calling
 * {@link SnapshotPinAction} once per shard, in shard-id order.
 *
 * <p><b>Genuinely all-or-nothing</b> for the pin case specifically, unlike {@link
 * IndexSnapshotRestoreAction} (see its own javadoc for why restore can't make the same guarantee):
 * if any shard's pin attempt fails, {@link TransportIndexSnapshotPinAction} releases {@code
 * snapshotId} from every shard it had already pinned in this same call before failing the whole
 * request -- {@link SnapshotReleaseAction} is idempotent, so the compensating release is itself
 * safe to retry if it too fails partway. A caller that sees this action fail can safely assume
 * {@code snapshotId} names no pin anywhere on this index; a caller that sees it succeed can safely
 * assume it names a consistent, matching-name pin on every shard.
 */
public class IndexSnapshotPinAction extends ActionType<IndexSnapshotPinResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final IndexSnapshotPinAction INSTANCE = new IndexSnapshotPinAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/index/snapshot_pin";

    private IndexSnapshotPinAction() {
        super(NAME, IndexSnapshotPinResponse::new);
    }
}
