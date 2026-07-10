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
 * The restore-in-place half of "snapshot = pinned manifest set" (rfc-serverless-opensearch.md
 * &sect;14: "Restore-in-place is 'point the shard-heads at the pinned manifests.'") -- {@link
 * SnapshotPinAction}/{@link SnapshotReleaseAction} landed the create/delete half in an earlier
 * increment; this closes the read side of the feature. CASes the shard's head so its {@code
 * latestManifestGeneration} points at whatever generation {@code snapshotId} currently pins,
 * leaving {@code primaryTerm} untouched -- a real point-in-time rollback of what a reader
 * materializes from this shard, not a new shard/index.
 *
 * <p><b>Deliberately refuses to restore while the shard's writer/compactor lease is currently
 * held</b> (see {@link TransportSnapshotRestoreAction}): a live writer holding the lease will keep
 * publishing forward on its own schedule regardless of what this action just wrote, so restoring
 * underneath an active writer would either be immediately overwritten by that writer's next flush
 * or, worse, leave the head in a confusing intermediate state neither the pre- nor post-restore
 * generation. Restore-in-place is meant for a quiesced shard (no active writer), not a live one --
 * true "restore into a fresh index" (the safe way to inspect old data next to a live original) is
 * {@link org.opensearch.serverless.storage.clone.action.ShardCloneAction} pointed at the pinned
 * generation instead, not this action.
 *
 * <p>Deliberately scoped to one shard per call, same shape as every other action this plugin
 * exposes -- an index-wide "restore every shard from one snapshot, all-or-nothing" orchestration
 * layer is separate future work, not attempted here.
 */
public class SnapshotRestoreAction extends ActionType<SnapshotRestoreResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final SnapshotRestoreAction INSTANCE = new SnapshotRestoreAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/snapshot_restore";

    private SnapshotRestoreAction() {
        super(NAME, SnapshotRestoreResponse::new);
    }
}
