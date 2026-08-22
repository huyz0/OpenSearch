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
 * The index-wide counterpart to {@link SnapshotRestoreAction}: restores every shard of an index to
 * the manifest generation pinned under {@code snapshotId}, by calling {@link SnapshotRestoreAction}
 * once per shard.
 *
 * <p><b>Not genuinely atomic, unlike {@link IndexSnapshotPinAction}</b> -- and this is deliberate,
 * not an oversight: {@link TransportIndexSnapshotRestoreAction} first validates that every shard
 * actually has a pin under {@code snapshotId} (a read-only pass, touching no shard head), and only
 * proceeds to the real per-shard restores once every shard passes that check. That closes the
 * common failure mode (a typo'd or partially-pinned {@code snapshotId}) before anything is
 * mutated. What it can <em>not</em> close is a shard's writer lease being (re)acquired between the
 * validation pass and that shard's own restore call -- {@link SnapshotRestoreAction} refuses in
 * that case (see its own javadoc), which would leave earlier shards in this same call already
 * restored while a later one is refused. A true cross-shard rollback would need each restored
 * shard's *previous* head recorded before restoring, which this call does not do (restore is
 * already a deliberate rollback of the shard's own history; compounding that with a second,
 * separate rollback log was judged not worth the complexity for an operation this rare and always
 * followed by an operator re-checking the outcome). Callers that need a hard guarantee should
 * restore-in-place onto a closed index (the same precondition {@link SnapshotRestoreAction} itself
 * requires per shard) so no lease can be reacquired mid-call.
 */
public class IndexSnapshotRestoreAction extends ActionType<IndexSnapshotRestoreResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final IndexSnapshotRestoreAction INSTANCE = new IndexSnapshotRestoreAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/index/snapshot_restore";

    private IndexSnapshotRestoreAction() {
        super(NAME, IndexSnapshotRestoreResponse::new);
    }
}
